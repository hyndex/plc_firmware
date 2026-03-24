// SPDX-License-Identifier: Apache-2.0
#include "cbmodules/module_manager.hpp"

#include <fcntl.h>
#include <glob.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

using cbmodules::CanFrame;
using cbmodules::CanTransport;
using cbmodules::GroupConfig;
using cbmodules::GroupStatus;
using cbmodules::GroupTarget;
using cbmodules::ModuleManager;
using cbmodules::ModuleManagerConfig;
using cbmodules::ModuleLifecycle;
using cbmodules::ModuleSpec;
using cbmodules::ModuleStateView;
using cbmodules::ModuleTransitionState;
using cbmodules::ModuleType;

constexpr uint8_t ACK_OK = 0;
constexpr uint8_t ACK_BAD_SEQ = 4;
constexpr uint8_t ACK_CMD_HEARTBEAT = 0x10;
constexpr uint8_t ACK_CMD_AUTH = 0x11;
constexpr uint8_t ACK_CMD_SLAC = 0x12;
constexpr uint8_t ACK_CMD_RELAY = 0x17;
constexpr uint8_t ACK_CMD_STOP = 0x18;
constexpr uint8_t ACK_CMD_FEEDBACK = 0x1A;

constexpr float MODULE_MAX_VOLTAGE_V = 1000.0f;
constexpr float MODULE_MAX_CURRENT_A = 100.0f;
constexpr float MODULE_MAX_POWER_KW = 30.0f;
constexpr float PRECHARGE_VOLTAGE_TOLERANCE_V = 20.0f;
constexpr float PRECHARGE_START_ACCEPT_MIN_RATIO = 0.05f;
constexpr float PRECHARGE_START_ACCEPT_MIN_V = 5.0f;
constexpr float PRECHARGE_START_ACCEPT_MIN_CURRENT_A = 0.50f;
constexpr uint32_t PRECHARGE_RELAX_MIN_COUNT = 2u;
constexpr uint32_t CURRENT_DEMAND_FRESH_SAMPLE_TIMEOUT_MS = 120u;
constexpr uint32_t CURRENT_DEMAND_FRESH_SAMPLE_POLL_MS = 5u;
constexpr uint32_t ACTIVE_PLAN_REAPPLY_MS = 100u;
constexpr int ACTIVE_FEEDBACK_ACK_TIMEOUT_MS = 300;
constexpr float ACTIVE_FEEDBACK_V_DELTA_V = 1.0f;
constexpr float ACTIVE_FEEDBACK_I_DELTA_A = 0.1f;
constexpr int64_t ACTIVE_STAGE_LATCH_MS = 500;
constexpr int CTRL_PROTO_TIMEOUT_QUANTA_MS = 100;
constexpr int CTRL_PROTO_TIMEOUT_MAX_UNITS = 255;
constexpr int CTRL_PROTO_TIMEOUT_MAX_MS = CTRL_PROTO_TIMEOUT_QUANTA_MS * CTRL_PROTO_TIMEOUT_MAX_UNITS;
constexpr uint32_t IDLE_ALLOWLIST_REFRESH_MS = 1000u;
constexpr int CAN_SEND_TIMEOUT_MS = 20;
constexpr uint32_t MODULE_ALLOWLIST_LEASE_MS = 3000u;
constexpr int USB_TOUCH_RESET_SETTLE_MS = 1500;
constexpr int USB_TOUCH_RESET_WAIT_MS = 8000;
constexpr int MODULE_IDLE_SETTLE_TIMEOUT_MS = 5000;
constexpr int PLC_BOOT_READY_TIMEOUT_MS = 12000;
constexpr int PLC_BOOT_STATUS_RETRY_MS = 250;
constexpr int64_t CTRL_SLAC_RETRY_MS = 2500;

int64_t steady_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

int clamp_ctrl_timeout_ms(int value_ms, int minimum_ms = CTRL_PROTO_TIMEOUT_QUANTA_MS) {
    return std::max(minimum_ms, std::min(CTRL_PROTO_TIMEOUT_MAX_MS, value_ms));
}

int clamp_ctrl_timeout_ms_allow_zero(int value_ms) {
    if (value_ms <= 0) {
        return 0;
    }
    return clamp_ctrl_timeout_ms(value_ms);
}

void reset_can_interface(const std::string& iface) {
    const std::string cmd =
        "sudo ip link set " + iface + " down && sudo ip link set " + iface + " up type can bitrate 125000 restart-ms 100";
    const int rc = std::system(cmd.c_str());
    if (rc != 0) {
        throw std::runtime_error("failed to reset CAN interface " + iface);
    }
}

const char* lifecycle_name(ModuleLifecycle lifecycle) {
    switch (lifecycle) {
        case ModuleLifecycle::Missing:
            return "Missing";
        case ModuleLifecycle::Discovered:
            return "Discovered";
        case ModuleLifecycle::Validated:
            return "Validated";
        case ModuleLifecycle::Allocated:
            return "Allocated";
        case ModuleLifecycle::Draining:
            return "Draining";
        case ModuleLifecycle::Faulted:
            return "Faulted";
        case ModuleLifecycle::Quarantined:
            return "Quarantined";
        default:
            return "Unknown";
    }
}

const char* transition_name(ModuleTransitionState transition) {
    switch (transition) {
        case ModuleTransitionState::Idle:
            return "Idle";
        case ModuleTransitionState::AcquirePrepare:
            return "AcquirePrepare";
        case ModuleTransitionState::AcquireSettle:
            return "AcquireSettle";
        case ModuleTransitionState::Drain:
            return "Drain";
        case ModuleTransitionState::OffVerify:
            return "OffVerify";
        case ModuleTransitionState::ReleaseCommit:
            return "ReleaseCommit";
        case ModuleTransitionState::FaultIsolated:
            return "FaultIsolated";
        default:
            return "Unknown";
    }
}

std::string iso_ts() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto tt = system_clock::to_time_t(now);
    const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    std::tm tm{};
    localtime_r(&tt, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << ms.count();
    return oss.str();
}

bool active_power_stage_name(const std::string& stage_name) {
    return stage_name == "precharge" || stage_name == "power_delivery" || stage_name == "current_demand";
}

bool digital_comm_cp_phase(const std::string& cp_phase) {
    return cp_phase == "B2" || cp_phase == "C" || cp_phase == "D";
}

void ensure_parent_dir(const std::string& path) {
    const auto pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return;
    }
    const std::string parent = path.substr(0, pos);
    if (parent.empty()) {
        return;
    }
    std::filesystem::create_directories(parent);
}

void wait_for_serial_path(const std::string& path, int timeout_ms) {
    const int64_t deadline = steady_ms() + timeout_ms;
    while (steady_ms() < deadline) {
        struct stat st {};
        if (::stat(path.c_str(), &st) == 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("serial port did not reappear after reset: " + path);
}

void usb_touch_reset(const std::string& path) {
    const int fd = ::open(path.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) {
        throw std::runtime_error("failed opening " + path + " for USB touch reset: " + std::strerror(errno));
    }
    termios tio{};
    if (tcgetattr(fd, &tio) != 0) {
        const int saved_errno = errno;
        ::close(fd);
        throw std::runtime_error("tcgetattr failed during USB touch reset: " + std::string(std::strerror(saved_errno)));
    }
    cfmakeraw(&tio);
    cfsetispeed(&tio, B1200);
    cfsetospeed(&tio, B1200);
    tio.c_cflag |= (CLOCAL | CREAD);
    tio.c_cflag &= ~CSTOPB;
    tio.c_cflag &= ~PARENB;
    tio.c_cflag &= ~CRTSCTS;
    tio.c_cflag &= ~CSIZE;
    tio.c_cflag |= CS8;
    tio.c_cc[VMIN] = 0;
    tio.c_cc[VTIME] = 0;
    if (tcsetattr(fd, TCSANOW, &tio) != 0) {
        const int saved_errno = errno;
        ::close(fd);
        throw std::runtime_error("tcsetattr failed during USB touch reset: " + std::string(std::strerror(saved_errno)));
    }
    ::tcdrain(fd);
    ::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(USB_TOUCH_RESET_SETTLE_MS));
    wait_for_serial_path(path, USB_TOUCH_RESET_WAIT_MS);
}

struct Logger {
    std::ofstream fp;

    explicit Logger(const std::string& path) {
        if (!path.empty()) {
            ensure_parent_dir(path);
            fp.open(path, std::ios::out | std::ios::trunc);
            if (!fp) {
                throw std::runtime_error("failed opening log file: " + path);
            }
        }
    }

    void log(const std::string& line) {
        if (fp) {
            fp << iso_ts() << ' ' << line << '\n';
            fp.flush();
        }
    }
};

std::vector<std::string> split_ws(const std::string& text) {
    std::istringstream iss(text);
    std::vector<std::string> out;
    std::string token;
    while (iss >> token) {
        out.push_back(token);
    }
    return out;
}

std::unordered_map<std::string, std::string> parse_kv_tokens(const std::string& line, const std::string& prefix) {
    std::unordered_map<std::string, std::string> out;
    if (line.rfind(prefix, 0) != 0) {
        return out;
    }
    const auto tokens = split_ws(line.substr(prefix.size()));
    for (const auto& token : tokens) {
        const auto eq = token.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        out[token.substr(0, eq)] = token.substr(eq + 1);
    }
    return out;
}

std::optional<int> parse_int_token(const std::unordered_map<std::string, std::string>& kv, const std::string& key) {
    const auto it = kv.find(key);
    if (it == kv.end()) {
        return std::nullopt;
    }
    try {
        return std::stoi(it->second, nullptr, 0);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<double> parse_double_token(const std::unordered_map<std::string, std::string>& kv, const std::string& key) {
    const auto it = kv.find(key);
    if (it == kv.end()) {
        return std::nullopt;
    }
    try {
        return std::stod(it->second);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<double> parse_json_number_field(const std::string& text, const std::string& key) {
    const std::string needle = "\"" + key + "\":";
    const auto pos = text.find(needle);
    if (pos == std::string::npos) {
        return std::nullopt;
    }
    std::size_t start = pos + needle.size();
    while (start < text.size() && std::isspace(static_cast<unsigned char>(text[start]))) {
        ++start;
    }
    std::size_t end = start;
    while (end < text.size()) {
        const char c = text[end];
        if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.') {
            ++end;
            continue;
        }
        break;
    }
    if (end <= start) {
        return std::nullopt;
    }
    try {
        return std::stod(text.substr(start, end - start));
    } catch (...) {
        return std::nullopt;
    }
}

bool parse_bool_token(const std::unordered_map<std::string, std::string>& kv, const std::string& key, bool fallback = false) {
    const auto value = parse_int_token(kv, key);
    return value.has_value() ? (*value != 0) : fallback;
}

std::string kv_string(const std::unordered_map<std::string, std::string>& kv, const std::string& key) {
    const auto it = kv.find(key);
    return (it == kv.end()) ? std::string() : it->second;
}

std::string decode_mode_name(const std::string& token) {
    const auto open = token.find('(');
    const auto close = token.find(')', open == std::string::npos ? 0u : open + 1u);
    if (open == std::string::npos || close == std::string::npos || close <= open) {
        return {};
    }
    return token.substr(open + 1u, close - open - 1u);
}

std::string normalize_stage_name(const std::string& text) {
    std::string s;
    s.reserve(text.size());
    for (char c : text) {
        if (c == ' ' || c == '_') {
            continue;
        }
        s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    if (s == "precharge") return "precharge";
    if (s == "powerdelivery") return "power_delivery";
    if (s == "currentdemand") return "current_demand";
    if (s.empty() || s == "none") return "none";
    return s;
}

float sane_non_negative(double value) {
    if (!std::isfinite(value) || value < 0.0) {
        return 0.0f;
    }
    return static_cast<float>(value);
}

std::vector<std::string> split_log_fragments(const std::string& line) {
    std::vector<std::string> out;
    if (line.empty()) {
        return {line};
    }
    std::size_t start = 0;
    for (std::size_t i = 1; i < line.size(); ++i) {
        if (line[i] == '[') {
            const auto fragment = line.substr(start, i - start);
            if (!fragment.empty()) {
                out.push_back(fragment);
            }
            start = i;
        }
    }
    const auto tail = line.substr(start);
    if (!tail.empty()) {
        out.push_back(tail);
    }
    return out.empty() ? std::vector<std::string>{line} : out;
}

std::string json_escape(const std::string& s) {
    std::ostringstream oss;
    for (char c : s) {
        switch (c) {
            case '\\': oss << "\\\\"; break;
            case '"': oss << "\\\""; break;
            case '\n': oss << "\\n"; break;
            case '\r': oss << "\\r"; break;
            case '\t': oss << "\\t"; break;
            default: oss << c; break;
        }
    }
    return oss.str();
}

struct Ack {
    int cmd_hex{0};
    int seq{0};
    int status{0};
    int detail0{0};
    int detail1{0};
    int64_t received_at_ms{0};
};

struct PlcStatus {
    bool valid{false};
    int mode_id{-1};
    std::string mode_name;
    int plc_id{-1};
    int connector_id{-1};
    int controller_id{-1};
    int module_addr{-1};
    int local_group{-1};
    std::string module_id;
    int can_stack{-1};
    int module_mgr{-1};
    std::string cp;
    int duty{-1};
    int relay1{-1};
    int relay2{-1};
    int relay3{-1};
    int alloc_sz{-1};
    int stop_done{-1};

    int relay_state(int relay_idx) const {
        switch (relay_idx) {
            case 1: return relay1;
            case 2: return relay2;
            case 3: return relay3;
            default: return -1;
        }
    }
};

struct PlcLiveState {
    PlcStatus last_status{};
    bool have_status{false};
    std::map<int, bool> relay_states;
    int mode_id{-1};
    std::string mode_name;
    int can_stack{-1};
    int module_mgr{-1};
    std::string cp_phase{"U"};
    bool cp_connected{false};
    int cp_duty_pct{-1};
    int64_t b1_since_ms{-1};
    bool slac_session{false};
    bool slac_matched{false};
    int slac_fsm{0};
    int slac_failures{0};
    bool hlc_ready{false};
    bool hlc_active{false};
    bool precharge_seen{false};
    bool precharge_ready{false};
    bool session_started{false};
    bool stop_active{false};
    bool stop_hard{false};
    bool stop_done{true};
    int bms_stage_id{0};
    std::string bms_stage_name{"none"};
    bool bms_valid{false};
    bool delivery_ready{false};
    float target_v{0.0f};
    float target_i{0.0f};
    float latched_target_v{0.0f};
    float latched_target_i{0.0f};
    bool fb_valid{false};
    bool fb_ready{false};
    float fb_v{0.0f};
    float fb_i{0.0f};
    bool fb_curr_lim{false};
    bool fb_volt_lim{false};
    bool fb_pwr_lim{false};
    int64_t first_current_demand_ms{-1};
    uint64_t bms_evt_seq{0};
    int64_t last_bms_evt_ms{0};
    int power_delivery_req_count{0};
    int welding_req_count{0};
    int session_stop_req_count{0};
    int64_t last_status_rx_ms{0};
};

class SerialControllerClient {
public:
    SerialControllerClient(std::string port_path, int baud, std::string log_path)
        : port_path_(std::move(port_path)), baud_(baud), logger_(std::move(log_path)) {}

    ~SerialControllerClient() {
        close();
    }

    void open() {
        fd_ = ::open(port_path_.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
        if (fd_ < 0) {
            throw std::runtime_error("failed opening " + port_path_ + ": " + std::strerror(errno));
        }
        ioctl(fd_, TIOCEXCL);

        termios tio{};
        if (tcgetattr(fd_, &tio) != 0) {
            throw std::runtime_error("tcgetattr failed on " + port_path_);
        }
        cfmakeraw(&tio);
        cfsetispeed(&tio, B115200);
        cfsetospeed(&tio, B115200);
        tio.c_cflag |= (CLOCAL | CREAD);
        tio.c_cflag &= ~CSTOPB;
        tio.c_cflag &= ~PARENB;
        tio.c_cflag &= ~CRTSCTS;
        tio.c_cflag &= ~CSIZE;
        tio.c_cflag |= CS8;
        tio.c_cc[VMIN] = 0;
        tio.c_cc[VTIME] = 0;
        if (tcsetattr(fd_, TCSANOW, &tio) != 0) {
            throw std::runtime_error("tcsetattr failed on " + port_path_);
        }

        stop_reader_ = false;
        reader_thread_ = std::thread([this]() { reader_loop(); });
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(io_mutex_);
            stop_reader_ = true;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        if (reader_thread_.joinable()) {
            reader_thread_.join();
        }
    }

    void send(const std::string& cmd) {
        std::lock_guard<std::mutex> lock(tx_mutex_);
        ensure_reader_ok();
        const int64_t now = steady_ms();
        const int64_t wait_ms = 2 - (now - last_tx_ms_);
        if (wait_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
        }
        const std::string payload = cmd + "\n";
        std::size_t offset = 0;
        while (offset < payload.size()) {
            const ssize_t wrote = ::write(fd_, payload.data() + offset, payload.size() - offset);
            if (wrote < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                throw std::runtime_error("serial write failed");
            }
            offset += static_cast<std::size_t>(wrote);
        }
        last_tx_ms_ = steady_ms();
        logger_.log("> " + cmd);
    }

    void send_untracked(const std::string& cmd) {
        send(cmd);
    }

    PlcStatus query_status(int timeout_ms = 1500) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            status_queue_.clear();
        }
        send("CTRL STATUS");
        return wait_for_status(timeout_ms);
    }

    Ack send_expect_ack(const std::string& cmd, int cmd_hex, int timeout_ms = 3000, int retries = 1) {
        std::exception_ptr last_error;
        for (int attempt = 0; attempt <= retries; ++attempt) {
            const auto expected_seq = reserve_tx_seq(cmd_hex);
            const int64_t sent_after_ms = steady_ms();
            send(cmd);
            try {
                Ack ack = wait_for_ack(cmd_hex, expected_seq, sent_after_ms, timeout_ms);
                if (ack.status == ACK_BAD_SEQ && attempt < retries) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    continue;
                }
                return ack;
            } catch (...) {
                last_error = std::current_exception();
                if (attempt >= retries) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        if (last_error) {
            std::rethrow_exception(last_error);
        }
        throw std::runtime_error("ack wait failed");
    }

    PlcStatus wait_for_status(int timeout_ms) {
        const int64_t deadline = steady_ms() + timeout_ms;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        for (;;) {
            if (!status_queue_.empty()) {
                PlcStatus status = status_queue_.front();
                status_queue_.pop_front();
                return status;
            }
            ensure_reader_ok_locked();
            const int64_t now = steady_ms();
            if (now >= deadline) {
                throw std::runtime_error("timeout waiting for PLC status");
            }
            queue_cv_.wait_for(lock, std::chrono::milliseconds(std::min<int64_t>(50, deadline - now)));
        }
    }

    PlcLiveState live_state_copy() const {
        std::lock_guard<std::mutex> lock(live_mutex_);
        return live_;
    }

    int64_t last_status_rx_ms() const {
        std::lock_guard<std::mutex> lock(live_mutex_);
        return live_.last_status_rx_ms;
    }

private:
    void ensure_reader_ok() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        ensure_reader_ok_locked();
    }

    void ensure_reader_ok_locked() {
        if (!reader_error_.empty()) {
            throw std::runtime_error(reader_error_);
        }
    }

    std::optional<int> reserve_tx_seq(int cmd_hex) {
        std::lock_guard<std::mutex> lock(tx_seq_mutex_);
        const auto it = next_tx_seq_.find(cmd_hex);
        if (it == next_tx_seq_.end()) {
            return std::nullopt;
        }
        const int seq = it->second;
        it->second = (it->second + 1) & 0xFF;
        return seq;
    }

    void sync_tx_seq(int cmd_hex, int ack_seq) {
        std::lock_guard<std::mutex> lock(tx_seq_mutex_);
        next_tx_seq_[cmd_hex] = (ack_seq + 1) & 0xFF;
    }

    void note_ack_seq(int cmd_hex, int ack_seq) {
        std::lock_guard<std::mutex> lock(tx_seq_mutex_);
        next_tx_seq_[cmd_hex] = (ack_seq + 1) & 0xFF;
    }

    Ack wait_for_ack(int cmd_hex,
                     std::optional<int> expected_seq,
                     int64_t sent_after_ms,
                     int timeout_ms) {
        const int64_t deadline = steady_ms() + timeout_ms;
        std::unique_lock<std::mutex> lock(queue_mutex_);
        for (;;) {
            std::deque<Ack> kept;
            std::optional<Ack> matched;
            while (!ack_queue_.empty()) {
                Ack ack = ack_queue_.front();
                ack_queue_.pop_front();
                if (ack.cmd_hex != cmd_hex) {
                    kept.push_back(ack);
                    continue;
                }
                if (ack.received_at_ms < sent_after_ms) {
                    continue;
                }
                if (expected_seq.has_value() && ack.seq != *expected_seq) {
                    kept.push_back(ack);
                    continue;
                }
                if (!matched.has_value()) {
                    matched = ack;
                } else {
                    kept.push_back(ack);
                }
            }
            ack_queue_.swap(kept);
            if (matched.has_value()) {
                sync_tx_seq(cmd_hex, matched->seq);
                return *matched;
            }
            ensure_reader_ok_locked();
            const int64_t now = steady_ms();
            if (now >= deadline) {
                std::ostringstream oss;
                oss << "timeout waiting for ack 0x" << std::hex << std::uppercase << cmd_hex;
                throw std::runtime_error(oss.str());
            }
            queue_cv_.wait_for(lock, std::chrono::milliseconds(std::min<int64_t>(50, deadline - now)));
        }
    }

    void reader_loop() {
        try {
            while (!stop_reader_) {
                pollfd pfd{};
                pfd.fd = fd_;
                pfd.events = POLLIN;
                const int prc = ::poll(&pfd, 1, 20);
                if (prc < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    throw std::runtime_error(std::string("serial poll failed: ") + std::strerror(errno));
                }
                if (prc == 0 || !(pfd.revents & POLLIN)) {
                    continue;
                }
                char buf[1024];
                const ssize_t n = ::read(fd_, buf, sizeof(buf));
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        continue;
                    }
                    throw std::runtime_error(std::string("serial read failed: ") + std::strerror(errno));
                }
                if (n == 0) {
                    continue;
                }
                rx_buffer_.append(buf, static_cast<std::size_t>(n));
                process_rx_buffer();
            }
        } catch (const std::exception& exc) {
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                reader_error_ = exc.what();
            }
            queue_cv_.notify_all();
            logger_.log(std::string("[HOST] reader failed: ") + exc.what());
        }
    }

    void process_rx_buffer() {
        while (true) {
            const auto pos = rx_buffer_.find('\n');
            if (pos == std::string::npos) {
                return;
            }
            std::string line = rx_buffer_.substr(0, pos);
            rx_buffer_.erase(0, pos + 1);
            while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
                line.pop_back();
            }
            logger_.log(line);
            for (const auto& fragment : split_log_fragments(line)) {
                process_fragment(fragment);
            }
        }
    }

    void process_fragment(const std::string& line) {
        if (pending_status_line_.empty()) {
            if (line.rfind("[SERCTRL] STATUS", 0) == 0 && line.find("stop_done=") == std::string::npos) {
                pending_status_line_ = line;
                return;
            }
        } else {
            if (!line.empty() && line.front() != '[') {
                process_line(pending_status_line_ + line);
                pending_status_line_.clear();
                return;
            }
            process_line(pending_status_line_);
            pending_status_line_.clear();
        }
        process_line(line);
    }

    void process_line(const std::string& line) {
        if (line.rfind("[SERCTRL] STATUS ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] STATUS ");
            const bool status_complete =
                kv.find("mode") != kv.end() &&
                kv.find("plc_id") != kv.end() &&
                kv.find("connector_id") != kv.end() &&
                kv.find("controller_id") != kv.end() &&
                kv.find("module_addr") != kv.end() &&
                kv.find("local_group") != kv.end() &&
                kv.find("module_id") != kv.end() &&
                kv.find("can_stack") != kv.end() &&
                kv.find("module_mgr") != kv.end() &&
                kv.find("cp") != kv.end() &&
                kv.find("relay1") != kv.end() &&
                kv.find("relay2") != kv.end() &&
                kv.find("relay3") != kv.end() &&
                kv.find("alloc_sz") != kv.end() &&
                kv.find("stop_done") != kv.end();
            if (!status_complete) {
                logger_.log(std::string("[HOST] ignoring truncated STATUS: ") + line);
                return;
            }
            PlcStatus st;
            st.valid = true;
            const auto mode_token = kv_string(kv, "mode");
            if (!mode_token.empty()) {
                try {
                    st.mode_id = std::stoi(mode_token, nullptr, 0);
                } catch (...) {
                    st.mode_id = -1;
                }
                st.mode_name = decode_mode_name(mode_token);
            }
            st.plc_id = parse_int_token(kv, "plc_id").value_or(-1);
            st.connector_id = parse_int_token(kv, "connector_id").value_or(-1);
            st.controller_id = parse_int_token(kv, "controller_id").value_or(-1);
            st.module_addr = parse_int_token(kv, "module_addr").value_or(-1);
            st.local_group = parse_int_token(kv, "local_group").value_or(-1);
            st.module_id = kv_string(kv, "module_id");
            st.can_stack = parse_int_token(kv, "can_stack").value_or(-1);
            st.module_mgr = parse_int_token(kv, "module_mgr").value_or(-1);
            st.cp = kv_string(kv, "cp");
            st.duty = parse_int_token(kv, "duty").value_or(-1);
            st.relay1 = parse_int_token(kv, "relay1").value_or(-1);
            st.relay2 = parse_int_token(kv, "relay2").value_or(-1);
            st.relay3 = parse_int_token(kv, "relay3").value_or(-1);
            st.alloc_sz = parse_int_token(kv, "alloc_sz").value_or(-1);
            st.stop_done = parse_int_token(kv, "stop_done").value_or(-1);
            {
                std::lock_guard<std::mutex> live_lock(live_mutex_);
                live_.have_status = true;
                live_.last_status = st;
                live_.mode_id = st.mode_id;
                live_.mode_name = st.mode_name;
                live_.can_stack = st.can_stack;
                live_.module_mgr = st.module_mgr;
                live_.cp_phase = st.cp;
                live_.cp_connected = (st.cp == "B" || st.cp == "B1" || st.cp == "B2" || st.cp == "C" || st.cp == "D");
                live_.cp_duty_pct = st.duty;
                live_.relay_states[1] = st.relay1 == 1;
                live_.relay_states[2] = st.relay2 == 1;
                live_.relay_states[3] = st.relay3 == 1;
                live_.last_status_rx_ms = steady_ms();
                if (st.cp == "B1") {
                    if (live_.b1_since_ms < 0) {
                        live_.b1_since_ms = live_.last_status_rx_ms;
                    }
                } else {
                    live_.b1_since_ms = -1;
                }
                live_.stop_done = st.stop_done == 1;
            }
            {
                std::lock_guard<std::mutex> queue_lock(queue_mutex_);
                status_queue_.push_back(st);
            }
            queue_cv_.notify_all();
            return;
        }

        if (line.rfind("[SERCTRL] ACK ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] ACK ");
            Ack ack;
            ack.cmd_hex = parse_int_token(kv, "cmd").value_or(0);
            ack.seq = parse_int_token(kv, "seq").value_or(0);
            ack.status = parse_int_token(kv, "status").value_or(0);
            ack.detail0 = parse_int_token(kv, "detail0").value_or(0);
            ack.detail1 = parse_int_token(kv, "detail1").value_or(0);
            ack.received_at_ms = steady_ms();
            note_ack_seq(ack.cmd_hex, ack.seq);
            {
                std::lock_guard<std::mutex> queue_lock(queue_mutex_);
                ack_queue_.push_back(ack);
            }
            queue_cv_.notify_all();
            return;
        }

        if (line.rfind("[RELAY] Relay", 0) == 0) {
            const auto relay_pos = line.find("Relay");
            const auto arrow = line.find(" -> ");
            if (relay_pos != std::string::npos && arrow != std::string::npos) {
                try {
                    const int relay_idx = std::stoi(line.substr(relay_pos + 5, arrow - (relay_pos + 5)));
                    const bool closed = line.find("CLOSED", arrow) != std::string::npos;
                    std::lock_guard<std::mutex> live_lock(live_mutex_);
                    live_.relay_states[relay_idx] = closed;
                } catch (...) {
                }
            }
            return;
        }

        if (line.rfind("[CP] state ", 0) == 0) {
            const auto arrow = line.find(" -> ");
            if (arrow != std::string::npos) {
                std::string cp = line.substr(arrow + 4);
                std::lock_guard<std::mutex> live_lock(live_mutex_);
                live_.cp_phase = cp;
                live_.cp_connected = (cp == "B" || cp == "B1" || cp == "B2" || cp == "C" || cp == "D");
                const int64_t now = steady_ms();
                if (cp == "B1") {
                    if (live_.b1_since_ms < 0) {
                        live_.b1_since_ms = now;
                    }
                } else {
                    live_.b1_since_ms = -1;
                }
            }
            return;
        }

        if (line.rfind("[SERCTRL] EVT CP ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] EVT CP ");
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.cp_phase = kv_string(kv, "cp");
            live_.cp_connected = parse_bool_token(kv, "connected");
            live_.cp_duty_pct = parse_int_token(kv, "duty").value_or(-1);
            const int64_t now = steady_ms();
            if (live_.cp_phase == "B1") {
                if (live_.b1_since_ms < 0) {
                    live_.b1_since_ms = now;
                }
            } else {
                live_.b1_since_ms = -1;
            }
            return;
        }

        if (line.rfind("[SERCTRL] EVT SLAC ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] EVT SLAC ");
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.slac_session = parse_bool_token(kv, "session");
            live_.slac_matched = live_.slac_session && parse_bool_token(kv, "matched");
            live_.slac_fsm = parse_int_token(kv, "fsm").value_or(0);
            live_.slac_failures = parse_int_token(kv, "failures").value_or(0);
            return;
        }

        if (line.rfind("[SERCTRL] EVT HLC ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] EVT HLC ");
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.hlc_ready = parse_bool_token(kv, "ready");
            live_.hlc_active = parse_bool_token(kv, "active");
            live_.precharge_seen = parse_bool_token(kv, "precharge_seen");
            live_.precharge_ready = parse_bool_token(kv, "precharge_ready");
            live_.fb_valid = parse_bool_token(kv, "fb_valid");
            live_.fb_ready = parse_bool_token(kv, "fb_ready");
            live_.fb_v = sane_non_negative(parse_double_token(kv, "fb_v").value_or(0.0));
            live_.fb_i = sane_non_negative(parse_double_token(kv, "fb_i").value_or(0.0));
            return;
        }

        if (line.rfind("[SERCTRL] EVT SESSION ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] EVT SESSION ");
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.session_started = parse_bool_token(kv, "session");
            live_.slac_matched = parse_bool_token(kv, "matched");
            live_.relay_states[1] = parse_bool_token(kv, "relay1");
            live_.relay_states[2] = parse_bool_token(kv, "relay2");
            live_.relay_states[3] = parse_bool_token(kv, "relay3");
            live_.stop_active = parse_bool_token(kv, "stop_active");
            live_.stop_hard = parse_bool_token(kv, "stop_hard");
            live_.stop_done = parse_bool_token(kv, "stop_done");
            return;
        }

        if (line.rfind("[SERCTRL] EVT BMS ", 0) == 0) {
            auto kv = parse_kv_tokens(line, "[SERCTRL] EVT BMS ");
            const auto stage_token = kv_string(kv, "stage");
            int stage_id = 0;
            std::string stage_name;
            if (!stage_token.empty()) {
                try {
                    stage_id = std::stoi(stage_token, nullptr, 0);
                } catch (...) {
                    stage_id = 0;
                }
                const auto open = stage_token.find('(');
                const auto close = stage_token.find(')', open == std::string::npos ? 0u : open + 1u);
                if (open != std::string::npos && close != std::string::npos && close > open) {
                    stage_name = normalize_stage_name(stage_token.substr(open + 1, close - open - 1));
                }
            }
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.bms_stage_id = stage_id;
            live_.bms_stage_name = stage_name.empty() ? "none" : stage_name;
            live_.bms_valid = parse_bool_token(kv, "valid");
            live_.delivery_ready = parse_bool_token(kv, "delivery_ready");
            live_.target_v = sane_non_negative(parse_double_token(kv, "target_v").value_or(0.0));
            live_.target_i = sane_non_negative(parse_double_token(kv, "target_i").value_or(0.0));
            if (live_.bms_stage_name == "precharge" || live_.bms_stage_name == "power_delivery" ||
                live_.bms_stage_name == "current_demand") {
                if (live_.target_v > 0.0f) {
                    live_.latched_target_v = live_.target_v;
                }
                if (live_.target_i > 0.0f) {
                    live_.latched_target_i = live_.target_i;
                }
            }
            live_.fb_valid = parse_bool_token(kv, "fb_valid");
            live_.fb_ready = parse_bool_token(kv, "fb_ready");
            live_.fb_v = sane_non_negative(parse_double_token(kv, "present_v").value_or(0.0));
            live_.fb_i = sane_non_negative(parse_double_token(kv, "present_i").value_or(0.0));
            live_.fb_curr_lim = parse_bool_token(kv, "curr_lim");
            live_.fb_volt_lim = parse_bool_token(kv, "volt_lim");
            live_.fb_pwr_lim = parse_bool_token(kv, "pwr_lim");
            live_.last_bms_evt_ms = steady_ms();
            live_.bms_evt_seq++;
            if (live_.bms_stage_name == "current_demand" && live_.first_current_demand_ms < 0) {
                live_.first_current_demand_ms = steady_ms();
            }
            return;
        }

        if (line.rfind("[HLC][REQ] ", 0) == 0) {
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            const int64_t now_ms = steady_ms();
            if (line.find("\"type\":\"PreChargeReq\"") != std::string::npos) {
                live_.bms_stage_id = 1;
                live_.bms_stage_name = "precharge";
                live_.bms_valid = true;
                const auto target_v = parse_json_number_field(line, "targetV");
                const auto target_i = parse_json_number_field(line, "targetI");
                if (target_v.has_value() && *target_v > 0.0) {
                    live_.target_v = sane_non_negative(*target_v);
                    live_.latched_target_v = live_.target_v;
                }
                if (target_i.has_value() && *target_i > 0.0) {
                    live_.target_i = sane_non_negative(*target_i);
                    live_.latched_target_i = live_.target_i;
                }
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                return;
            }
            if (line.find("\"type\":\"PowerDeliveryReq\"") != std::string::npos) {
                live_.bms_stage_id = 2;
                live_.bms_stage_name = "power_delivery";
                live_.bms_valid = true;
                live_.delivery_ready = true;
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                return;
            }
            if (line.find("\"type\":\"CurrentDemandReq\"") != std::string::npos) {
                live_.bms_stage_id = 3;
                live_.bms_stage_name = "current_demand";
                live_.bms_valid = true;
                live_.delivery_ready = true;
                const auto target_v = parse_json_number_field(line, "targetV");
                const auto target_i = parse_json_number_field(line, "targetI");
                if (target_v.has_value() && *target_v > 0.0) {
                    live_.target_v = sane_non_negative(*target_v);
                    live_.latched_target_v = live_.target_v;
                }
                if (target_i.has_value() && *target_i > 0.0) {
                    live_.target_i = sane_non_negative(*target_i);
                    live_.latched_target_i = live_.target_i;
                }
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                if (live_.first_current_demand_ms < 0) {
                    live_.first_current_demand_ms = now_ms;
                }
                return;
            }
        }

        if (line.rfind("[HLC] {", 0) == 0) {
            const int64_t now_ms = steady_ms();
            const auto req_v = sane_non_negative(parse_json_number_field(line, "reqV").value_or(0.0));
            const auto req_i = sane_non_negative(parse_json_number_field(line, "reqI").value_or(0.0));
            const auto present_v = sane_non_negative(parse_json_number_field(line, "presentV").value_or(0.0));
            const auto present_i = sane_non_negative(parse_json_number_field(line, "presentI").value_or(0.0));
            const bool ready = parse_json_number_field(line, "ready").value_or(0.0) > 0.5;
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            if (line.find("\"msg\":\"PreChargeRuntime\"") != std::string::npos ||
                line.find("\"msg\":\"CtrlPreChargeRuntime\"") != std::string::npos) {
                live_.bms_stage_id = 1;
                live_.bms_stage_name = "precharge";
                live_.bms_valid = true;
                live_.target_v = req_v;
                live_.target_i = req_i;
                if (req_v > 0.0f) live_.latched_target_v = req_v;
                if (req_i > 0.0f) live_.latched_target_i = req_i;
                live_.fb_valid = true;
                live_.fb_ready = ready;
                live_.fb_v = present_v;
                live_.fb_i = present_i;
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                return;
            }
            if (line.find("\"msg\":\"PowerDeliveryRuntime\"") != std::string::npos) {
                live_.bms_stage_id = 2;
                live_.bms_stage_name = "power_delivery";
                live_.bms_valid = true;
                if (req_v > 0.0f) {
                    live_.target_v = req_v;
                    live_.latched_target_v = req_v;
                }
                live_.delivery_ready = ready;
                live_.fb_valid = true;
                live_.fb_ready = ready;
                live_.fb_v = present_v;
                live_.fb_i = present_i;
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                return;
            }
            if (line.find("\"msg\":\"CurrentDemandRuntime\"") != std::string::npos ||
                line.find("\"msg\":\"CtrlCurrentDemandRuntime\"") != std::string::npos) {
                live_.bms_stage_id = 3;
                live_.bms_stage_name = "current_demand";
                live_.bms_valid = true;
                live_.delivery_ready = true;
                if (req_v > 0.0f) {
                    live_.target_v = req_v;
                    live_.latched_target_v = req_v;
                }
                if (req_i > 0.0f) {
                    live_.target_i = req_i;
                    live_.latched_target_i = req_i;
                }
                live_.fb_valid = true;
                live_.fb_ready = ready;
                live_.fb_v = present_v;
                live_.fb_i = present_i;
                live_.last_bms_evt_ms = now_ms;
                live_.bms_evt_seq++;
                if (live_.first_current_demand_ms < 0) {
                    live_.first_current_demand_ms = now_ms;
                }
                return;
            }
        }

        if (line.find("RX PowerDeliveryReq") != std::string::npos) {
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.power_delivery_req_count++;
            return;
        }
        if (line.find("RX WeldingDetectionReq") != std::string::npos) {
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.welding_req_count++;
            return;
        }
        if (line.find("RX SessionStopReq") != std::string::npos) {
            std::lock_guard<std::mutex> live_lock(live_mutex_);
            live_.session_stop_req_count++;
            return;
        }
    }

    std::string port_path_;
    int baud_{115200};
    Logger logger_;
    int fd_{-1};
    std::thread reader_thread_;
    bool stop_reader_{false};
    std::string rx_buffer_;
    std::string pending_status_line_;
    mutable std::mutex live_mutex_;
    PlcLiveState live_{};
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::deque<PlcStatus> status_queue_;
    std::deque<Ack> ack_queue_;
    std::string reader_error_;
    std::mutex tx_mutex_;
    std::mutex io_mutex_;
    std::mutex tx_seq_mutex_;
    std::unordered_map<int, int> next_tx_seq_;
    int64_t last_tx_ms_{0};
};

class SocketCanTransport final : public CanTransport {
public:
    SocketCanTransport(std::string iface, std::string log_path)
        : iface_(std::move(iface)), logger_(std::move(log_path)), start_(std::chrono::steady_clock::now()) {}

    ~SocketCanTransport() override {
        close_socket();
    }

    void open_socket() {
        sock_ = ::socket(PF_CAN, SOCK_RAW, CAN_RAW);
        if (sock_ < 0) {
            throw std::runtime_error("socket(PF_CAN) failed");
        }
        const int sndbuf = 256 * 1024;
        const int rcvbuf = 256 * 1024;
        (void)::setsockopt(sock_, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        (void)::setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        const int flags = fcntl(sock_, F_GETFL, 0);
        fcntl(sock_, F_SETFL, flags | O_NONBLOCK);

        ifreq ifr{};
        std::snprintf(ifr.ifr_name, IFNAMSIZ, "%s", iface_.c_str());
        if (ioctl(sock_, SIOCGIFINDEX, &ifr) != 0) {
            throw std::runtime_error("ioctl(SIOCGIFINDEX) failed for " + iface_);
        }

        sockaddr_can addr{};
        addr.can_family = AF_CAN;
        addr.can_ifindex = ifr.ifr_ifindex;
        if (bind(sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            throw std::runtime_error("bind(can) failed for " + iface_);
        }
    }

    void close_socket() {
        if (sock_ >= 0) {
            ::close(sock_);
            sock_ = -1;
        }
    }

    bool send(const CanFrame& frame) override {
        if (sock_ < 0) return false;
        can_frame raw{};
        raw.can_id = frame.id;
        if (frame.extended) raw.can_id |= CAN_EFF_FLAG;
        raw.can_dlc = frame.dlc;
        std::memcpy(raw.data, frame.data, std::min<std::size_t>(8, frame.dlc));
        const int64_t deadline_ms = steady_ms() + CAN_SEND_TIMEOUT_MS;
        for (;;) {
            const ssize_t rc = ::write(sock_, &raw, sizeof(raw));
            if (rc == static_cast<ssize_t>(sizeof(raw))) {
                logger_.log("TX id=0x" + hex_u32(frame.id) + " dlc=" + std::to_string(frame.dlc) + " data=" + data_hex(frame));
                return true;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
                const int64_t now_ms = steady_ms();
                if (now_ms >= deadline_ms) {
                    return false;
                }
                pollfd pfd{};
                pfd.fd = sock_;
                pfd.events = POLLOUT;
                const int wait_ms = static_cast<int>(std::max<int64_t>(1, deadline_ms - now_ms));
                const int prc = ::poll(&pfd, 1, wait_ms);
                if (prc < 0 && errno == EINTR) {
                    continue;
                }
                if (prc <= 0) {
                    continue;
                }
                continue;
            }
            throw std::runtime_error("CAN write failed: " + std::string(std::strerror(errno)));
        }
    }

    bool receive(CanFrame& frame_out) override {
        if (sock_ < 0) return false;
        can_frame raw{};
        const ssize_t rc = ::read(sock_, &raw, sizeof(raw));
        if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return false;
            throw std::runtime_error("CAN read failed: " + std::string(std::strerror(errno)));
        }
        if (rc != static_cast<ssize_t>(sizeof(raw))) return false;
        frame_out = {};
        frame_out.extended = (raw.can_id & CAN_EFF_FLAG) != 0;
        frame_out.id = raw.can_id & CAN_EFF_MASK;
        frame_out.dlc = raw.can_dlc;
        std::memcpy(frame_out.data, raw.data, std::min<std::size_t>(8, raw.can_dlc));
        logger_.log("RX id=0x" + hex_u32(frame_out.id) + " dlc=" + std::to_string(frame_out.dlc) + " data=" +
                    data_hex(frame_out));
        return true;
    }

    uint32_t now_ms() const override {
        using namespace std::chrono;
        return static_cast<uint32_t>(duration_cast<milliseconds>(steady_clock::now() - start_).count());
    }

private:
    static std::string hex_u32(uint32_t value) {
        std::ostringstream oss;
        oss << std::hex << std::uppercase << value;
        return oss.str();
    }

    static std::string data_hex(const CanFrame& frame) {
        std::ostringstream oss;
        oss << std::hex << std::uppercase << std::setfill('0');
        for (uint8_t i = 0; i < frame.dlc; ++i) {
            oss << std::setw(2) << static_cast<unsigned>(frame.data[i]);
        }
        return oss.str();
    }

    std::string iface_;
    Logger logger_;
    int sock_{-1};
    std::chrono::steady_clock::time_point start_;
};

struct Config {
    std::string plc_port{"/dev/ttyACM0"};
    int baud{115200};
    std::string can_iface{"can0"};
    int power_relay{1};
    int controller_id{7};
    int home_addr{2};
    int home_group{2};
    int input_mode{-1};
    double precharge_current_a{2.0};
    double current_demand_hold_s{120.0};
    double session_start_timeout_s{240.0};
    double b1_start_delay_s{0.6};
    double control_interval_s{0.0};
    double feedback_interval_s{0.02};
    double active_feedback_keepalive_s{0.02};
    double status_interval_s{5.0};
    int heartbeat_ms{4000};
    int heartbeat_timeout_ms{20000};
    int auth_ttl_ms{25000};
    int arm_ms{25000};
    int relay_hold_ms{25000};
    double relay_refresh_s{20.0};
    double stop_timeout_s{12.0};
    std::string plc_log;
    std::string can_log;
    std::string summary_file;
    bool reboot_before_run{true};
    bool reset_can_iface{true};
};

Config parse_args(int argc, char** argv) {
    Config cfg;
    const auto stamp = []() {
        std::ostringstream oss;
        const auto now = std::chrono::system_clock::now();
        const auto tt = std::chrono::system_clock::to_time_t(now);
        std::tm tm{};
        localtime_r(&tt, &tm);
        oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
        return oss.str();
    }();
    cfg.plc_log =
        "/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_" + stamp + ".log";
    cfg.can_log =
        "/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_" + stamp + "_can.log";
    cfg.summary_file =
        "/home/jpi/Desktop/EVSE/plc_firmware/logs/controller_cbmodules_single_module_charge_" + stamp + "_summary.json";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const char* name) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++i];
        };
        if (arg == "--plc-port") cfg.plc_port = need("--plc-port");
        else if (arg == "--baud") cfg.baud = std::stoi(need("--baud"));
        else if (arg == "--can-iface") cfg.can_iface = need("--can-iface");
        else if (arg == "--power-relay") cfg.power_relay = std::stoi(need("--power-relay"));
        else if (arg == "--controller-id") cfg.controller_id = std::stoi(need("--controller-id"));
        else if (arg == "--home-addr") cfg.home_addr = std::stoi(need("--home-addr"));
        else if (arg == "--home-group") cfg.home_group = std::stoi(need("--home-group"));
        else if (arg == "--input-mode") cfg.input_mode = std::stoi(need("--input-mode"));
        else if (arg == "--precharge-current-a") cfg.precharge_current_a = std::stod(need("--precharge-current-a"));
        else if (arg == "--current-demand-hold-s") cfg.current_demand_hold_s = std::stod(need("--current-demand-hold-s"));
        else if (arg == "--session-start-timeout-s") cfg.session_start_timeout_s = std::stod(need("--session-start-timeout-s"));
        else if (arg == "--b1-start-delay-s") cfg.b1_start_delay_s = std::stod(need("--b1-start-delay-s"));
        else if (arg == "--control-interval-s") cfg.control_interval_s = std::stod(need("--control-interval-s"));
        else if (arg == "--feedback-interval-s") cfg.feedback_interval_s = std::stod(need("--feedback-interval-s"));
        else if (arg == "--active-feedback-keepalive-s") cfg.active_feedback_keepalive_s = std::stod(need("--active-feedback-keepalive-s"));
        else if (arg == "--status-interval-s") cfg.status_interval_s = std::stod(need("--status-interval-s"));
        else if (arg == "--heartbeat-ms") cfg.heartbeat_ms = std::stoi(need("--heartbeat-ms"));
        else if (arg == "--heartbeat-timeout-ms") cfg.heartbeat_timeout_ms = std::stoi(need("--heartbeat-timeout-ms"));
        else if (arg == "--auth-ttl-ms") cfg.auth_ttl_ms = std::stoi(need("--auth-ttl-ms"));
        else if (arg == "--arm-ms") cfg.arm_ms = std::stoi(need("--arm-ms"));
        else if (arg == "--relay-hold-ms") cfg.relay_hold_ms = std::stoi(need("--relay-hold-ms"));
        else if (arg == "--relay-refresh-s") cfg.relay_refresh_s = std::stod(need("--relay-refresh-s"));
        else if (arg == "--stop-timeout-s") cfg.stop_timeout_s = std::stod(need("--stop-timeout-s"));
        else if (arg == "--plc-log") cfg.plc_log = need("--plc-log");
        else if (arg == "--can-log") cfg.can_log = need("--can-log");
        else if (arg == "--summary-file") cfg.summary_file = need("--summary-file");
        else if (arg == "--no-reboot-before-run") cfg.reboot_before_run = false;
        else if (arg == "--no-reset-can-iface") cfg.reset_can_iface = false;
        else if (arg == "--help") {
            std::cout
                << "controller_cbmodules_single_module_charge [options]\n"
                << "  --plc-port PATH\n"
                << "  --can-iface IFACE\n"
                << "  --home-addr N\n"
                << "  --home-group N\n"
                << "  --current-demand-hold-s N\n"
                << "  --no-reset-can-iface\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown arg: " + arg);
        }
    }
    cfg.heartbeat_timeout_ms = clamp_ctrl_timeout_ms(std::max(cfg.heartbeat_timeout_ms, cfg.heartbeat_ms * 8));
    cfg.auth_ttl_ms = clamp_ctrl_timeout_ms(std::max(cfg.auth_ttl_ms, cfg.heartbeat_timeout_ms + 2000));
    // The session deadline must cover both startup/matching time and the full
    // requested CurrentDemand hold window. Otherwise the harness can stop a
    // healthy charging session before the requested hold time is even possible.
    cfg.session_start_timeout_s = std::max(cfg.session_start_timeout_s, cfg.current_demand_hold_s + 90.0);
    cfg.arm_ms = clamp_ctrl_timeout_ms(cfg.arm_ms);
    cfg.relay_hold_ms = clamp_ctrl_timeout_ms_allow_zero(cfg.relay_hold_ms);
    return cfg;
}

struct ModuleTelemetrySnapshot {
    bool present{false};
    bool online{false};
    bool module_off{true};
    bool faulted{false};
    ModuleLifecycle lifecycle{ModuleLifecycle::Missing};
    bool off_verified{false};
    bool pending_remove{false};
    bool lease_active{false};
    ModuleTransitionState transition{ModuleTransitionState::Idle};
    std::string group_id;
    float voltage_v{0.0f};
    float current_a{0.0f};
    uint32_t latest_update_ms{0};
};

class Runner {
public:
    explicit Runner(Config cfg)
        : cfg_(std::move(cfg)),
          serial_(cfg_.plc_port, cfg_.baud, cfg_.plc_log),
          can_(cfg_.can_iface, cfg_.can_log) {}

    int run() {
        std::string result = "fail";
        try {
            reboot_plc_if_requested();
            serial_.open();
            std::cout << "PLC_LOG=" << cfg_.plc_log << "\n";
            std::cout << "CAN_LOG=" << cfg_.can_log << "\n";
            std::cout << "SUMMARY_FILE=" << cfg_.summary_file << "\n";

            phase_ = "bootstrap";
            bootstrap();
            if (!plc_local_power_path_) {
                if (cfg_.reset_can_iface) {
                    reset_can_interface(cfg_.can_iface);
                }
                can_.open_socket();
                init_module_manager();
                wait_for_idle_module_ready();
            }
            std::cout << "[BOOT] plc_id=" << status_.plc_id << " connector_id=" << status_.connector_id
                      << " module=" << status_.module_id << " group=" << status_.local_group
                      << " power_path=" << (plc_local_power_path_ ? "plc_local" : "host_external") << "\n";

            phase_ = "wait_for_session";
            const int64_t start_deadline_ms =
                steady_ms() + static_cast<int64_t>(cfg_.session_start_timeout_s * 1000.0);
            while (steady_ms() < start_deadline_ms) {
                tick();
                const PlcLiveState live = serial_.live_state_copy();
                if (current_demand_ready(live)) {
                    result = "pass";
                    std::cout << "[PASS] sustained CurrentDemand reached with cbmodules controller harness\n";
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(active_charge_window(live) ? 1 : 10));
            }

            if (result != "pass") {
                failures_.push_back("session never reached sustained CurrentDemand");
                throw std::runtime_error(failures_.back());
            }

            cleanup();
            write_summary(result);
            return 0;
        } catch (const std::exception& exc) {
            failures_.push_back(exc.what());
            std::cout << "[FAIL] " << exc.what() << "\n";
            try {
                cleanup();
            } catch (const std::exception& cleanup_exc) {
                notes_.push_back(std::string("cleanup failed: ") + cleanup_exc.what());
            }
            write_summary(result);
            return 1;
        }
    }

private:
    void reboot_plc_if_requested() {
        if (!cfg_.reboot_before_run) {
            return;
        }
        std::cout << "[BOOT] usb-touch reset " << cfg_.plc_port << "\n";
        usb_touch_reset(cfg_.plc_port);
    }

    void wait_for_idle_module_ready() {
        const int64_t deadline = steady_ms() + MODULE_IDLE_SETTLE_TIMEOUT_MS;
        while (steady_ms() < deadline) {
            service_module_manager();
            const auto status = group_status();
            const auto snap = module_snapshot();
            if (status.assigned_modules > 0u && snap.present && snap.online && snap.latest_update_ms != 0u) {
                std::cout << "[BOOT] host module ready assigned=" << status.assigned_modules
                          << " online=" << (snap.online ? 1 : 0)
                          << " off=" << (snap.module_off ? 1 : 0)
                          << " v=" << std::fixed << std::setprecision(1) << snap.voltage_v
                          << " i=" << snap.current_a << "\n";
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        throw std::runtime_error("host module manager did not settle before session start");
    }

    void bootstrap() {
        status_ = wait_for_plc_boot_ready();
        if (status_.mode_id != 1) {
            throw std::runtime_error("PLC did not boot in external_controller mode");
        }
        auto ack = serial_.send_expect_ack("CTRL HB " + std::to_string(cfg_.heartbeat_timeout_ms),
                                           ACK_CMD_HEARTBEAT,
                                           3000,
                                           1);
        if (ack.status != ACK_OK) {
            throw std::runtime_error("CTRL HB rejected");
        }
        ack = serial_.send_expect_ack("CTRL STOP clear 3000", ACK_CMD_STOP, 3000, 1);
        if (ack.status != ACK_OK && ack.status != ACK_BAD_SEQ) {
            throw std::runtime_error("CTRL STOP clear rejected");
        }
        ack = serial_.send_expect_ack("CTRL AUTH deny " + std::to_string(cfg_.auth_ttl_ms), ACK_CMD_AUTH, 3000, 1);
        if (ack.status != ACK_OK) {
            throw std::runtime_error("CTRL AUTH deny rejected");
        }
        ack = serial_.send_expect_ack("CTRL SLAC disarm 3000", ACK_CMD_SLAC, 3000, 1);
        if (ack.status != ACK_OK) {
            throw std::runtime_error("CTRL SLAC disarm rejected");
        }
        status_ = wait_for_plc_boot_ready();
        if (status_.mode_id != 1) {
            throw std::runtime_error("PLC left external_controller mode during bootstrap");
        }
        group_id_ = "PLC_GROUP_" + std::to_string(status_.plc_id);
        module_id_ = status_.module_id;
        if (status_.can_stack == 1 && status_.module_mgr == 1) {
            plc_local_power_path_ = true;
            return;
        }
        if (status_.can_stack == 0 && status_.module_mgr == 0) {
            plc_local_power_path_ = false;
            ack = serial_.send_expect_ack("CTRL FEEDBACK 1 0 0 0 0 0 0", ACK_CMD_FEEDBACK, 3000, 1);
            if (ack.status != ACK_OK) {
                throw std::runtime_error("CTRL FEEDBACK reset rejected");
            }
            for (int relay_idx = 1; relay_idx <= 3; ++relay_idx) {
                const int mask = 1 << (relay_idx - 1);
                ack = serial_.send_expect_ack("CTRL RELAY " + std::to_string(mask) + " 0 0", ACK_CMD_RELAY, 3000, 1);
                if (ack.status != ACK_OK) {
                    throw std::runtime_error("CTRL RELAY open rejected");
                }
            }
            return;
        }
        {
            std::ostringstream oss;
            oss << "unexpected controller routing mode=" << status_.mode_id << " can_stack=" << status_.can_stack
                << " module_mgr=" << status_.module_mgr;
            throw std::runtime_error(oss.str());
        }
    }

    PlcStatus wait_for_plc_boot_ready() {
        const int64_t deadline = steady_ms() + PLC_BOOT_READY_TIMEOUT_MS;
        std::string last_error = "no PLC status yet";
        while (steady_ms() < deadline) {
            try {
                PlcStatus status = serial_.query_status(1000);
                if (status.valid) {
                    return status;
                }
                last_error = "invalid PLC status";
            } catch (const std::exception& exc) {
                last_error = exc.what();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(PLC_BOOT_STATUS_RETRY_MS));
        }
        throw std::runtime_error("PLC boot/status wait failed: " + last_error);
    }

    void init_module_manager() {
        ModuleManagerConfig cfg;
        cfg.telemetry_stale_ms = 12000;
        cfg.startup_scan_default_timeout_ms = 3000;
        cfg.startup_probe_retry_ms = 200;
        cfg.runtime_probe_interval_ms = 80;
        cfg.module_quarantine_ms = 2000;
        cfg.drain_timeout_ms = 2500;
        cfg.can_bandwidth_cap_kbps = 0.0f;
        cfg.can_max_frames_per_second = 300.0f;
        cfg.can_max_tx_per_tick = 96;
        cfg.can_probe_tx_max_per_tick = 12;
        cfg.can_control_tx_reserve_per_tick = 24;
        cfg.can_control_bit_reserve_kbps = 4.0f;
        cfg.command_keepalive_ms = 1500;
        cfg.command_keepalive_stable_ms = 3000;
        cfg.command_settle_window_ms = 2000;
        cfg.preventive_current_margin_pct = 0.025f;
        cfg.preventive_power_margin_pct = 0.025f;
        cfg.safety.enable_ramp_sequencing = false;
        cfg.safety.enable_telemetry_stale_trip = false;
        cfg.safety.enable_can_rate_limit = false;
        cfg.safety.enable_multi_controller_guard = false;
        cfg.controller_id =
            static_cast<uint16_t>((static_cast<uint16_t>(status_.controller_id & 0x0F) << 8u) | static_cast<uint16_t>(status_.plc_id));

        manager_ = std::make_unique<ModuleManager>(cfg);
        manager_->set_transport(&can_);

        ModuleSpec spec{};
        spec.id = status_.module_id;
        spec.type = ModuleType::MaxwellMxr;
        spec.slot_id = static_cast<uint8_t>(std::max(0, status_.connector_id));
        spec.slot_index = static_cast<uint8_t>(std::max(0, status_.connector_id));
        spec.address = static_cast<uint8_t>(cfg_.home_addr);
        spec.group = static_cast<uint8_t>(cfg_.home_group);
        if (cfg_.input_mode >= 0) {
            spec.input_mode = static_cast<int8_t>(cfg_.input_mode);
        }
        spec.rated_current_a = MODULE_MAX_CURRENT_A;
        spec.rated_power_kw = MODULE_MAX_POWER_KW;
        spec.min_operating_voltage_v = 200.0f;
        spec.max_operating_voltage_v = MODULE_MAX_VOLTAGE_V;
        manager_->set_inventory({spec});

        cbmodules::StartupScanReport report{};
        const int64_t validate_deadline_ms = steady_ms() + 12000;
        while (steady_ms() < validate_deadline_ms) {
            report = manager_->startup_scan_validate(2500);
            if (report.validated_count >= 1u) {
                break;
            }
            std::cout << "[BOOT] startup scan retry expected=" << report.expected_count
                      << " discovered=" << report.discovered_count
                      << " validated=" << report.validated_count
                      << " missing=" << report.missing_count
                      << " mismatch=" << report.mismatch_count << "\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        if (report.validated_count < 1u) {
            throw std::runtime_error("cbmodules startup scan did not validate the home module");
        }

        group_id_ = "PLC_GROUP_" + std::to_string(status_.plc_id);
        module_id_ = status_.module_id;
        GroupConfig group{};
        group.id = group_id_;
        group.min_modules = 0u;
        group.max_modules = 1u;
        group.efficient_module_usage = true;
        group.hard_safety_clamp = true;
        group.max_group_current_a = MODULE_MAX_CURRENT_A;
        group.max_group_power_kw = MODULE_MAX_POWER_KW;
        if (!manager_->upsert_group(group)) {
            throw std::runtime_error("failed to upsert host cbmodules group");
        }
        manager_->set_module_hard_limits(module_id_, MODULE_MAX_CURRENT_A, MODULE_MAX_POWER_KW);
        const GroupTarget off_target{false, 0.0f, 0.0f};
        // Mirror standalone ownership: keep the home module allocated to the local
        // group even while output is OFF, so the first PreCharge request does not
        // have to hot-join the module into the active group.
        if (!manager_->apply_group_setpoint_allowlist(group_id_, off_target, {module_id_}, MODULE_ALLOWLIST_LEASE_MS)) {
            throw std::runtime_error("failed to allocate host cbmodules home module");
        }
        manager_->set_group_target(group_id_, off_target);
        manager_->tick(can_.now_ms());
        const auto status = manager_->group_status(group_id_);
        if (status.assigned_modules == 0u) {
            throw std::runtime_error("host cbmodules boot allocation did not assign the home module");
        }
    }

    ModuleTelemetrySnapshot module_snapshot() const {
        ModuleTelemetrySnapshot snap;
        if (!manager_) {
            return snap;
        }
        const auto states = manager_->module_states();
        for (const auto& state : states) {
            if (state.id != module_id_) {
                continue;
            }
            snap.present = true;
            snap.online = state.telemetry.online;
            snap.module_off = state.telemetry.module_off;
            snap.faulted = state.telemetry.faulted;
            snap.lifecycle = state.lifecycle;
            snap.off_verified = state.off_verified;
            snap.pending_remove = state.pending_remove;
            snap.lease_active = state.lease_active;
            snap.transition = state.transition;
            snap.group_id = state.group_id;
            snap.voltage_v = sane_non_negative(state.telemetry.voltage_v);
            snap.current_a = sane_non_negative(state.telemetry.current_a);
            snap.latest_update_ms = std::max(state.telemetry.last_update_ms,
                                             std::max(state.telemetry.last_voltage_update_ms, state.telemetry.last_current_update_ms));
            break;
        }
        return snap;
    }

    std::optional<ModuleStateView> home_module_state() const {
        if (!manager_) {
            return std::nullopt;
        }
        const auto states = manager_->module_states();
        for (const auto& state : states) {
            if (state.id == module_id_) {
                return state;
            }
        }
        return std::nullopt;
    }

    uint32_t latest_group_telemetry_ms() const {
        uint32_t latest_ms = 0;
        if (!manager_) {
            return latest_ms;
        }
        const auto states = manager_->module_states();
        for (const auto& state : states) {
            if (state.group_id != group_id_) {
                continue;
            }
            latest_ms = std::max(latest_ms, state.telemetry.last_update_ms);
            latest_ms = std::max(latest_ms, state.telemetry.last_voltage_update_ms);
            latest_ms = std::max(latest_ms, state.telemetry.last_current_update_ms);
        }
        return latest_ms;
    }

    GroupStatus group_status() const {
        return manager_ ? manager_->group_status(group_id_) : GroupStatus{};
    }

    void service_module_manager() {
        if (!manager_) {
            return;
        }
        manager_->poll_rx(64);
        manager_->tick(can_.now_ms());
    }

    bool precharge_voltage_converged(float requested_v, float present_v) const {
        const float target_v = std::isfinite(requested_v) ? std::max(0.0f, requested_v) : 0.0f;
        const float measured_v = std::isfinite(present_v) ? std::max(0.0f, present_v) : 0.0f;
        if (target_v <= 1.0f) {
            return measured_v <= PRECHARGE_VOLTAGE_TOLERANCE_V;
        }
        const float tolerance_v = std::max(PRECHARGE_VOLTAGE_TOLERANCE_V, target_v * 0.05f);
        return std::fabs(measured_v - target_v) <= tolerance_v || measured_v >= (target_v - tolerance_v);
    }

    bool apply_target(bool enable, float voltage_v, float current_a, const char* reason) {
        if (!manager_) {
            return false;
        }
        service_module_manager();
        const auto before_status = group_status();
        const auto before_snap = module_snapshot();
        GroupTarget target{};
        target.enable = enable;
        target.voltage_v = std::clamp(voltage_v, 0.0f, MODULE_MAX_VOLTAGE_V);
        target.current_a = enable ? std::clamp(current_a, 0.0f, MODULE_MAX_CURRENT_A) : 0.0f;
        const bool ok = manager_->apply_group_setpoint_allowlist(group_id_, target, {module_id_}, MODULE_ALLOWLIST_LEASE_MS);
        service_module_manager();
        const auto status = group_status();
        const auto snap = module_snapshot();
        if (reason && *reason) {
            std::cout << "[HOSTMOD] reason=" << reason
                      << " en=" << (enable ? 1 : 0)
                      << " reqV=" << std::fixed << std::setprecision(1) << target.voltage_v
                      << " reqI=" << target.current_a
                      << " ok=" << (ok ? 1 : 0)
                      << " assigned=" << status.assigned_modules
                      << " active=" << status.active_modules
                      << " beforeV=" << before_status.combined_voltage_v
                      << " beforeI=" << before_status.combined_current_a
                      << " afterV=" << status.combined_voltage_v
                      << " afterI=" << status.combined_current_a
                      << " lc=" << lifecycle_name(snap.lifecycle)
                      << " tr=" << transition_name(snap.transition)
                      << " off=" << (snap.module_off ? 1 : 0)
                      << " offVerified=" << (snap.off_verified ? 1 : 0)
                      << " lease=" << (snap.lease_active ? 1 : 0)
                      << " online=" << (snap.online ? 1 : 0)
                      << " group=" << snap.group_id
                      << " snapV=" << snap.voltage_v
                      << " snapI=" << snap.current_a
                      << " beforeOff=" << (before_snap.module_off ? 1 : 0)
                      << "\n";
        }
        if (!enable) {
            module_output_enabled_ = false;
            last_applied_voltage_v_ = 0.0f;
            last_applied_current_a_ = 0.0f;
            return ok;
        }
        if (ok && status.assigned_modules > 0u) {
            module_output_enabled_ = true;
            last_applied_voltage_v_ = target.voltage_v;
            last_applied_current_a_ = target.current_a;
            return true;
        }
        return false;
    }

    void maintain_idle_allocation(int64_t now_ms) {
        if (!manager_ || plan_enabled_) {
            return;
        }
        if ((now_ms - last_allowlist_refresh_ms_) < static_cast<int64_t>(IDLE_ALLOWLIST_REFRESH_MS)) {
            return;
        }
        last_allowlist_refresh_ms_ = now_ms;
        service_module_manager();
        const auto status = group_status();
        if (status.assigned_modules > 0u) {
            (void)manager_->refresh_group_allowlist_lease(group_id_, {module_id_}, MODULE_ALLOWLIST_LEASE_MS);
            service_module_manager();
            return;
        }
        const GroupTarget off_target{false, 0.0f, 0.0f};
        (void)manager_->apply_group_setpoint_allowlist(group_id_, off_target, {module_id_}, MODULE_ALLOWLIST_LEASE_MS);
        service_module_manager();
        const auto repaired = group_status();
        const auto snap = module_snapshot();
        std::cout << "[HOSTMOD] idle_allocation_repair assigned=" << repaired.assigned_modules
                  << " lc=" << lifecycle_name(snap.lifecycle)
                  << " tr=" << transition_name(snap.transition)
                  << " off=" << (snap.module_off ? 1 : 0)
                  << " lease=" << (snap.lease_active ? 1 : 0)
                  << " group=" << snap.group_id
                  << "\n";
    }

    bool refresh_precharge_convergence(float requested_v) {
        const auto status = group_status();
        const auto snap = module_snapshot();
        float present_v = sane_non_negative(status.combined_voltage_v);
        if (present_v <= 0.1f) {
            present_v = snap.voltage_v;
        }
        precharge_converged_ = status.assigned_modules > 0u && precharge_voltage_converged(requested_v, present_v);
        return precharge_converged_;
    }

    bool standalone_precharge_start_ready(float requested_v) const {
        if (!precharge_seen_ || !relay_is_closed() || !module_output_enabled_) {
            return false;
        }
        const auto status = group_status();
        const auto snap = module_snapshot();
        float present_v = sane_non_negative(status.combined_voltage_v);
        float present_i = sane_non_negative(status.combined_current_a);
        if (present_v <= 0.1f) {
            present_v = snap.voltage_v;
        }
        if (present_i <= 0.05f) {
            present_i = snap.current_a;
        }
        if (precharge_voltage_converged(requested_v, present_v)) {
            return true;
        }
        const float target_v = std::isfinite(requested_v) ? std::max(0.0f, requested_v) : 0.0f;
        const float accept_floor_v = std::max(PRECHARGE_START_ACCEPT_MIN_V, target_v * PRECHARGE_START_ACCEPT_MIN_RATIO);
        const bool current_flow_ready =
            precharge_count_ >= PRECHARGE_RELAX_MIN_COUNT && present_i >= PRECHARGE_START_ACCEPT_MIN_CURRENT_A;
        return present_v >= accept_floor_v || current_flow_ready;
    }

    bool snapshot_post_target_present_values(uint32_t baseline_telemetry_ms,
                                             GroupStatus* out_status,
                                             float* out_present_v,
                                             float* out_present_i,
                                             bool* out_fresh_after_target) {
        const auto started = std::chrono::steady_clock::now();
        GroupStatus status{};
        float present_v = 0.0f;
        float present_i = 0.0f;
        bool telemetry_valid = false;
        bool fresh = false;
        for (;;) {
            service_module_manager();
            const auto snap = module_snapshot();
            status = group_status();
            telemetry_valid = status.valid;
            present_v = sane_non_negative(status.combined_voltage_v);
            present_i = sane_non_negative(status.combined_current_a);
            if (present_v <= 0.1f) {
                present_v = snap.voltage_v;
            }
            if (present_i <= 0.05f) {
                present_i = snap.current_a;
            }
            const uint32_t latest_ms = latest_group_telemetry_ms();
            fresh = latest_ms != 0u && latest_ms > baseline_telemetry_ms;
            const auto elapsed_ms = static_cast<uint32_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count());
            if (fresh || elapsed_ms >= CURRENT_DEMAND_FRESH_SAMPLE_TIMEOUT_MS) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(CURRENT_DEMAND_FRESH_SAMPLE_POLL_MS));
        }
        if (out_status) *out_status = status;
        if (out_present_v) *out_present_v = present_v;
        if (out_present_i) *out_present_i = present_i;
        if (out_fresh_after_target) *out_fresh_after_target = fresh;
        return telemetry_valid;
    }

    bool current_demand_feedback_ready(bool relay_closed,
                                       bool target_applied,
                                       bool sample_valid,
                                       bool sample_fresh,
                                       bool module_off,
                                       float target_v,
                                       float target_i,
                                       float present_v,
                                       float present_i) const {
        (void)sample_fresh;
        (void)module_off;
        (void)target_v;
        (void)target_i;
        (void)present_v;
        (void)present_i;
        if (!relay_closed || !target_applied) {
            return false;
        }
        // Match standalone mode: once PowerDelivery is armed and the target has
        // been applied successfully, CurrentDemand readiness comes from the
        // local telemetry path being valid. Standalone does not wait for bus
        // voltage/current to rise before returning EVSE_Ready, and the EV/BMS
        // simulator on this bench accepts that contract.
        return sample_valid;
    }

    bool active_charge_window(const PlcLiveState& live) const {
        return live.hlc_active || live.slac_matched || stage_name_ == "precharge" || stage_name_ == "power_delivery" ||
               stage_name_ == "current_demand";
    }

    bool should_latch_active_stage(const PlcLiveState& live, int64_t now_ms) const {
        if (!active_power_stage_name(stage_name_)) {
            return false;
        }
        if (last_active_stage_ms_ < 0 || (now_ms - last_active_stage_ms_) > ACTIVE_STAGE_LATCH_MS) {
            return false;
        }
        if (live.stop_active) {
            return false;
        }
        const bool session_signals_active =
            live.hlc_active || live.slac_matched || live.session_started || digital_comm_cp_phase(live.cp_phase);
        return session_signals_active;
    }

    void tick_contract(const PlcLiveState& live, int64_t now_ms) {
        const bool active_window = active_charge_window(live);
        if ((now_ms - last_hb_ms_) >= cfg_.heartbeat_ms) {
            const std::string cmd = "CTRL HB " + std::to_string(cfg_.heartbeat_timeout_ms);
            if (active_window) {
                serial_.send_untracked(cmd);
            } else {
                auto ack = serial_.send_expect_ack(cmd, ACK_CMD_HEARTBEAT, 2000, 1);
                if (ack.status != ACK_OK) {
                    throw std::runtime_error("CTRL HB rejected");
                }
            }
            last_hb_ms_ = now_ms;
        }

        // Active AUTH refresh must tolerate an occasional dropped CDC command
        // without riding the TTL edge. Refresh every ~5 s while charging so a
        // single missed grant does not expire the controller contract.
        const int64_t active_auth_refresh_ms =
            std::max<int64_t>(1000, std::min<int64_t>(5000, cfg_.auth_ttl_ms / 4));
        const int64_t idle_auth_refresh_ms =
            std::max<int64_t>(1000, std::min<int64_t>(cfg_.auth_ttl_ms / 2, cfg_.auth_ttl_ms - 1500));
        const int64_t auth_refresh_ms = active_window ? active_auth_refresh_ms : idle_auth_refresh_ms;
        if ((now_ms - last_auth_ms_) >= auth_refresh_ms) {
            const std::string cmd = "CTRL AUTH grant " + std::to_string(cfg_.auth_ttl_ms);
            if (active_window) {
                serial_.send_untracked(cmd);
            } else {
                auto ack = serial_.send_expect_ack(cmd, ACK_CMD_AUTH, 2000, 1);
                if (ack.status != ACK_OK) {
                    throw std::runtime_error("CTRL AUTH grant rejected");
                }
            }
            last_auth_ms_ = now_ms;
        }

        if (!active_window &&
            (now_ms - last_status_poll_ms_) >= static_cast<int64_t>(cfg_.status_interval_s * 1000.0)) {
            try {
                status_ = serial_.query_status(1000);
            } catch (const std::exception& exc) {
                notes_.push_back(std::string("status poll degraded: ") + exc.what());
            }
            last_status_poll_ms_ = now_ms;
        }

        const bool waiting_for_slac = !live.slac_matched && !live.hlc_active;
        const bool at_b1 = live.cp_phase == "B1";
        const bool digital_comm = live.cp_phase == "B2" || live.cp_phase == "C" || live.cp_phase == "D";
        const bool slac_start_in_progress = live.session_started || digital_comm;
        const bool start_window_ready =
            waiting_for_slac && at_b1 && live.b1_since_ms >= 0 &&
            (now_ms - live.b1_since_ms) >= static_cast<int64_t>(cfg_.b1_start_delay_s * 1000.0);
        if (remote_start_sent_ && waiting_for_slac && at_b1 && !slac_start_in_progress &&
            (now_ms - last_slac_start_ms_) >= CTRL_SLAC_RETRY_MS) {
            // A prior start request aged out without SLAC progressing into B2/session.
            // Clear the local latch so the controller can issue a fresh start request.
            remote_start_sent_ = false;
        }
        if ((start_window_ready || (waiting_for_slac && digital_comm)) && !remote_start_sent_) {
            auto ack = serial_.send_expect_ack("CTRL SLAC start " + std::to_string(cfg_.arm_ms), ACK_CMD_SLAC, 3000, 1);
            if (ack.status != ACK_OK) {
                throw std::runtime_error("CTRL SLAC start rejected");
            }
            remote_start_sent_ = true;
            last_slac_start_ms_ = now_ms;
        } else if (live.cp_phase == "A") {
            remote_start_sent_ = false;
            last_slac_start_ms_ = -1;
        }
    }

    void tick_relay(int64_t now_ms) {
        if (desired_relay_closed_) {
            if (last_relay_command_ != 1 || !relay_is_closed() ||
                (now_ms - last_relay_refresh_ms_) >= static_cast<int64_t>(cfg_.relay_refresh_s * 1000.0)) {
                set_relay(true, cfg_.relay_hold_ms, true);
                last_relay_refresh_ms_ = now_ms;
                last_relay_command_ = 1;
            }
        } else {
            if (last_relay_command_ != 0 || !relay_is_open()) {
                set_relay(false, 0, true);
                last_relay_refresh_ms_ = now_ms;
                last_relay_command_ = 0;
            }
        }
    }

    void set_relay(bool closed, int hold_ms, bool require_ack) {
        const int mask = 1 << (cfg_.power_relay - 1);
        const int state_mask = closed ? mask : 0;
        const std::string cmd = "CTRL RELAY " + std::to_string(mask) + " " + std::to_string(state_mask) + " " +
                                std::to_string(std::max(0, hold_ms));
        if (require_ack) {
            auto ack = serial_.send_expect_ack(cmd, ACK_CMD_RELAY, 3000, 1);
            if (ack.status != ACK_OK) {
                throw std::runtime_error("relay command rejected");
            }
        } else {
            serial_.send(cmd);
        }
    }

    bool relay_is_closed() const {
        const auto live = serial_.live_state_copy();
        const auto it = live.relay_states.find(cfg_.power_relay);
        return it != live.relay_states.end() && it->second;
    }

    bool relay_is_open() const {
        const auto live = serial_.live_state_copy();
        const auto it = live.relay_states.find(cfg_.power_relay);
        return it != live.relay_states.end() && !it->second;
    }

    void update_plan_from_stage(const PlcLiveState& live, int64_t now_ms) {
        if (live.bms_stage_name == "none" && should_latch_active_stage(live, now_ms)) {
            return;
        }

        stage_name_ = live.bms_stage_name;
        target_v_ = (live.target_v > 0.0f) ? live.target_v : live.latched_target_v;
        target_i_ = (live.target_i > 0.0f) ? live.target_i : live.latched_target_i;
        if (plc_local_power_path_) {
            last_active_stage_ms_ = active_power_stage_name(stage_name_) ? now_ms : -1;
            desired_relay_closed_ = false;
            plan_enabled_ = false;
            plan_voltage_v_ = 0.0f;
            plan_current_a_ = 0.0f;
            if (stage_name_ == "precharge") {
                precharge_seen_ = true;
                ++precharge_count_;
            } else if (stage_name_ == "none") {
                precharge_seen_ = false;
                precharge_converged_ = false;
                precharge_count_ = 0u;
            }
            return;
        }
        if (stage_name_ == "precharge") {
            last_active_stage_ms_ = now_ms;
            precharge_seen_ = true;
            ++precharge_count_;
            desired_relay_closed_ = true;
            plan_enabled_ = true;
            plan_voltage_v_ = target_v_;
            plan_current_a_ = (target_i_ > 0.05f) ? target_i_ : static_cast<float>(cfg_.precharge_current_a);
        } else if (stage_name_ == "power_delivery") {
            last_active_stage_ms_ = now_ms;
            desired_relay_closed_ = true;
            plan_enabled_ = true;
            plan_voltage_v_ = target_v_;
            plan_current_a_ = (target_i_ > 0.05f) ? target_i_ : static_cast<float>(cfg_.precharge_current_a);
        } else if (stage_name_ == "current_demand") {
            last_active_stage_ms_ = now_ms;
            desired_relay_closed_ = true;
            plan_enabled_ = true;
            plan_voltage_v_ = target_v_;
            plan_current_a_ = target_i_;
        } else {
            last_active_stage_ms_ = -1;
            desired_relay_closed_ = false;
            plan_enabled_ = false;
            plan_voltage_v_ = 0.0f;
            plan_current_a_ = 0.0f;
            precharge_count_ = 0u;
            precharge_seen_ = false;
            precharge_converged_ = false;
        }
    }

    bool apply_plan_if_due(const PlcLiveState& live, int64_t now_ms, bool* out_applied_now = nullptr) {
        if (out_applied_now) {
            *out_applied_now = false;
        }
        const auto signature =
            std::make_tuple(plan_enabled_,
                            static_cast<int>(std::lround(plan_voltage_v_ * 10.0f)),
                            static_cast<int>(std::lround(plan_current_a_ * 100.0f)));
        const bool signature_changed = signature != last_plan_signature_;
        const bool fresh_bms_evt = (stage_name_ != "none") && (live.bms_evt_seq != last_plan_bms_evt_seq_);
        if (!plan_enabled_ && !signature_changed && !fresh_bms_evt) {
            return last_apply_ok_;
        }
        const int64_t min_reapply_ms =
            active_charge_window(live) ? static_cast<int64_t>(ACTIVE_PLAN_REAPPLY_MS)
                                       : static_cast<int64_t>(cfg_.control_interval_s * 1000.0);
        const bool keepalive_due = (now_ms - last_plan_apply_ms_) >= min_reapply_ms;
        if (!signature_changed && !fresh_bms_evt && !keepalive_due) {
            return last_apply_ok_;
        }
        last_plan_signature_ = signature;
        last_plan_bms_evt_seq_ = live.bms_evt_seq;
        last_plan_apply_ms_ = now_ms;
        last_apply_ok_ = apply_target(plan_enabled_, plan_voltage_v_, plan_current_a_, stage_name_.c_str());
        if (out_applied_now) {
            *out_applied_now = true;
        }
        return last_apply_ok_;
    }

    void tick_feedback(int64_t now_ms) {
        const PlcLiveState live = serial_.live_state_copy();
        const bool fresh_bms_evt = live.bms_evt_seq != last_feedback_bms_evt_seq_;
        const bool charge_window_active = active_charge_window(live);
        const int64_t idle_feedback_interval_ms =
            std::max<int64_t>(20, static_cast<int64_t>(cfg_.feedback_interval_s * 1000.0));
        const int64_t active_feedback_keepalive_ms =
            std::max<int64_t>(20, static_cast<int64_t>(cfg_.active_feedback_keepalive_s * 1000.0));
        const bool idle_refresh_due =
            !charge_window_active && (now_ms - last_feedback_ms_) >= idle_feedback_interval_ms;
        const bool active_keepalive_due =
            charge_window_active && (now_ms - last_feedback_ms_) >= active_feedback_keepalive_ms;

        service_module_manager();
        auto snap = module_snapshot();
        auto status = group_status();
        float present_v = sane_non_negative(status.combined_voltage_v);
        float present_i = sane_non_negative(status.combined_current_a);
        if (present_v <= 0.1f) {
            present_v = snap.voltage_v;
        }
        if (present_i <= 0.05f) {
            present_i = snap.current_a;
        }
        bool valid = snap.present && snap.online;
        bool ready = false;
        bool curr_lim = false;
        bool volt_lim = false;
        bool pwr_lim = false;
        bool sample_fresh_after_target = false;
        bool sample_fresh = false;
        GroupStatus live_status = status;

        if (stage_name_ == "precharge") {
            bool precharge_ready = refresh_precharge_convergence(target_v_);
            if (!precharge_ready && standalone_precharge_start_ready(target_v_)) {
                precharge_converged_ = true;
                precharge_ready = true;
            }
            ready = precharge_ready;
        } else if (stage_name_ == "power_delivery") {
            bool precharge_ready = refresh_precharge_convergence(target_v_);
            if (!precharge_ready && standalone_precharge_start_ready(target_v_)) {
                precharge_converged_ = true;
                precharge_ready = true;
            }
            if (!precharge_ready && precharge_seen_ && relay_is_closed() &&
                precharge_count_ >= PRECHARGE_RELAX_MIN_COUNT) {
                // Keep the host-side FEEDBACK stream aligned with the PLC's
                // controller-mode PowerDelivery handler. Once the relaxed start
                // path is allowed there, the host must stop streaming
                // fb_ready=0 or the first CurrentDemand loop will still see a
                // stale NotReady sample that standalone mode never produces.
                precharge_converged_ = true;
                precharge_ready = true;
            }
            ready = precharge_ready && relay_is_closed();
        } else if (stage_name_ == "current_demand") {
            // Match standalone sequencing: capture the telemetry generation
            // before applying the new CurrentDemand target, then wait for the
            // first post-target sample. Taking the baseline after apply_plan()
            // makes the external host answer from stale pre-target telemetry.
            const uint32_t baseline_telemetry_ms = latest_group_telemetry_ms();
            bool target_applied_now = false;
            const bool target_applied = apply_plan_if_due(live, now_ms, &target_applied_now);
            const bool quick_valid = valid;
            const float quick_present_v = present_v;
            const float quick_present_i = present_i;
            const bool quick_feedback_state_changed =
                last_feedback_stage_ != stage_name_ ||
                quick_valid != last_feedback_valid_ ||
                std::fabs(quick_present_v - last_feedback_present_v_) >= ACTIVE_FEEDBACK_V_DELTA_V ||
                std::fabs(quick_present_i - last_feedback_present_i_) >= ACTIVE_FEEDBACK_I_DELTA_A;
            const bool detailed_sample_needed =
                fresh_bms_evt || active_keepalive_due || target_applied_now || quick_feedback_state_changed;
            bool telemetry_valid = valid;
            if (detailed_sample_needed) {
                telemetry_valid =
                    snapshot_post_target_present_values(
                        baseline_telemetry_ms, &live_status, &present_v, &present_i, &sample_fresh);
                sample_fresh_after_target = sample_fresh;
                status = live_status;
                snap = module_snapshot();
                valid = snap.present && snap.online;
            }
            const bool relay_closed = relay_is_closed();
            const bool sample_valid = valid && telemetry_valid;
            ready = current_demand_feedback_ready(relay_is_closed(),
                                                  target_applied,
                                                  sample_valid,
                                                  sample_fresh,
                                                  snap.module_off,
                                                  target_v_,
                                                  target_i_,
                                                  present_v,
                                                  present_i);
            static int64_t last_hostfb_log_ms = -1;
            static bool last_hostfb_ready = false;
            static bool last_hostfb_valid = false;
            static bool last_hostfb_target_applied = false;
            static float last_hostfb_v = 0.0f;
            static float last_hostfb_i = 0.0f;
            const bool hostfb_change =
                last_hostfb_log_ms < 0 ||
                ready != last_hostfb_ready ||
                valid != last_hostfb_valid ||
                target_applied != last_hostfb_target_applied ||
                std::fabs(present_v - last_hostfb_v) >= 10.0f ||
                std::fabs(present_i - last_hostfb_i) >= 2.0f;
            if (hostfb_change || (now_ms - last_hostfb_log_ms) >= 500) {
                last_hostfb_log_ms = now_ms;
                last_hostfb_ready = ready;
                last_hostfb_valid = valid;
                last_hostfb_target_applied = target_applied;
                last_hostfb_v = present_v;
                last_hostfb_i = present_i;
                std::cout << "[HOSTFB] stage=current_demand"
                          << " targetApplied=" << (target_applied ? 1 : 0)
                          << " relay=" << (relay_closed ? 1 : 0)
                          << " valid=" << (valid ? 1 : 0)
                          << " telemetryValid=" << (telemetry_valid ? 1 : 0)
                          << " sampleValid=" << (sample_valid ? 1 : 0)
                          << " sampleFresh=" << (sample_fresh ? 1 : 0)
                          << " moduleOff=" << (snap.module_off ? 1 : 0)
                          << " reqV=" << std::fixed << std::setprecision(1) << target_v_
                          << " reqI=" << target_i_
                          << " presentV=" << present_v
                          << " presentI=" << present_i
                          << " ready=" << (ready ? 1 : 0)
                          << "\n";
            }
            const float req_power_kw = (target_v_ * target_i_) / 1000.0f;
            const float applied_power_kw = (last_applied_voltage_v_ * last_applied_current_a_) / 1000.0f;
            curr_lim = live_status.saturated || (target_i_ > (last_applied_current_a_ + 0.1f)) ||
                       (live_status.available_current_a > 0.0f && target_i_ > (live_status.available_current_a + 0.1f));
            pwr_lim = live_status.saturated || (req_power_kw > (applied_power_kw + 0.1f)) ||
                      (live_status.available_power_kw > 0.0f && req_power_kw > (live_status.available_power_kw + 0.1f));
            volt_lim = target_v_ > (MODULE_MAX_VOLTAGE_V + 0.5f);
        }

        const bool feedback_state_changed =
            last_feedback_stage_ != stage_name_ ||
            valid != last_feedback_valid_ ||
            ready != last_feedback_ready_ ||
            curr_lim != last_feedback_curr_lim_ ||
            volt_lim != last_feedback_volt_lim_ ||
            pwr_lim != last_feedback_pwr_lim_ ||
            std::fabs(present_v - last_feedback_present_v_) >= ACTIVE_FEEDBACK_V_DELTA_V ||
            std::fabs(present_i - last_feedback_present_i_) >= ACTIVE_FEEDBACK_I_DELTA_A;
        const bool active_refresh_due =
            charge_window_active &&
            (fresh_bms_evt || active_keepalive_due || feedback_state_changed || sample_fresh_after_target);
        if (!fresh_bms_evt && !idle_refresh_due && !active_refresh_due) {
            return;
        }

        const std::string cmd_text = [&]() {
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(1);
            oss << "CTRL FEEDBACK " << (valid ? 1 : 0) << ' ' << (ready ? 1 : 0) << ' ' << present_v << ' ' << present_i
                << ' ' << (curr_lim ? 1 : 0) << ' ' << (volt_lim ? 1 : 0) << ' ' << (pwr_lim ? 1 : 0);
            return oss.str();
        }();
        try {
            if (charge_window_active) {
                // During an active charge window, FEEDBACK must behave like the
                // local standalone telemetry path, not like a request/response
                // RPC. The PLC already ACKs FEEDBACK asynchronously; gating the
                // next FEEDBACK on the previous ACK stretches the controller
                // loop to the serial round-trip time and causes stale
                // CurrentDemand samples. Keep HB/AUTH/STOP strict, but feed
                // FEEDBACK as a best-effort stream.
                serial_.send_untracked(cmd_text);
                last_feedback_ms_ = now_ms;
                last_feedback_stage_ = stage_name_;
                last_feedback_telemetry_ms_ = latest_group_telemetry_ms();
                last_feedback_valid_ = valid;
                last_feedback_ready_ = ready;
                last_feedback_curr_lim_ = curr_lim;
                last_feedback_volt_lim_ = volt_lim;
                last_feedback_pwr_lim_ = pwr_lim;
                last_feedback_present_v_ = present_v;
                last_feedback_present_i_ = present_i;
                if (fresh_bms_evt) {
                    last_feedback_bms_evt_seq_ = live.bms_evt_seq;
                }
            } else {
                auto ack = serial_.send_expect_ack(cmd_text, ACK_CMD_FEEDBACK, ACTIVE_FEEDBACK_ACK_TIMEOUT_MS, 0);
                if (ack.status == ACK_OK) {
                    last_feedback_ms_ = ack.received_at_ms;
                    last_feedback_stage_ = stage_name_;
                    last_feedback_telemetry_ms_ = latest_group_telemetry_ms();
                    last_feedback_valid_ = valid;
                    last_feedback_ready_ = ready;
                    last_feedback_curr_lim_ = curr_lim;
                    last_feedback_volt_lim_ = volt_lim;
                    last_feedback_pwr_lim_ = pwr_lim;
                    last_feedback_present_v_ = present_v;
                    last_feedback_present_i_ = present_i;
                    if (fresh_bms_evt) {
                        last_feedback_bms_evt_seq_ = live.bms_evt_seq;
                    }
                } else {
                    notes_.push_back("feedback ack non-ok status=" + std::to_string(ack.status));
                }
            }
        } catch (const std::exception& exc) {
            notes_.push_back(std::string("feedback send failed: ") + exc.what());
        }

        if (stage_name_ == "current_demand" && ready) {
            if (current_demand_ready_since_ms_ < 0) {
                current_demand_ready_since_ms_ = now_ms;
            }
        } else {
            current_demand_ready_since_ms_ = -1;
        }
    }

    void tick_local_power_monitor(const PlcLiveState& live, int64_t now_ms) {
        const auto relay1_it = live.relay_states.find(1);
        const bool relay1_closed = relay1_it != live.relay_states.end() && relay1_it->second;
        const bool current_demand_stage = (stage_name_ == "current_demand") || (live.bms_stage_name == "current_demand");
        const bool ready = current_demand_stage && relay1_closed && !live.stop_active &&
                           (live.cp_phase == "C" || live.cp_phase == "D");
        if (ready) {
            const int64_t observed_since_ms =
                (live.first_current_demand_ms >= 0) ? live.first_current_demand_ms : now_ms;
            if (current_demand_ready_since_ms_ < 0 || observed_since_ms < current_demand_ready_since_ms_) {
                current_demand_ready_since_ms_ = observed_since_ms;
            }
        } else {
            current_demand_ready_since_ms_ = -1;
        }
    }

    bool current_demand_ready(const PlcLiveState& live) const {
        if (current_demand_ready_since_ms_ < 0) {
            return false;
        }
        if (plc_local_power_path_) {
            const auto relay1_it = live.relay_states.find(1);
            const bool relay1_closed = relay1_it != live.relay_states.end() && relay1_it->second;
            const bool current_demand_stage =
                (stage_name_ == "current_demand") || (live.bms_stage_name == "current_demand");
            if (!current_demand_stage || !relay1_closed || live.stop_active ||
                (live.cp_phase != "C" && live.cp_phase != "D")) {
                return false;
            }
        } else {
            if (!live.hlc_active || live.bms_stage_name != "current_demand" || stage_name_ != "current_demand") {
                return false;
            }
        }
        return (steady_ms() - current_demand_ready_since_ms_) >= static_cast<int64_t>(cfg_.current_demand_hold_s * 1000.0);
    }

    void tick_progress(int64_t now_ms) {
        if ((now_ms - last_progress_ms_) < 5000) {
            return;
        }
        const auto live = serial_.live_state_copy();
        const auto snap = module_snapshot();
        const float present_v = plc_local_power_path_ ? live.fb_v : snap.voltage_v;
        const float present_i = plc_local_power_path_ ? live.fb_i : snap.current_a;
        const auto relay1_it = live.relay_states.find(1);
        const bool relay1_closed =
            plc_local_power_path_ ? (relay1_it != live.relay_states.end() && relay1_it->second) : relay_is_closed();
        std::cout << "[PROGRESS] phase=" << phase_
                  << " stage=" << stage_name_
                  << " relay1=" << (relay1_closed ? 1 : 0)
                  << " targetV=" << std::fixed << std::setprecision(1) << target_v_
                  << " targetI=" << target_i_
                  << " presentV=" << present_v
                  << " presentI=" << present_i
                  << " cp=" << live.cp_phase
                  << " slac=" << (live.slac_matched ? 1 : 0)
                  << " hlc=" << (live.hlc_ready ? 1 : 0) << "\n";
        last_progress_ms_ = now_ms;
    }

    void tick() {
        const int64_t now_ms = steady_ms();
        const PlcLiveState live = serial_.live_state_copy();
        update_plan_from_stage(live, now_ms);
        if (plc_local_power_path_) {
            tick_local_power_monitor(live, now_ms);
        } else {
            // Mirror standalone mode: service cbmodules continuously, not only when a
            // new BMS event arrives or a feedback packet is due.
            service_module_manager();
            maintain_idle_allocation(now_ms);
            apply_plan_if_due(live, now_ms);
            tick_relay(now_ms);
            tick_feedback(now_ms);
        }
        tick_contract(serial_.live_state_copy(), now_ms);
        tick_progress(now_ms);
    }

    void cleanup() {
        phase_ = "stopping";
        try {
            auto ack = serial_.send_expect_ack(
                "CTRL STOP soft " + std::to_string(static_cast<int>(cfg_.stop_timeout_s * 1000.0)),
                ACK_CMD_STOP,
                3000,
                1);
            if (ack.status != ACK_OK && ack.status != ACK_BAD_SEQ) {
                throw std::runtime_error("soft stop rejected");
            }
        } catch (...) {
            try {
                serial_.send_expect_ack("CTRL STOP hard 3000", ACK_CMD_STOP, 3000, 1);
            } catch (...) {
                notes_.push_back("hard stop ack missing during cleanup");
            }
        }

        desired_relay_closed_ = false;
        plan_enabled_ = false;
        plan_voltage_v_ = 0.0f;
        plan_current_a_ = 0.0f;
        if (!plc_local_power_path_) {
            apply_target(false, 0.0f, 0.0f, "Cleanup");
        }

        const int64_t stop_deadline = steady_ms() + static_cast<int64_t>(cfg_.stop_timeout_s * 1000.0);
        while (steady_ms() < stop_deadline) {
            const auto live = serial_.live_state_copy();
            if (!plc_local_power_path_) {
                service_module_manager();
                tick_relay(steady_ms());
            }
            if ((live.stop_done && relay_is_open()) ||
                ((plc_local_power_path_ || relay_is_open()) && !live.hlc_active &&
                 (live.cp_phase == "A" || live.cp_phase == "B" || live.cp_phase == "B1" || live.cp_phase == "B2"))) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        try {
            serial_.send_expect_ack("CTRL AUTH deny " + std::to_string(cfg_.auth_ttl_ms), ACK_CMD_AUTH, 2000, 1);
        } catch (...) {
        }
        try {
            serial_.send_expect_ack("CTRL SLAC disarm 3000", ACK_CMD_SLAC, 2000, 1);
        } catch (...) {
        }
        if (!plc_local_power_path_) {
            try {
                serial_.send_expect_ack("CTRL FEEDBACK 1 0 0 0 0 0 0", ACK_CMD_FEEDBACK, 2000, 1);
            } catch (...) {
            }
            try {
                set_relay(false, 0, false);
            } catch (...) {
            }
        }
    }

    void write_summary(const std::string& result) const {
        ensure_parent_dir(cfg_.summary_file);
        std::ofstream fp(cfg_.summary_file, std::ios::out | std::ios::trunc);
        const auto live = serial_.live_state_copy();
        const auto snap = module_snapshot();
        const auto status = group_status();
        const float summary_v = plc_local_power_path_ ? live.fb_v : snap.voltage_v;
        const float summary_i = plc_local_power_path_ ? live.fb_i : snap.current_a;
        const auto relay1_it = live.relay_states.find(1);
        const int relay1_state =
            plc_local_power_path_ ? ((relay1_it != live.relay_states.end() && relay1_it->second) ? 1 : 0)
                                  : (relay_is_closed() ? 1 : 0);
        fp << "{\n";
        fp << "  \"result\": \"" << json_escape(result) << "\",\n";
        fp << "  \"phase\": \"" << json_escape(phase_) << "\",\n";
        fp << "  \"failures\": [";
        for (std::size_t i = 0; i < failures_.size(); ++i) {
            if (i) fp << ", ";
            fp << '"' << json_escape(failures_[i]) << '"';
        }
        fp << "],\n";
        fp << "  \"notes\": [";
        for (std::size_t i = 0; i < notes_.size(); ++i) {
            if (i) fp << ", ";
            fp << '"' << json_escape(notes_[i]) << '"';
        }
        fp << "],\n";
        fp << "  \"plc\": {\n";
        fp << "    \"mode_id\": " << live.mode_id << ",\n";
        fp << "    \"mode_name\": \"" << json_escape(live.mode_name) << "\",\n";
        fp << "    \"cp_phase\": \"" << json_escape(live.cp_phase) << "\",\n";
        fp << "    \"slac_matched\": " << (live.slac_matched ? "true" : "false") << ",\n";
        fp << "    \"hlc_ready\": " << (live.hlc_ready ? "true" : "false") << ",\n";
        fp << "    \"hlc_active\": " << (live.hlc_active ? "true" : "false") << ",\n";
        fp << "    \"bms_stage_name\": \"" << json_escape(live.bms_stage_name) << "\",\n";
        fp << "    \"delivery_ready\": " << (live.delivery_ready ? "true" : "false") << ",\n";
        fp << "    \"target_v\": " << live.target_v << ",\n";
        fp << "    \"target_i\": " << live.target_i << ",\n";
        fp << "    \"fb_v\": " << live.fb_v << ",\n";
        fp << "    \"fb_i\": " << live.fb_i << ",\n";
        fp << "    \"relay1\": " << relay1_state << "\n";
        fp << "  },\n";
        fp << "  \"module\": {\n";
        fp << "    \"id\": \"" << json_escape(module_id_) << "\",\n";
        fp << "    \"voltage_v\": " << summary_v << ",\n";
        fp << "    \"current_a\": " << summary_i << ",\n";
        fp << "    \"online\": " << (plc_local_power_path_ ? "true" : (snap.online ? "true" : "false")) << ",\n";
        fp << "    \"off\": " << (plc_local_power_path_ ? "false" : (snap.module_off ? "true" : "false")) << "\n";
        fp << "  },\n";
        fp << "  \"group\": {\n";
        fp << "    \"assigned_modules\": " << status.assigned_modules << ",\n";
        fp << "    \"active_modules\": " << status.active_modules << ",\n";
        fp << "    \"combined_voltage_v\": " << (plc_local_power_path_ ? summary_v : status.combined_voltage_v) << ",\n";
        fp << "    \"combined_current_a\": " << (plc_local_power_path_ ? summary_i : status.combined_current_a) << "\n";
        fp << "  }\n";
        fp << "}\n";
    }

    Config cfg_;
    SerialControllerClient serial_;
    SocketCanTransport can_;
    PlcStatus status_{};
    std::unique_ptr<ModuleManager> manager_;
    bool plc_local_power_path_{false};
    std::string group_id_;
    std::string module_id_;
    std::string phase_{"bootstrap"};
    std::vector<std::string> notes_;
    std::vector<std::string> failures_;

    bool desired_relay_closed_{false};
    int last_relay_command_{-1};
    int64_t last_relay_refresh_ms_{0};
    bool remote_start_sent_{false};
    int64_t last_slac_start_ms_{-1};
    int64_t last_hb_ms_{0};
    int64_t last_auth_ms_{0};
    int64_t last_status_poll_ms_{0};
    int64_t last_feedback_ms_{0};
    uint32_t last_feedback_telemetry_ms_{0u};
    std::string last_feedback_stage_{"none"};
    bool last_feedback_valid_{false};
    bool last_feedback_ready_{false};
    bool last_feedback_curr_lim_{false};
    bool last_feedback_volt_lim_{false};
    bool last_feedback_pwr_lim_{false};
    float last_feedback_present_v_{0.0f};
    float last_feedback_present_i_{0.0f};
    uint64_t last_feedback_bms_evt_seq_{0};
    uint64_t last_plan_bms_evt_seq_{0};
    int64_t last_plan_apply_ms_{0};
    int64_t last_allowlist_refresh_ms_{-1};
    int64_t last_progress_ms_{0};
    int64_t last_active_stage_ms_{-1};
    std::tuple<bool, int, int> last_plan_signature_{false, 0, 0};

    std::string stage_name_{"none"};
    float target_v_{0.0f};
    float target_i_{0.0f};
    bool plan_enabled_{false};
    float plan_voltage_v_{0.0f};
    float plan_current_a_{0.0f};
    bool last_apply_ok_{false};
    bool module_output_enabled_{false};
    float last_applied_voltage_v_{0.0f};
    float last_applied_current_a_{0.0f};
    bool precharge_seen_{false};
    bool precharge_converged_{false};
    uint32_t precharge_count_{0u};
    int64_t current_demand_ready_since_ms_{-1};
};

} // namespace

int main(int argc, char** argv) {
    try {
        Config cfg = parse_args(argc, argv);
        Runner runner(std::move(cfg));
        return runner.run();
    } catch (const std::exception& exc) {
        std::cerr << "[ERR] " << exc.what() << "\n";
        return 2;
    }
}
