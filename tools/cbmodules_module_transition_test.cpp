// SPDX-License-Identifier: Apache-2.0
#include "cbmodules/module_manager.hpp"

#include <fcntl.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using cbmodules::CanFrame;
using cbmodules::CanTransport;
using cbmodules::GroupConfig;
using cbmodules::GroupStatus;
using cbmodules::GroupTarget;
using cbmodules::ModuleLeaseRequest;
using cbmodules::ModuleManager;
using cbmodules::ModuleManagerConfig;
using cbmodules::ModuleSpec;
using cbmodules::ModuleStateView;
using cbmodules::ModuleTransitionState;
using cbmodules::ModuleType;

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

void ensure_parent_dir(const std::string& path) {
    const auto pos = path.find_last_of('/');
    if (pos == std::string::npos) return;
    const std::string parent = path.substr(0, pos);
    if (parent.empty()) return;
    std::string cmd = "mkdir -p \"" + parent + "\"";
    const int rc = std::system(cmd.c_str());
    if (rc != 0) {
        throw std::runtime_error("failed creating parent dir for " + path);
    }
}

struct Logger {
    std::ofstream fp;

    explicit Logger(const std::string& path) {
        if (!path.empty()) {
            ensure_parent_dir(path);
            fp.open(path, std::ios::out | std::ios::trunc);
            if (!fp) throw std::runtime_error("failed opening log file: " + path);
        }
    }

    void log(const std::string& line) {
        if (fp) {
            fp << iso_ts() << ' ' << line << '\n';
            fp.flush();
        }
    }
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

int parse_int_after(const std::string& line, const std::string& key, int fallback = -1) {
    const std::string needle = key + "=";
    const auto pos = line.find(needle);
    if (pos == std::string::npos) return fallback;
    std::size_t idx = pos + needle.size();
    std::size_t end = idx;
    while (end < line.size() && line[end] != ' ') ++end;
    try {
        return std::stoi(line.substr(idx, end - idx), nullptr, 0);
    } catch (...) {
        return fallback;
    }
}

std::string parse_str_after(const std::string& line, const std::string& key) {
    const std::string needle = key + "=";
    const auto pos = line.find(needle);
    if (pos == std::string::npos) return {};
    std::size_t idx = pos + needle.size();
    std::size_t end = idx;
    while (end < line.size() && line[end] != ' ') ++end;
    return line.substr(idx, end - idx);
}

class SerialClient {
public:
    SerialClient(std::string name, std::string port_path, int baud, std::string log_path)
        : name_(std::move(name)), port_path_(std::move(port_path)), baud_(baud), logger_(std::move(log_path)) {}

    ~SerialClient() {
        close_port();
    }

    void open_port() {
        fd_ = ::open(port_path_.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
        if (fd_ < 0) {
            throw std::runtime_error(name_ + ": failed opening " + port_path_ + ": " + std::strerror(errno));
        }

        termios tio{};
        if (tcgetattr(fd_, &tio) != 0) {
            throw std::runtime_error(name_ + ": tcgetattr failed");
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
            throw std::runtime_error(name_ + ": tcsetattr failed");
        }
        tcflush(fd_, TCIOFLUSH);
    }

    void close_port() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    void pump() {
        if (fd_ < 0) return;
        char buf[256];
        while (true) {
            const ssize_t n = ::read(fd_, buf, sizeof(buf));
            if (n > 0) {
                rx_buf_.append(buf, static_cast<std::size_t>(n));
                process_rx_buffer();
                continue;
            }
            if (n == 0) return;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            throw std::runtime_error(name_ + ": serial read failed: " + std::strerror(errno));
        }
    }

    void send_cmd(const std::string& cmd) {
        if (fd_ < 0) throw std::runtime_error(name_ + ": serial not open");
        const std::string payload = cmd + "\n";
        const auto now = steady_ms();
        const int64_t wait_ms = 40 - (now - last_tx_ms_);
        if (wait_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
        }
        const ssize_t wrote = ::write(fd_, payload.data(), payload.size());
        if (wrote != static_cast<ssize_t>(payload.size())) {
            throw std::runtime_error(name_ + ": serial write failed");
        }
        tcdrain(fd_);
        last_tx_ms_ = steady_ms();
        logger_.log("> " + cmd);
    }

    void query_status() {
        status_seq_++;
        send_cmd("CTRL STATUS");
    }

    bool relay_closed(int relay_idx) const {
        if (!status_.valid) return false;
        return status_.relay_state(relay_idx) == 1;
    }

    const PlcStatus& status() const {
        return status_;
    }

    int64_t last_status_ms() const {
        return last_status_ms_;
    }

private:
    static int64_t steady_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }

    void process_rx_buffer() {
        while (true) {
            const auto pos = rx_buf_.find('\n');
            if (pos == std::string::npos) return;
            std::string line = rx_buf_.substr(0, pos);
            rx_buf_.erase(0, pos + 1);
            while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
            logger_.log(line);

            if (!pending_status_.empty()) {
                if (!line.empty() && line.front() != '[') {
                    const std::string combined = pending_status_ + line;
                    pending_status_.clear();
                    process_line(combined);
                    continue;
                }
                process_line(pending_status_);
                pending_status_.clear();
            }

            if (line.rfind("[SERCTRL] STATUS", 0) == 0 && line.find("stop_done=") == std::string::npos) {
                pending_status_ = line;
                continue;
            }
            process_line(line);
        }
    }

    void process_line(const std::string& line) {
        if (line.rfind("[SERCTRL] MODE", 0) == 0) {
            status_.valid = true;
            status_.mode_id = parse_int_after(line, "mode");
            const auto mode_pos = line.find("mode=");
            if (mode_pos != std::string::npos) {
                const auto open = line.find('(', mode_pos);
                const auto close = line.find(')', open == std::string::npos ? mode_pos : open);
                if (open != std::string::npos && close != std::string::npos && close > open) {
                    status_.mode_name = line.substr(open + 1, close - open - 1);
                }
            }
            status_.plc_id = parse_int_after(line, "plc_id");
            status_.connector_id = parse_int_after(line, "connector_id");
            status_.controller_id = parse_int_after(line, "controller_id");
            status_.module_addr = parse_int_after(line, "module_addr");
            status_.can_stack = parse_int_after(line, "can_stack");
            status_.module_mgr = parse_int_after(line, "module_mgr");
            last_status_ms_ = steady_ms();
            return;
        }
        if (line.rfind("[SERCTRL] STATUS", 0) == 0) {
            PlcStatus st;
            st.valid = true;
            st.mode_id = parse_int_after(line, "mode");
            const auto mode_pos = line.find("mode=");
            if (mode_pos != std::string::npos) {
                const auto open = line.find('(', mode_pos);
                const auto close = line.find(')', open == std::string::npos ? mode_pos : open);
                if (open != std::string::npos && close != std::string::npos && close > open) {
                    st.mode_name = line.substr(open + 1, close - open - 1);
                }
            }
            st.plc_id = parse_int_after(line, "plc_id");
            st.connector_id = parse_int_after(line, "connector_id");
            st.controller_id = parse_int_after(line, "controller_id");
            st.module_addr = parse_int_after(line, "module_addr");
            st.local_group = parse_int_after(line, "local_group");
            st.module_id = parse_str_after(line, "module_id");
            st.can_stack = parse_int_after(line, "can_stack");
            st.module_mgr = parse_int_after(line, "module_mgr");
            st.cp = parse_str_after(line, "cp");
            st.duty = parse_int_after(line, "duty");
            st.relay1 = parse_int_after(line, "relay1");
            st.relay2 = parse_int_after(line, "relay2");
            st.relay3 = parse_int_after(line, "relay3");
            st.alloc_sz = parse_int_after(line, "alloc_sz");
            st.stop_done = parse_int_after(line, "stop_done");
            status_ = st;
            last_status_ms_ = steady_ms();
        }
    }

    std::string name_;
    std::string port_path_;
    int baud_{115200};
    int fd_{-1};
    Logger logger_;
    std::string rx_buf_;
    std::string pending_status_;
    PlcStatus status_{};
    int64_t last_status_ms_{0};
    int64_t last_tx_ms_{0};
    uint32_t status_seq_{0};
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
        int flags = fcntl(sock_, F_GETFL, 0);
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
        const ssize_t rc = ::write(sock_, &raw, sizeof(raw));
        if (rc == static_cast<ssize_t>(sizeof(raw))) {
            logger_.log("TX id=0x" + hex_u32(frame.id) + " dlc=" + std::to_string(frame.dlc) + " data=" + data_hex(frame));
            return true;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
            return false;
        }
        throw std::runtime_error("CAN write failed: " + std::string(std::strerror(errno)));
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
        return static_cast<uint32_t>(
            duration_cast<milliseconds>(steady_clock::now() - start_).count());
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

struct PhaseMetrics {
    std::string name;
    int samples{0};
    double min_bus_v{1.0e9};
    double max_bus_v{0.0};
    double min_home_v{1.0e9};
    double max_home_v{0.0};
    double min_donor_v{1.0e9};
    double max_donor_v{0.0};
    double min_total_i{1.0e9};
    double max_total_i{0.0};
    int low_voltage_events{0};

    void observe(double bus_v, double home_v, double donor_v, double total_i) {
        ++samples;
        min_bus_v = std::min(min_bus_v, bus_v);
        max_bus_v = std::max(max_bus_v, bus_v);
        min_home_v = std::min(min_home_v, home_v);
        max_home_v = std::max(max_home_v, home_v);
        min_donor_v = std::min(min_donor_v, donor_v);
        max_donor_v = std::max(max_donor_v, donor_v);
        min_total_i = std::min(min_total_i, total_i);
        max_total_i = std::max(max_total_i, total_i);
    }
};

struct Config {
    std::string plc1_port{"/dev/ttyACM0"};
    std::string plc2_port{"/dev/ttyACM1"};
    std::string can_iface{"can0"};
    int baud{115200};
    int controller_id{1};
    int plc1_id{1};
    int plc2_id{2};
    double home_voltage_v{500.0};
    double shared_voltage_v{500.0};
    double home_current_a{40.0};
    double shared_current_a{40.0};
    double donor_precharge_current_a{0.5};
    double startup_current_per_module_a{1.0};
    double home_duration_s{40.0};
    double shared_duration_s{40.0};
    double post_release_duration_s{40.0};
    double transition_grace_s{3.0};
    double min_home_bus_v{450.0};
    double min_shared_bus_v{450.0};
    double match_tolerance_v{15.0};
    double release_current_a{1.0};
    int relay_hold_ms{1500};
    double relay_refresh_s{0.6};
    double telemetry_interval_s{1.0};
    double progress_interval_s{5.0};
    double status_interval_s{10.0};
    int startup_timeout_s{30};
    int release_timeout_s{20};
    int max_total_s{180};
    double rated_current_a{50.0};
    double rated_power_kw{25.0};
    std::string group_id{"DC_BUS"};
    std::string plc1_log;
    std::string plc2_log;
    std::string can_log;
    std::string summary_file;
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
    cfg.plc1_log = "/home/jpi/Desktop/EVSE/plc_firmware/logs/cbmodules_transition_" + stamp + "_plc1.log";
    cfg.plc2_log = "/home/jpi/Desktop/EVSE/plc_firmware/logs/cbmodules_transition_" + stamp + "_plc2.log";
    cfg.can_log = "/home/jpi/Desktop/EVSE/plc_firmware/logs/cbmodules_transition_" + stamp + "_can.log";
    cfg.summary_file =
        "/home/jpi/Desktop/EVSE/plc_firmware/logs/cbmodules_transition_" + stamp + "_summary.json";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const char* name) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++i];
        };
        if (arg == "--plc1-port") cfg.plc1_port = need("--plc1-port");
        else if (arg == "--plc2-port") cfg.plc2_port = need("--plc2-port");
        else if (arg == "--can-iface") cfg.can_iface = need("--can-iface");
        else if (arg == "--baud") cfg.baud = std::stoi(need("--baud"));
        else if (arg == "--controller-id") cfg.controller_id = std::stoi(need("--controller-id"));
        else if (arg == "--plc1-id") cfg.plc1_id = std::stoi(need("--plc1-id"));
        else if (arg == "--plc2-id") cfg.plc2_id = std::stoi(need("--plc2-id"));
        else if (arg == "--home-voltage-v") cfg.home_voltage_v = std::stod(need("--home-voltage-v"));
        else if (arg == "--shared-voltage-v") cfg.shared_voltage_v = std::stod(need("--shared-voltage-v"));
        else if (arg == "--home-current-a") cfg.home_current_a = std::stod(need("--home-current-a"));
        else if (arg == "--shared-current-a") cfg.shared_current_a = std::stod(need("--shared-current-a"));
        else if (arg == "--donor-precharge-current-a")
            cfg.donor_precharge_current_a = std::stod(need("--donor-precharge-current-a"));
        else if (arg == "--startup-current-per-module-a")
            cfg.startup_current_per_module_a = std::stod(need("--startup-current-per-module-a"));
        else if (arg == "--home-duration-s")
            cfg.home_duration_s = std::stod(need("--home-duration-s"));
        else if (arg == "--shared-duration-s")
            cfg.shared_duration_s = std::stod(need("--shared-duration-s"));
        else if (arg == "--post-release-duration-s")
            cfg.post_release_duration_s = std::stod(need("--post-release-duration-s"));
        else if (arg == "--transition-grace-s")
            cfg.transition_grace_s = std::stod(need("--transition-grace-s"));
        else if (arg == "--min-home-bus-v")
            cfg.min_home_bus_v = std::stod(need("--min-home-bus-v"));
        else if (arg == "--min-shared-bus-v")
            cfg.min_shared_bus_v = std::stod(need("--min-shared-bus-v"));
        else if (arg == "--match-tolerance-v")
            cfg.match_tolerance_v = std::stod(need("--match-tolerance-v"));
        else if (arg == "--release-current-a")
            cfg.release_current_a = std::stod(need("--release-current-a"));
        else if (arg == "--relay-hold-ms")
            cfg.relay_hold_ms = std::stoi(need("--relay-hold-ms"));
        else if (arg == "--relay-refresh-s")
            cfg.relay_refresh_s = std::stod(need("--relay-refresh-s"));
        else if (arg == "--telemetry-interval-s")
            cfg.telemetry_interval_s = std::stod(need("--telemetry-interval-s"));
        else if (arg == "--progress-interval-s")
            cfg.progress_interval_s = std::stod(need("--progress-interval-s"));
        else if (arg == "--status-interval-s")
            cfg.status_interval_s = std::stod(need("--status-interval-s"));
        else if (arg == "--startup-timeout-s")
            cfg.startup_timeout_s = std::stoi(need("--startup-timeout-s"));
        else if (arg == "--release-timeout-s")
            cfg.release_timeout_s = std::stoi(need("--release-timeout-s"));
        else if (arg == "--max-total-s")
            cfg.max_total_s = std::stoi(need("--max-total-s"));
        else if (arg == "--rated-current-a")
            cfg.rated_current_a = std::stod(need("--rated-current-a"));
        else if (arg == "--rated-power-kw")
            cfg.rated_power_kw = std::stod(need("--rated-power-kw"));
        else if (arg == "--plc1-log") cfg.plc1_log = need("--plc1-log");
        else if (arg == "--plc2-log") cfg.plc2_log = need("--plc2-log");
        else if (arg == "--can-log") cfg.can_log = need("--can-log");
        else if (arg == "--summary-file") cfg.summary_file = need("--summary-file");
        else if (arg == "--help") {
            std::cout
                << "cbmodules_module_transition_test [options]\n"
                << "  --plc1-port PATH\n"
                << "  --plc2-port PATH\n"
                << "  --can-iface IFACE\n"
                << "  --home-voltage-v N\n"
                << "  --shared-voltage-v N\n"
                << "  --home-current-a N\n"
                << "  --shared-current-a N\n"
                << "  --donor-precharge-current-a N\n"
                << "  --startup-current-per-module-a N\n"
                << "  --home-duration-s N\n"
                << "  --shared-duration-s N\n"
                << "  --post-release-duration-s N\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown arg: " + arg);
        }
    }
    return cfg;
}

struct ModuleViewSnapshot {
    bool present{false};
    bool online{false};
    bool module_off{true};
    bool faulted{false};
    bool lease_active{false};
    bool pending_remove{false};
    bool off_verified{false};
    ModuleTransitionState transition{ModuleTransitionState::Idle};
    std::string active_group;
    double voltage_v{0.0};
    double current_a{0.0};
};

class Runner {
public:
    explicit Runner(Config cfg)
        : cfg_(std::move(cfg)),
          plc1_("plc1", cfg_.plc1_port, cfg_.baud, cfg_.plc1_log),
          plc2_("plc2", cfg_.plc2_port, cfg_.baud, cfg_.plc2_log),
          can_(cfg_.can_iface, cfg_.can_log) {}

    int run() {
        std::string result = "fail";
        try {
            plc1_.open_port();
            plc2_.open_port();
            can_.open_socket();
            std::cout << "PLC1_LOG=" << cfg_.plc1_log << "\n";
            std::cout << "PLC2_LOG=" << cfg_.plc2_log << "\n";
            std::cout << "CAN_LOG=" << cfg_.can_log << "\n";
            std::cout << "SUMMARY_FILE=" << cfg_.summary_file << "\n";

            bootstrap_plc(plc1_, cfg_.plc1_id);
            bootstrap_plc(plc2_, cfg_.plc2_id);

            ModuleManagerConfig mm_cfg;
            mm_cfg.can_max_frames_per_second = 180.0f;
            mm_cfg.can_max_tx_per_tick = 24;
            mm_cfg.can_max_rx_per_tick = 64;
            mm_cfg.command_min_interval_ms = 80;
            mm_cfg.command_keepalive_ms = 500;
            mm_cfg.command_keepalive_stable_ms = 1200;
            mm_cfg.command_settle_window_ms = 2000;
            mm_cfg.telemetry_stale_ms = 2000;
            mm_cfg.runtime_probe_interval_ms = 250;
            mm_cfg.module_lease_default_timeout_ms = 30000;
            mm_cfg.off_verify_current_a = static_cast<float>(cfg_.release_current_a);
            mm_cfg.off_verify_samples = 2;
            mm_cfg.off_verify_timeout_ms = 2000;
            mm_cfg.ramp_up_voltage_v_per_s = 250.0f;
            mm_cfg.ramp_up_current_a_per_s = 80.0f;
            mm_cfg.ramp_down_voltage_v_per_s = 350.0f;
            mm_cfg.ramp_down_current_a_per_s = 120.0f;
            mm_cfg.startup_current_per_module_a = static_cast<float>(cfg_.startup_current_per_module_a);
            mm_cfg.safety.enable_multi_controller_guard = false;
            manager_ = std::make_unique<ModuleManager>(mm_cfg);
            manager_->set_transport(&can_);

            ModuleSpec home{};
            home.id = "PLC1_MXR_01";
            home.type = ModuleType::MaxwellMxr;
            home.address = 1;
            home.group = 1;
            home.input_mode = 3;
            home.rated_current_a = static_cast<float>(cfg_.rated_current_a);
            home.rated_power_kw = static_cast<float>(cfg_.rated_power_kw);

            ModuleSpec donor = home;
            donor.id = "LEASED_MXR_03";
            donor.address = 3;
            donor.group = 2;

            manager_->set_inventory({home, donor});
            const auto scan = manager_->startup_scan_validate(3000);
            if (scan.validated_count < 2) {
                throw std::runtime_error("cbmodules startup scan did not validate both modules");
            }

            GroupConfig group{};
            group.id = cfg_.group_id;
            group.min_modules = 1;
            group.max_modules = 2;
            group.efficient_module_usage = false;
            group.hard_safety_clamp = true;
            group.max_group_current_a = 100.0f;
            if (!manager_->upsert_group(group)) {
                throw std::runtime_error("failed to upsert group");
            }
            if (!manager_->allocate_module(home.id, group.id)) {
                throw std::runtime_error("failed to allocate home module");
            }
            if (!manager_->set_group_target(group.id, enabled_target(cfg_.home_voltage_v, cfg_.home_current_a))) {
                throw std::runtime_error("failed to set home target");
            }

            desired_relay1_closed_ = true;
            phase_ = "home_1_startup";
            phase_started_ms_ = now_ms();
            wait_until(cfg_.startup_timeout_s * 1000, "PLC1 Relay1 did not close",
                       [&] { return plc1_.relay_closed(1); });
            wait_until(cfg_.startup_timeout_s * 1000, "home module failed to reach target voltage", [&] {
                return home_view().present && home_view().online && !home_view().module_off &&
                       home_view().voltage_v >= cfg_.min_home_bus_v;
            });

            set_phase("home_1");
            hold_for(static_cast<int>(cfg_.home_duration_s * 1000.0));

            set_phase("shared_prepare");
            const double live_home_bus_v = bus_voltage_estimate();
            const double attach_voltage_v = (live_home_bus_v >= cfg_.min_home_bus_v) ? live_home_bus_v : cfg_.shared_voltage_v;
            ModuleLeaseRequest lease{};
            lease.module_id = donor.id;
            lease.enable = true;
            lease.voltage_v = static_cast<float>(attach_voltage_v);
            lease.current_a = static_cast<float>(cfg_.donor_precharge_current_a);
            lease.hold_timeout_ms = 30000;
            if (!manager_->set_module_lease(lease)) {
                throw std::runtime_error("failed to start donor precharge lease");
            }
            wait_until(cfg_.startup_timeout_s * 1000, "donor module failed isolated precharge match", [&] {
                const auto dv = donor_view();
                return dv.present && dv.online && !dv.module_off &&
                       std::fabs(dv.voltage_v - attach_voltage_v) <= cfg_.match_tolerance_v;
            });
            desired_relay2_closed_ = true;
            wait_until(cfg_.startup_timeout_s * 1000, "PLC2 Relay2 did not close",
                       [&] { return plc2_.relay_closed(2); });
            if (!manager_->allocate_module(donor.id, group.id)) {
                throw std::runtime_error("failed to allocate donor module into active group");
            }
            if (!manager_->set_group_target(group.id, enabled_target(cfg_.shared_voltage_v, cfg_.shared_current_a))) {
                throw std::runtime_error("failed to raise shared target");
            }
            set_phase("shared");
            wait_until(cfg_.startup_timeout_s * 1000, "shared bus failed to stabilize", [&] {
                const GroupStatus st = manager_->group_status(cfg_.group_id);
                return st.active_modules >= 2 && bus_voltage_estimate() >= cfg_.min_shared_bus_v &&
                       donor_view().current_a >= 5.0;
            });
            hold_for(static_cast<int>(cfg_.shared_duration_s * 1000.0));

            set_phase("release_prepare");
            if (!manager_->set_group_target(group.id, enabled_target(cfg_.home_voltage_v, cfg_.home_current_a))) {
                throw std::runtime_error("failed to return to home target before release");
            }
            if (!manager_->deallocate_module(donor.id)) {
                throw std::runtime_error("failed to start donor deallocation");
            }
            wait_until(cfg_.release_timeout_s * 1000, "donor did not release cleanly", [&] {
                const auto dv = donor_view();
                return dv.present && dv.active_group.empty() && dv.off_verified && std::fabs(dv.current_a) <= cfg_.release_current_a;
            });
            desired_relay2_closed_ = false;
            wait_until(cfg_.release_timeout_s * 1000, "PLC2 Relay2 did not open",
                       [&] { return !plc2_.relay_closed(2); });

            set_phase("home_2");
            wait_until(cfg_.release_timeout_s * 1000, "home path did not recover after donor release", [&] {
                return bus_voltage_estimate() >= cfg_.min_home_bus_v;
            });
            hold_for(static_cast<int>(cfg_.post_release_duration_s * 1000.0));

            set_phase("stop");
            (void)manager_->set_group_target(cfg_.group_id, GroupTarget{});
            wait_until(10000, "home module did not turn off", [&] {
                const auto hv = home_view();
                return hv.present && hv.module_off && std::fabs(hv.current_a) <= cfg_.release_current_a;
            });
            desired_relay1_closed_ = false;
            wait_until(cfg_.release_timeout_s * 1000, "PLC1 Relay1 did not open",
                       [&] { return !plc1_.relay_closed(1); });
            result = failures_.empty() ? "pass" : "fail";
            if (result == "pass") {
                std::cout << "[PASS] cbmodules transition completed without bus collapse\n";
            } else {
                std::cout << "[FAIL] failures present\n";
            }
        } catch (const std::exception& exc) {
            failures_.push_back(exc.what());
            std::cout << "[FAIL] " << exc.what() << "\n";
        }

        cleanup();
        write_summary(result);
        std::cout << "SUMMARY_FILE_DONE=" << cfg_.summary_file << "\n";
        return (result == "pass") ? 0 : 1;
    }

private:
    GroupTarget enabled_target(double voltage_v, double current_a) const {
        GroupTarget t{};
        t.enable = true;
        t.voltage_v = static_cast<float>(voltage_v);
        t.current_a = static_cast<float>(current_a);
        return t;
    }

    uint32_t now_ms() const {
        return can_.now_ms();
    }

    void bootstrap_plc(SerialClient& plc, int plc_id) {
        plc.send_cmd("CTRL MODE 2 " + std::to_string(plc_id) + " " + std::to_string(cfg_.controller_id));
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        plc.pump();
        plc.send_cmd("CTRL STOP clear 3000");
        plc.send_cmd("CTRL RELAY 1 0 0");
        plc.send_cmd("CTRL RELAY 2 0 0");
        plc.send_cmd("CTRL RELAY 4 0 0");
        plc.query_status();
        uint32_t last_query_ms = now_ms();
        wait_until(5000, "PLC failed to enter mode 2", [&] {
            plc.pump();
            const uint32_t now = now_ms();
            if (now >= last_query_ms + 750u) {
                plc.query_status();
                last_query_ms = now;
            }
            const auto& st = plc.status();
            return st.valid && st.mode_id == 2 && st.can_stack == 0 && st.module_mgr == 0;
        });
    }

    ModuleViewSnapshot snapshot_for(const std::string& module_id) {
        ModuleViewSnapshot out{};
        const auto views = manager_->module_states();
        for (const auto& view : views) {
            if (view.id != module_id) continue;
            out.present = true;
            out.online = view.telemetry.online;
            out.module_off = view.telemetry.module_off;
            out.faulted = view.telemetry.faulted;
            out.lease_active = view.lease_active;
            out.pending_remove = view.pending_remove;
            out.off_verified = view.off_verified;
            out.transition = view.transition;
            out.active_group = view.group_id;
            out.voltage_v = view.telemetry.voltage_v;
            out.current_a = view.telemetry.current_a;
            return out;
        }
        return out;
    }

    ModuleViewSnapshot home_view() {
        return snapshot_for("PLC1_MXR_01");
    }

    ModuleViewSnapshot donor_view() {
        return snapshot_for("LEASED_MXR_03");
    }

    double bus_voltage_estimate() {
        const auto hv = home_view();
        const auto dv = donor_view();
        if (desired_relay1_closed_ && hv.present && hv.voltage_v > 0.0) return hv.voltage_v;
        if (desired_relay2_closed_ && dv.present && dv.voltage_v > 0.0) return dv.voltage_v;
        return 0.0;
    }

    double bus_current_estimate() {
        const auto hv = home_view();
        const auto dv = donor_view();
        double total = 0.0;
        if (desired_relay1_closed_ && hv.present) total += std::max(0.0, hv.current_a);
        if (desired_relay2_closed_ && dv.present) total += std::max(0.0, dv.current_a);
        return total;
    }

    void set_phase(const std::string& phase) {
        phase_ = phase;
        phase_started_ms_ = now_ms();
        metrics_[phase].name = phase;
        std::cout << "[PHASE] " << phase << "\n";
        events_.push_back(iso_ts() + " phase=" + phase);
    }

    void step_once() {
        plc1_.pump();
        plc2_.pump();
        const uint32_t now = now_ms();

        if (desired_relay1_closed_ && (now - last_relay1_refresh_ms_) >= static_cast<uint32_t>(cfg_.relay_refresh_s * 1000.0)) {
            plc1_.send_cmd("CTRL RELAY 1 1 " + std::to_string(cfg_.relay_hold_ms));
            last_relay1_refresh_ms_ = now;
        } else if (!desired_relay1_closed_ && plc1_.relay_closed(1) &&
                   (now - last_relay1_refresh_ms_) >= 800u) {
            plc1_.send_cmd("CTRL RELAY 1 0 0");
            last_relay1_refresh_ms_ = now;
        }

        if (desired_relay2_closed_ && (now - last_relay2_refresh_ms_) >= static_cast<uint32_t>(cfg_.relay_refresh_s * 1000.0)) {
            plc2_.send_cmd("CTRL RELAY 2 2 " + std::to_string(cfg_.relay_hold_ms));
            last_relay2_refresh_ms_ = now;
        } else if (!desired_relay2_closed_ && plc2_.relay_closed(2) &&
                   (now - last_relay2_refresh_ms_) >= 800u) {
            plc2_.send_cmd("CTRL RELAY 2 0 0");
            last_relay2_refresh_ms_ = now;
        }

        if ((now - last_status_poll_ms_) >= static_cast<uint32_t>(cfg_.status_interval_s * 1000.0)) {
            plc1_.query_status();
            plc2_.query_status();
            last_status_poll_ms_ = now;
        }

        manager_->tick(now);

        if ((now - last_telemetry_ms_) >= static_cast<uint32_t>(cfg_.telemetry_interval_s * 1000.0)) {
            record_metrics();
            last_telemetry_ms_ = now;
        }

        if ((now - last_progress_ms_) >= static_cast<uint32_t>(cfg_.progress_interval_s * 1000.0)) {
            const auto hv = home_view();
            const auto dv = donor_view();
            std::cout << "[PROGRESS] phase=" << phase_ << " relay1=" << (plc1_.relay_closed(1) ? 1 : 0)
                      << " relay2=" << (plc2_.relay_closed(2) ? 1 : 0) << " busV=" << std::fixed
                      << std::setprecision(1) << bus_voltage_estimate() << " busI=" << bus_current_estimate()
                      << " homeV=" << hv.voltage_v << " homeI=" << hv.current_a
                      << " donorV=" << dv.voltage_v << " donorI=" << dv.current_a << "\n";
            last_progress_ms_ = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    void wait_until(uint32_t timeout_ms, const std::string& what, const std::function<bool()>& predicate) {
        const uint32_t deadline = now_ms() + timeout_ms;
        while (now_ms() < deadline) {
            if (predicate()) return;
            if (now_ms() > static_cast<uint32_t>(cfg_.max_total_s * 1000)) {
                throw std::runtime_error("test timeout exceeded");
            }
            step_once();
        }
        throw std::runtime_error(what);
    }

    void hold_for(int duration_ms) {
        const uint32_t end_ms = now_ms() + static_cast<uint32_t>(duration_ms);
        while (now_ms() < end_ms) {
            step_once();
        }
    }

    void record_metrics() {
        auto& pm = metrics_[phase_];
        pm.name = phase_;
        const auto hv = home_view();
        const auto dv = donor_view();
        const double bus_v = bus_voltage_estimate();
        const double bus_i = bus_current_estimate();
        pm.observe(bus_v, hv.voltage_v, dv.voltage_v, bus_i);
        const uint32_t now = now_ms();
        if ((now - phase_started_ms_) < static_cast<uint32_t>(cfg_.transition_grace_s * 1000.0)) {
            return;
        }
        double threshold = 0.0;
        if (phase_ == "home_1" || phase_ == "home_2") threshold = cfg_.min_home_bus_v;
        else if (phase_ == "shared") threshold = cfg_.min_shared_bus_v;
        if (threshold > 0.0 && bus_v > 0.0 && bus_v < threshold) {
            pm.low_voltage_events++;
            if (pm.low_voltage_events == 1) {
                std::ostringstream oss;
                oss << phase_ << " low bus voltage " << std::fixed << std::setprecision(1) << bus_v << " V < "
                    << threshold << " V";
                failures_.push_back(oss.str());
            }
        }
    }

    void cleanup() {
        if (manager_) {
            try {
                manager_->set_group_target(cfg_.group_id, GroupTarget{});
            } catch (...) {
            }
            for (int i = 0; i < 50; ++i) {
                try {
                    manager_->tick(now_ms());
                } catch (...) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        try {
            desired_relay1_closed_ = false;
            desired_relay2_closed_ = false;
            plc1_.send_cmd("CTRL RELAY 1 0 0");
            plc2_.send_cmd("CTRL RELAY 2 0 0");
            plc1_.query_status();
            plc2_.query_status();
            for (int i = 0; i < 20; ++i) {
                plc1_.pump();
                plc2_.pump();
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        } catch (...) {
        }
    }

    static std::string json_escape(const std::string& input) {
        std::ostringstream oss;
        for (char ch : input) {
            switch (ch) {
                case '\\': oss << "\\\\"; break;
                case '"': oss << "\\\""; break;
                case '\n': oss << "\\n"; break;
                case '\r': oss << "\\r"; break;
                case '\t': oss << "\\t"; break;
                default: oss << ch; break;
            }
        }
        return oss.str();
    }

    void write_summary(const std::string& result) {
        ensure_parent_dir(cfg_.summary_file);
        std::ofstream fp(cfg_.summary_file, std::ios::out | std::ios::trunc);
        if (!fp) return;
        const auto hv = home_view();
        const auto dv = donor_view();
        fp << "{\n";
        fp << "  \"result\": \"" << json_escape(result) << "\",\n";
        fp << "  \"phase\": \"" << json_escape(phase_) << "\",\n";
        fp << "  \"failures\": [";
        for (std::size_t i = 0; i < failures_.size(); ++i) {
            if (i) fp << ", ";
            fp << "\"" << json_escape(failures_[i]) << "\"";
        }
        fp << "],\n";
        fp << "  \"events\": [";
        for (std::size_t i = 0; i < events_.size(); ++i) {
            if (i) fp << ", ";
            fp << "\"" << json_escape(events_[i]) << "\"";
        }
        fp << "],\n";
        fp << "  \"home_module\": {\"voltage_v\": " << hv.voltage_v << ", \"current_a\": " << hv.current_a
           << ", \"module_off\": " << (hv.module_off ? "true" : "false") << "},\n";
        fp << "  \"donor_module\": {\"voltage_v\": " << dv.voltage_v << ", \"current_a\": " << dv.current_a
           << ", \"module_off\": " << (dv.module_off ? "true" : "false") << "},\n";
        fp << "  \"phase_metrics\": {\n";
        bool first = true;
        for (const auto& [name, pm] : metrics_) {
            if (!first) fp << ",\n";
            first = false;
            fp << "    \"" << json_escape(name) << "\": {"
               << "\"samples\": " << pm.samples
               << ", \"min_bus_v\": " << (pm.samples ? pm.min_bus_v : 0.0)
               << ", \"max_bus_v\": " << pm.max_bus_v
               << ", \"min_home_v\": " << (pm.samples ? pm.min_home_v : 0.0)
               << ", \"max_home_v\": " << pm.max_home_v
               << ", \"min_donor_v\": " << (pm.samples ? pm.min_donor_v : 0.0)
               << ", \"max_donor_v\": " << pm.max_donor_v
               << ", \"min_total_i\": " << (pm.samples ? pm.min_total_i : 0.0)
               << ", \"max_total_i\": " << pm.max_total_i
               << ", \"low_voltage_events\": " << pm.low_voltage_events << "}";
        }
        fp << "\n  }\n";
        fp << "}\n";
    }

    Config cfg_;
    SerialClient plc1_;
    SerialClient plc2_;
    SocketCanTransport can_;
    std::unique_ptr<ModuleManager> manager_;
    bool desired_relay1_closed_{false};
    bool desired_relay2_closed_{false};
    uint32_t last_relay1_refresh_ms_{0};
    uint32_t last_relay2_refresh_ms_{0};
    uint32_t last_status_poll_ms_{0};
    uint32_t last_telemetry_ms_{0};
    uint32_t last_progress_ms_{0};
    std::string phase_{"bootstrap"};
    uint32_t phase_started_ms_{0};
    std::map<std::string, PhaseMetrics> metrics_;
    std::vector<std::string> failures_;
    std::vector<std::string> events_;
};

} // namespace

int main(int argc, char** argv) {
    try {
        Runner runner(parse_args(argc, argv));
        return runner.run();
    } catch (const std::exception& exc) {
        std::cerr << "[ERR] " << exc.what() << '\n';
        return 2;
    }
}
