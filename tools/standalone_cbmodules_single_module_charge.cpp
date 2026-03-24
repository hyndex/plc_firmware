// SPDX-License-Identifier: Apache-2.0
#include "cbmodules/module_manager.hpp"

#include <fcntl.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <net/if.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
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
using cbmodules::ModuleLifecycle;
using cbmodules::ModuleManager;
using cbmodules::ModuleManagerConfig;
using cbmodules::ModuleSpec;
using cbmodules::ModuleStateView;
using cbmodules::ModuleTransitionState;
using cbmodules::ModuleType;

constexpr float MODULE_MAX_VOLTAGE_V = 1000.0f;
constexpr float MODULE_MAX_CURRENT_A = 100.0f;
constexpr float MODULE_MAX_POWER_KW = 30.0f;
constexpr uint32_t MODULE_ALLOWLIST_LEASE_MS = 3000u;
constexpr int CAN_SEND_TIMEOUT_MS = 20;
constexpr int MODULE_READY_TIMEOUT_MS = 10000;
constexpr int OFF_WAIT_TIMEOUT_MS = 8000;
constexpr int SAMPLE_INTERVAL_MS = 10;
constexpr float OFF_VERIFY_CURRENT_A = 0.20f;
constexpr float HOLD_READY_VOLTAGE_V = 490.0f;
constexpr float HOLD_READY_CURRENT_A = 40.0f;

int64_t steady_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
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

void ensure_parent_dir(const std::string& path) {
    const auto pos = path.find_last_of('/');
    if (pos == std::string::npos) {
        return;
    }
    const auto parent = path.substr(0, pos);
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
}

std::string timestamp_stamp() {
    const auto now = std::chrono::system_clock::now();
    const auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&tt, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
    return oss.str();
}

float sane_non_negative(float value) {
    if (!std::isfinite(value) || value < 0.0f) {
        return 0.0f;
    }
    return value;
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

struct Logger {
    std::ofstream fp;

    explicit Logger(const std::string& path) {
        ensure_parent_dir(path);
        fp.open(path, std::ios::out | std::ios::trunc);
        if (!fp) {
            throw std::runtime_error("failed opening log file: " + path);
        }
    }

    void log(const std::string& line) {
        fp << iso_ts() << ' ' << line << '\n';
        fp.flush();
    }
};

class SocketCanTransport final : public CanTransport {
public:
    SocketCanTransport(std::string iface, std::string log_path)
        : iface_(std::move(iface)), logger_(std::move(log_path)), start_(std::chrono::steady_clock::now()) {}

    ~SocketCanTransport() override { close_socket(); }

    void open_socket() {
        close_socket();
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
        logger_.log("RX id=0x" + hex_u32(frame_out.id) + " dlc=" + std::to_string(frame_out.dlc) + " data=" + data_hex(frame_out));
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
    std::string can_iface{"can0"};
    std::string module_id;
    std::string group_id;
    int home_addr{2};
    int home_group{2};
    int input_mode{-1};
    double target_voltage_v{500.0};
    double target_current_a{50.0};
    double precharge_current_a{2.0};
    double precharge_hold_s{0.6};
    double hold_s{15.0};
    double stop_timeout_s{8.0};
    std::string run_log;
    std::string can_log;
    std::string summary_file;
    bool reset_can_iface{true};
};

Config parse_args(int argc, char** argv) {
    Config cfg;
    const std::string stamp = timestamp_stamp();
    cfg.module_id = "HOST_MXR_02";
    cfg.group_id = "HOST_GROUP_2";
    cfg.run_log = "/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_cbmodules_single_module_charge_" + stamp + ".log";
    cfg.can_log = "/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_cbmodules_single_module_charge_" + stamp + "_can.log";
    cfg.summary_file =
        "/home/jpi/Desktop/EVSE/plc_firmware/logs/standalone_cbmodules_single_module_charge_" + stamp + "_summary.json";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need = [&](const char* name) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + name);
            return argv[++i];
        };
        if (arg == "--can-iface") cfg.can_iface = need("--can-iface");
        else if (arg == "--module-id") cfg.module_id = need("--module-id");
        else if (arg == "--group-id") cfg.group_id = need("--group-id");
        else if (arg == "--home-addr") cfg.home_addr = std::stoi(need("--home-addr"));
        else if (arg == "--home-group") cfg.home_group = std::stoi(need("--home-group"));
        else if (arg == "--input-mode") cfg.input_mode = std::stoi(need("--input-mode"));
        else if (arg == "--target-voltage-v") cfg.target_voltage_v = std::stod(need("--target-voltage-v"));
        else if (arg == "--target-current-a") cfg.target_current_a = std::stod(need("--target-current-a"));
        else if (arg == "--precharge-current-a") cfg.precharge_current_a = std::stod(need("--precharge-current-a"));
        else if (arg == "--precharge-hold-s") cfg.precharge_hold_s = std::stod(need("--precharge-hold-s"));
        else if (arg == "--hold-s") cfg.hold_s = std::stod(need("--hold-s"));
        else if (arg == "--stop-timeout-s") cfg.stop_timeout_s = std::stod(need("--stop-timeout-s"));
        else if (arg == "--run-log") cfg.run_log = need("--run-log");
        else if (arg == "--can-log") cfg.can_log = need("--can-log");
        else if (arg == "--summary-file") cfg.summary_file = need("--summary-file");
        else if (arg == "--no-reset-can-iface") cfg.reset_can_iface = false;
        else if (arg == "--help") {
            std::cout
                << "standalone_cbmodules_single_module_charge [options]\n"
                << "  --can-iface IFACE\n"
                << "  --home-addr N\n"
                << "  --home-group N\n"
                << "  --target-voltage-v N\n"
                << "  --target-current-a N\n"
                << "  --hold-s N\n"
                << "  --no-reset-can-iface\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown arg: " + arg);
        }
    }

    if (cfg.module_id == "HOST_MXR_02") {
        std::ostringstream oss;
        oss << "HOST_MXR_" << std::setw(2) << std::setfill('0') << cfg.home_addr;
        cfg.module_id = oss.str();
    }
    if (cfg.group_id == "HOST_GROUP_2") {
        cfg.group_id = "HOST_GROUP_" + std::to_string(cfg.home_group);
    }
    return cfg;
}

struct ModuleTelemetrySnapshot {
    bool present{false};
    bool online{false};
    bool module_off{true};
    bool faulted{false};
    ModuleLifecycle lifecycle{ModuleLifecycle::Missing};
    ModuleTransitionState transition{ModuleTransitionState::Idle};
    std::string group_id;
    float voltage_v{0.0f};
    float current_a{0.0f};
    uint32_t latest_update_ms{0};
};

struct Sample {
    double t_s{0.0};
    float voltage_v{0.0f};
    float current_a{0.0f};
    bool module_off{true};
};

struct Summary {
    std::string result{"fail"};
    std::string reason;
    bool validated{false};
    bool allocated{false};
    size_t sample_count{0};
    float peak_voltage_v{0.0f};
    float peak_current_a{0.0f};
    double reached_150v_s{-1.0};
    double reached_250v_s{-1.0};
    double reached_400v_s{-1.0};
    double reached_490v_s{-1.0};
    double reached_20a_s{-1.0};
    double reached_30a_s{-1.0};
    double reached_40a_s{-1.0};
    double first_hold_ready_s{-1.0};
    double hold_duration_s{0.0};
};

class Runner {
public:
    explicit Runner(Config cfg) : cfg_(std::move(cfg)), log_(cfg_.run_log), can_(cfg_.can_iface, cfg_.can_log) {}

    int run() {
        try {
            if (cfg_.reset_can_iface) {
                reset_can_interface(cfg_.can_iface);
            }
            can_.open_socket();
            std::cout << "RUN_LOG=" << cfg_.run_log << "\n";
            std::cout << "CAN_LOG=" << cfg_.can_log << "\n";
            std::cout << "SUMMARY_FILE=" << cfg_.summary_file << "\n";
            init_module_manager();
            wait_for_ready_module();
            ensure_off();
            run_sequence();
            ensure_off();
            summary_.result = "pass";
            write_summary();
            return 0;
        } catch (const std::exception& exc) {
            if (summary_.reason.empty()) {
                summary_.reason = exc.what();
            }
            std::cout << "[FAIL] " << exc.what() << "\n";
            try {
                ensure_off();
            } catch (...) {
            }
            write_summary();
            return 1;
        }
    }

private:
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
        cfg.controller_id = 0x0202;

        manager_ = std::make_unique<ModuleManager>(cfg);
        manager_->set_transport(&can_);

        ModuleSpec spec{};
        spec.id = cfg_.module_id;
        spec.type = ModuleType::MaxwellMxr;
        spec.slot_id = static_cast<uint8_t>(std::max(0, cfg_.home_group));
        spec.slot_index = static_cast<uint8_t>(std::max(0, cfg_.home_group));
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

        const auto report = manager_->startup_scan_validate(5000);
        summary_.validated = report.validated_count >= 1u;
        log_.log("[BOOT] scan complete=" + std::to_string(report.complete ? 1 : 0) +
                 " discovered=" + std::to_string(report.discovered_count) +
                 " validated=" + std::to_string(report.validated_count));
        if (!summary_.validated) {
            throw std::runtime_error("cbmodules startup scan did not validate the home module");
        }

        GroupConfig group{};
        group.id = cfg_.group_id;
        group.min_modules = 0;
        group.max_modules = 1;
        group.efficient_module_usage = true;
        group.hard_safety_clamp = true;
        group.max_group_current_a = MODULE_MAX_CURRENT_A;
        group.max_group_power_kw = MODULE_MAX_POWER_KW;
        if (!manager_->upsert_group(group)) {
            throw std::runtime_error("failed to upsert host group");
        }
        manager_->set_module_hard_limits(cfg_.module_id, MODULE_MAX_CURRENT_A, MODULE_MAX_POWER_KW);
        const GroupTarget off_target{false, 0.0f, 0.0f};
        if (!manager_->apply_group_setpoint_allowlist(cfg_.group_id, off_target, {cfg_.module_id}, MODULE_ALLOWLIST_LEASE_MS)) {
            throw std::runtime_error("failed to allocate module into host group");
        }
        manager_->set_group_target(cfg_.group_id, off_target);
        service();
        summary_.allocated = group_status().assigned_modules > 0u;
        if (!summary_.allocated) {
            throw std::runtime_error("module allocation did not assign the home module");
        }
    }

    void service() {
        if (!manager_) {
            return;
        }
        manager_->poll_rx(64);
        manager_->tick(can_.now_ms());
    }

    GroupStatus group_status() const {
        return manager_ ? manager_->group_status(cfg_.group_id) : GroupStatus{};
    }

    ModuleTelemetrySnapshot module_snapshot() const {
        ModuleTelemetrySnapshot snap;
        if (!manager_) {
            return snap;
        }
        for (const auto& state : manager_->module_states()) {
            if (state.id != cfg_.module_id) {
                continue;
            }
            snap.present = true;
            snap.online = state.telemetry.online;
            snap.module_off = state.telemetry.module_off;
            snap.faulted = state.telemetry.faulted;
            snap.lifecycle = state.lifecycle;
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

    void wait_for_ready_module() {
        const int64_t deadline = steady_ms() + MODULE_READY_TIMEOUT_MS;
        while (steady_ms() < deadline) {
            service();
            const auto status = group_status();
            const auto snap = module_snapshot();
            if (status.assigned_modules > 0u && snap.present && snap.online && snap.latest_update_ms != 0u) {
                std::cout << "[BOOT] host module ready assigned=" << status.assigned_modules
                          << " online=" << (snap.online ? 1 : 0)
                          << " off=" << (snap.module_off ? 1 : 0)
                          << " lc=" << lifecycle_name(snap.lifecycle)
                          << " tr=" << transition_name(snap.transition)
                          << " v=" << std::fixed << std::setprecision(1) << snap.voltage_v
                          << " i=" << snap.current_a << "\n";
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        throw std::runtime_error("host module manager did not settle before test");
    }

    bool apply_target(bool enable, float voltage_v, float current_a, const char* reason) {
        service();
        GroupTarget target{};
        target.enable = enable;
        target.voltage_v = std::clamp(voltage_v, 0.0f, MODULE_MAX_VOLTAGE_V);
        target.current_a = enable ? std::clamp(current_a, 0.0f, MODULE_MAX_CURRENT_A) : 0.0f;
        const bool ok =
            manager_->apply_group_setpoint_allowlist(cfg_.group_id, target, {cfg_.module_id}, MODULE_ALLOWLIST_LEASE_MS);
        service();
        const auto status = group_status();
        const auto snap = module_snapshot();
        std::ostringstream oss;
        oss << "[HOSTMOD] reason=" << reason
            << " en=" << (enable ? 1 : 0)
            << " reqV=" << std::fixed << std::setprecision(1) << target.voltage_v
            << " reqI=" << target.current_a
            << " ok=" << (ok ? 1 : 0)
            << " assigned=" << status.assigned_modules
            << " active=" << status.active_modules
            << " combinedV=" << status.combined_voltage_v
            << " combinedI=" << status.combined_current_a
            << " lc=" << lifecycle_name(snap.lifecycle)
            << " tr=" << transition_name(snap.transition)
            << " off=" << (snap.module_off ? 1 : 0)
            << " online=" << (snap.online ? 1 : 0)
            << " snapV=" << snap.voltage_v
            << " snapI=" << snap.current_a;
        log_.log(oss.str());
        return ok;
    }

    void refresh_allowlist_if_due(int64_t now_ms) {
        if ((now_ms - last_allowlist_refresh_ms_) < 1000) {
            return;
        }
        last_allowlist_refresh_ms_ = now_ms;
        (void)manager_->refresh_group_allowlist_lease(cfg_.group_id, {cfg_.module_id}, MODULE_ALLOWLIST_LEASE_MS);
        service();
    }

    void ensure_off() {
        if (!manager_) {
            return;
        }
        (void)apply_target(false, 0.0f, 0.0f, "off");
        const int64_t deadline = steady_ms() + static_cast<int64_t>(cfg_.stop_timeout_s * 1000.0);
        while (steady_ms() < deadline) {
            service();
            const auto snap = module_snapshot();
            if (snap.present && snap.online && snap.module_off && snap.current_a <= OFF_VERIFY_CURRENT_A) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    Sample sample_now(double ramp_t_s) {
        service();
        const auto status = group_status();
        const auto snap = module_snapshot();
        float present_v = sane_non_negative(status.combined_voltage_v);
        float present_i = sane_non_negative(status.combined_current_a);
        if (present_v <= 0.1f) present_v = snap.voltage_v;
        if (present_i <= 0.05f) present_i = snap.current_a;
        Sample sample;
        sample.t_s = ramp_t_s;
        sample.voltage_v = present_v;
        sample.current_a = present_i;
        sample.module_off = snap.module_off;
        return sample;
    }

    void record_threshold(double& slot, double t_s, bool hit) {
        if (slot < 0.0 && hit) {
            slot = t_s;
        }
    }

    void record_sample(const Sample& sample) {
        summary_.sample_count++;
        summary_.peak_voltage_v = std::max(summary_.peak_voltage_v, sample.voltage_v);
        summary_.peak_current_a = std::max(summary_.peak_current_a, sample.current_a);
        record_threshold(summary_.reached_150v_s, sample.t_s, sample.voltage_v >= 150.0f);
        record_threshold(summary_.reached_250v_s, sample.t_s, sample.voltage_v >= 250.0f);
        record_threshold(summary_.reached_400v_s, sample.t_s, sample.voltage_v >= 400.0f);
        record_threshold(summary_.reached_490v_s, sample.t_s, sample.voltage_v >= 490.0f);
        record_threshold(summary_.reached_20a_s, sample.t_s, sample.current_a >= 20.0f);
        record_threshold(summary_.reached_30a_s, sample.t_s, sample.current_a >= 30.0f);
        record_threshold(summary_.reached_40a_s, sample.t_s, sample.current_a >= 40.0f);
        if (summary_.first_hold_ready_s < 0.0 &&
            sample.voltage_v >= HOLD_READY_VOLTAGE_V &&
            sample.current_a >= HOLD_READY_CURRENT_A) {
            summary_.first_hold_ready_s = sample.t_s;
        }
    }

    void run_sequence() {
        const float target_v = static_cast<float>(cfg_.target_voltage_v);
        const float target_i = static_cast<float>(cfg_.target_current_a);
        const float precharge_i = static_cast<float>(cfg_.precharge_current_a);

        if (cfg_.precharge_hold_s > 0.0) {
            if (!apply_target(true, target_v, precharge_i, "precharge")) {
                throw std::runtime_error("failed to apply precharge target");
            }
            const int64_t precharge_deadline = steady_ms() + static_cast<int64_t>(cfg_.precharge_hold_s * 1000.0);
            while (steady_ms() < precharge_deadline) {
                refresh_allowlist_if_due(steady_ms());
                const Sample sample = sample_now(-1.0);
                log_sample("precharge", sample);
                std::this_thread::sleep_for(std::chrono::milliseconds(SAMPLE_INTERVAL_MS));
            }
        }

        if (!apply_target(true, target_v, target_i, "current_demand")) {
            throw std::runtime_error("failed to apply current-demand target");
        }

        const int64_t ramp_start_ms = steady_ms();
        const int64_t hold_deadline_ms = ramp_start_ms + static_cast<int64_t>(cfg_.hold_s * 1000.0);
        while (steady_ms() < hold_deadline_ms) {
            const int64_t now_ms = steady_ms();
            refresh_allowlist_if_due(now_ms);
            const double t_s = static_cast<double>(now_ms - ramp_start_ms) / 1000.0;
            const Sample sample = sample_now(t_s);
            record_sample(sample);
            log_sample("current_demand", sample);
            std::this_thread::sleep_for(std::chrono::milliseconds(SAMPLE_INTERVAL_MS));
        }

        if (summary_.first_hold_ready_s < 0.0) {
            summary_.reason = "never reached stable high-output hold";
            throw std::runtime_error(summary_.reason);
        }
        summary_.hold_duration_s = static_cast<double>(hold_deadline_ms - ramp_start_ms) / 1000.0;
    }

    void log_sample(const char* stage, const Sample& sample) {
        std::ostringstream oss;
        oss << "[SAMPLE] stage=" << stage
            << " t=" << std::fixed << std::setprecision(3) << sample.t_s
            << " v=" << std::setprecision(1) << sample.voltage_v
            << " i=" << std::setprecision(2) << sample.current_a
            << " off=" << (sample.module_off ? 1 : 0);
        log_.log(oss.str());
    }

    void write_summary() {
        ensure_parent_dir(cfg_.summary_file);
        std::ofstream fp(cfg_.summary_file, std::ios::out | std::ios::trunc);
        if (!fp) {
            return;
        }
        fp << "{\n";
        fp << "  \"result\": \"" << summary_.result << "\",\n";
        fp << "  \"reason\": \"" << escape_json(summary_.reason) << "\",\n";
        fp << "  \"validated\": " << (summary_.validated ? "true" : "false") << ",\n";
        fp << "  \"allocated\": " << (summary_.allocated ? "true" : "false") << ",\n";
        fp << "  \"sample_count\": " << summary_.sample_count << ",\n";
        fp << "  \"peak_voltage_v\": " << std::fixed << std::setprecision(3) << summary_.peak_voltage_v << ",\n";
        fp << "  \"peak_current_a\": " << std::fixed << std::setprecision(3) << summary_.peak_current_a << ",\n";
        fp << "  \"reached_150v_s\": " << summary_.reached_150v_s << ",\n";
        fp << "  \"reached_250v_s\": " << summary_.reached_250v_s << ",\n";
        fp << "  \"reached_400v_s\": " << summary_.reached_400v_s << ",\n";
        fp << "  \"reached_490v_s\": " << summary_.reached_490v_s << ",\n";
        fp << "  \"reached_20a_s\": " << summary_.reached_20a_s << ",\n";
        fp << "  \"reached_30a_s\": " << summary_.reached_30a_s << ",\n";
        fp << "  \"reached_40a_s\": " << summary_.reached_40a_s << ",\n";
        fp << "  \"first_hold_ready_s\": " << summary_.first_hold_ready_s << ",\n";
        fp << "  \"hold_duration_s\": " << summary_.hold_duration_s << "\n";
        fp << "}\n";
    }

    static std::string escape_json(const std::string& in) {
        std::ostringstream oss;
        for (const char ch : in) {
            switch (ch) {
                case '\\':
                    oss << "\\\\";
                    break;
                case '"':
                    oss << "\\\"";
                    break;
                case '\n':
                    oss << "\\n";
                    break;
                default:
                    oss << ch;
                    break;
            }
        }
        return oss.str();
    }

    Config cfg_;
    Logger log_;
    SocketCanTransport can_;
    std::unique_ptr<ModuleManager> manager_;
    int64_t last_allowlist_refresh_ms_{std::numeric_limits<int64_t>::min()};
    Summary summary_;
};

}  // namespace

int main(int argc, char** argv) {
    try {
        Runner runner(parse_args(argc, argv));
        return runner.run();
    } catch (const std::exception& exc) {
        std::cerr << "[FATAL] " << exc.what() << '\n';
        return 2;
    }
}
