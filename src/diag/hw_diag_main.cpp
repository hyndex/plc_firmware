#include <Arduino.h>
#include <MFRC522.h>
#include <PCA95x5.h>
#include <SPI.h>
#include <Wire.h>

#include <array>
#include <algorithm>
#include <cstring>

namespace {

constexpr int kSerialBaud = 115200;
constexpr int kRelayI2cSda = 12;
constexpr int kRelayI2cScl = 11;
constexpr uint8_t kRelayI2cAddr = 0x20;

constexpr int kCanSpiSck = 48;
constexpr int kCanSpiMiso = 21;
constexpr int kCanSpiMosi = 47;
constexpr uint32_t kRfidSpiHz = 1000000UL;

constexpr uint32_t kInputPollMs = 20u;
constexpr uint32_t kInputDebounceMs = 50u;
constexpr uint32_t kStatusPeriodMs = 5000u;
constexpr uint32_t kIoRetryMs = 2000u;
constexpr uint32_t kRfidRetryMs = 5000u;
constexpr uint32_t kRfidKeepaliveMs = 1000u;
constexpr uint32_t kRfidCardReleaseMs = 350u;
constexpr size_t kRfidMaxUidBytes = 10u;

struct RelayDef {
    const char* name;
    PCA95x5::Port::Port port;
};

constexpr std::array<RelayDef, 3> kRelays{{
    {"Relay1", PCA95x5::Port::P00},
    {"Relay2", PCA95x5::Port::P01},
    {"Relay3", PCA95x5::Port::P02},
}};

struct InputDef {
    const char* name;
    PCA95x5::Port::Port port;
    bool active_low;
    const char* active_label;
};

constexpr std::array<InputDef, 7> kInputs{{
    {"SW1", PCA95x5::Port::P03, true, "pressed"},
    {"SW2", PCA95x5::Port::P04, true, "pressed"},
    {"SW3", PCA95x5::Port::P05, true, "pressed"},
    {"SW4", PCA95x5::Port::P06, true, "pressed"},
    {"SW5", PCA95x5::Port::P07, true, "pressed"},
    {"ESTOP", PCA95x5::Port::P10, true, "fault"},
    {"EARTH", PCA95x5::Port::P11, false, "fault"},
}};

struct RfidMap {
    const char* name;
    uint8_t ss_pin;
    uint8_t rst_pin;
};

constexpr std::array<RfidMap, 2> kRfidMaps{{
    {"spare_36_39", 36, 39},
    {"legacy_18_17", 18, 17},
}};

struct DebouncedInputState {
    bool initialized{false};
    bool raw_high{true};
    bool stable_high{true};
    uint32_t last_raw_change_ms{0};
};

PCA9555 g_expander;
MFRC522 g_rfid;

bool g_io_ready = false;
uint32_t g_next_io_retry_ms = 0;
std::array<bool, kRelays.size()> g_relay_closed{};
std::array<uint32_t, kRelays.size()> g_relay_pulse_deadline_ms{};
std::array<DebouncedInputState, kInputs.size()> g_inputs{};

bool g_rfid_ready = false;
int g_rfid_map_index = -1;
uint8_t g_rfid_version = 0;
uint32_t g_next_rfid_retry_ms = 0;
uint32_t g_next_rfid_keepalive_ms = 0;
bool g_card_latched = false;
std::array<uint8_t, kRfidMaxUidBytes> g_card_uid{};
size_t g_card_uid_len = 0;
uint32_t g_card_last_seen_ms = 0;

uint32_t g_next_input_poll_ms = 0;
uint32_t g_next_status_ms = 0;
String g_serial_line;

bool time_reached(uint32_t now_ms, uint32_t target_ms) {
    return static_cast<int32_t>(now_ms - target_ms) >= 0;
}

bool relay_level_from_closed(bool closed) {
    return closed;
}

bool input_is_active(const InputDef& def, bool raw_high) {
    return def.active_low ? !raw_high : raw_high;
}

void print_help() {
    Serial.println("[DIAG] commands:");
    Serial.println("[DIAG]   help");
    Serial.println("[DIAG]   status");
    Serial.println("[DIAG]   relay <1|2|3|all> <open|close>");
    Serial.println("[DIAG]   pulse <1|2|3|all> <ms>");
    Serial.println("[DIAG]   rfid auto");
    Serial.println("[DIAG]   rfid map <basic|type2_hlw>");
    Serial.println("[DIAG]   rfid probe");
}

void print_banner() {
    Serial.println();
    Serial.println("[DIAG] PLC hardware diagnostic");
    Serial.printf("[DIAG] I2C relay expander: SDA=%d SCL=%d addr=0x%02X\n",
                  kRelayI2cSda,
                  kRelayI2cScl,
                  static_cast<unsigned>(kRelayI2cAddr));
    Serial.printf("[DIAG] RFID SPI shared pins: SCK=%d MISO=%d MOSI=%d\n",
                  kCanSpiSck,
                  kCanSpiMiso,
                  kCanSpiMosi);
    for (const auto& map : kRfidMaps) {
        Serial.printf("[DIAG] RFID candidate map=%s ss=%u rst=%u\n",
                      map.name,
                      static_cast<unsigned>(map.ss_pin),
                      static_cast<unsigned>(map.rst_pin));
    }
}

bool probe_io_expander(bool log_result) {
    Wire.begin(kRelayI2cSda, kRelayI2cScl);
    Wire.setTimeOut(50);
    Wire.beginTransmission(kRelayI2cAddr);
    const uint8_t rc = Wire.endTransmission();
    if (rc != 0u) {
        if (log_result) {
            Serial.printf("[DIAG][IO] PCA9555 probe failed addr=0x%02X rc=%u\n",
                          static_cast<unsigned>(kRelayI2cAddr),
                          static_cast<unsigned>(rc));
        }
        g_io_ready = false;
        return false;
    }

    g_expander.attach(Wire, kRelayI2cAddr);
    (void)g_expander.direction(PCA95x5::Direction::IN_ALL);
    for (size_t i = 0; i < kRelays.size(); ++i) {
        (void)g_expander.direction(kRelays[i].port, PCA95x5::Direction::OUT);
        (void)g_expander.write(kRelays[i].port, PCA95x5::Level::L);
        g_relay_closed[i] = false;
        g_relay_pulse_deadline_ms[i] = 0;
    }
    for (const auto& input : kInputs) {
        (void)g_expander.direction(input.port, PCA95x5::Direction::IN);
        (void)g_expander.polarity(input.port, PCA95x5::Polarity::ORIGINAL);
    }

    g_io_ready = true;
    if (log_result) {
        Serial.println("[DIAG][IO] PCA9555 ready");
    }
    return true;
}

bool ensure_io_ready(uint32_t now_ms) {
    if (g_io_ready) {
        return true;
    }
    if (!time_reached(now_ms, g_next_io_retry_ms)) {
        return false;
    }
    g_next_io_retry_ms = now_ms + kIoRetryMs;
    return probe_io_expander(true);
}

bool read_port_level(PCA95x5::Port::Port port, bool* raw_high) {
    if (!g_io_ready || !raw_high) {
        return false;
    }
    const auto level = g_expander.read(port);
    if (level != PCA95x5::Level::H && level != PCA95x5::Level::L) {
        return false;
    }
    *raw_high = (level == PCA95x5::Level::H);
    return true;
}

bool set_relay_state(size_t index, bool closed, const char* reason) {
    if (index >= kRelays.size()) {
        return false;
    }
    if (!g_io_ready) {
        return false;
    }
    if (g_relay_closed[index] == closed) {
        return true;
    }
    const bool level = relay_level_from_closed(closed);
    const bool ok = g_expander.direction(kRelays[index].port, PCA95x5::Direction::OUT) &&
                    g_expander.write(kRelays[index].port, level ? PCA95x5::Level::H : PCA95x5::Level::L);
    if (ok) {
        g_relay_closed[index] = closed;
        Serial.printf("[DIAG][RELAY] %s -> %s (%s)\n",
                      kRelays[index].name,
                      closed ? "CLOSED" : "OPEN",
                      reason ? reason : "-");
    } else {
        Serial.printf("[DIAG][RELAY] %s write failed (%s)\n",
                      kRelays[index].name,
                      reason ? reason : "-");
    }
    return ok;
}

void print_status(const char* reason) {
    const uint32_t now_ms = millis();
    Serial.printf("[DIAG][STATUS] reason=%s uptime_ms=%lu io=%d rfid=%d",
                  reason ? reason : "-",
                  static_cast<unsigned long>(now_ms),
                  g_io_ready ? 1 : 0,
                  g_rfid_ready ? 1 : 0);
    if (g_rfid_ready && g_rfid_map_index >= 0 &&
        g_rfid_map_index < static_cast<int>(kRfidMaps.size())) {
        const auto& map = kRfidMaps[static_cast<size_t>(g_rfid_map_index)];
        Serial.printf(" map=%s version=0x%02X", map.name, static_cast<unsigned>(g_rfid_version));
    }
    Serial.println();

    if (g_io_ready) {
        for (size_t i = 0; i < kRelays.size(); ++i) {
            Serial.printf("[DIAG][STATUS] relay=%s closed=%d pulse_deadline_ms=%lu\n",
                          kRelays[i].name,
                          g_relay_closed[i] ? 1 : 0,
                          static_cast<unsigned long>(g_relay_pulse_deadline_ms[i]));
        }
        for (size_t i = 0; i < kInputs.size(); ++i) {
            const bool active = input_is_active(kInputs[i], g_inputs[i].stable_high);
            Serial.printf("[DIAG][STATUS] input=%s raw=%s stable=%s active=%d meaning=%s\n",
                          kInputs[i].name,
                          g_inputs[i].raw_high ? "HIGH" : "LOW",
                          g_inputs[i].stable_high ? "HIGH" : "LOW",
                          active ? 1 : 0,
                          active ? kInputs[i].active_label : "idle");
        }
    }
}

void initialize_input_snapshots(uint32_t now_ms) {
    for (size_t i = 0; i < kInputs.size(); ++i) {
        bool raw_high = true;
        if (!read_port_level(kInputs[i].port, &raw_high)) {
            continue;
        }
        g_inputs[i].initialized = true;
        g_inputs[i].raw_high = raw_high;
        g_inputs[i].stable_high = raw_high;
        g_inputs[i].last_raw_change_ms = now_ms;
        const bool active = input_is_active(kInputs[i], raw_high);
        Serial.printf("[DIAG][INPUT] %s boot=%s active=%d meaning=%s\n",
                      kInputs[i].name,
                      raw_high ? "HIGH" : "LOW",
                      active ? 1 : 0,
                      active ? kInputs[i].active_label : "idle");
    }
}

void poll_inputs(uint32_t now_ms) {
    if (!ensure_io_ready(now_ms)) {
        return;
    }
    if (!time_reached(now_ms, g_next_input_poll_ms)) {
        return;
    }
    g_next_input_poll_ms = now_ms + kInputPollMs;

    for (size_t i = 0; i < kInputs.size(); ++i) {
        bool raw_high = true;
        if (!read_port_level(kInputs[i].port, &raw_high)) {
            continue;
        }

        auto& state = g_inputs[i];
        if (!state.initialized) {
            state.initialized = true;
            state.raw_high = raw_high;
            state.stable_high = raw_high;
            state.last_raw_change_ms = now_ms;
            continue;
        }

        if (raw_high != state.raw_high) {
            state.raw_high = raw_high;
            state.last_raw_change_ms = now_ms;
        }

        if (raw_high != state.stable_high && time_reached(now_ms, state.last_raw_change_ms + kInputDebounceMs)) {
            state.stable_high = raw_high;
            const bool active = input_is_active(kInputs[i], state.stable_high);
            Serial.printf("[DIAG][INPUT] %s -> %s active=%d meaning=%s\n",
                          kInputs[i].name,
                          state.stable_high ? "HIGH" : "LOW",
                          active ? 1 : 0,
                          active ? kInputs[i].active_label : "idle");
        }
    }
}

void deselect_all_rfid_chip_selects() {
    for (const auto& map : kRfidMaps) {
        pinMode(map.ss_pin, OUTPUT);
        digitalWrite(map.ss_pin, HIGH);
    }
}

bool probe_rfid_map(size_t index, bool log_failure) {
    if (index >= kRfidMaps.size()) {
        return false;
    }

    const auto& map = kRfidMaps[index];
    deselect_all_rfid_chip_selects();
    pinMode(map.rst_pin, OUTPUT);
    digitalWrite(map.rst_pin, LOW);
    delay(2);
    digitalWrite(map.rst_pin, HIGH);

    SPI.begin(kCanSpiSck, kCanSpiMiso, kCanSpiMosi, map.ss_pin);
    SPI.setFrequency(kRfidSpiHz);
    SPI.setDataMode(SPI_MODE0);

    g_rfid.PCD_Init(map.ss_pin, map.rst_pin);
    delay(10);
    const uint8_t version = g_rfid.PCD_ReadRegister(MFRC522::VersionReg);
    if (version == 0x00u || version == 0xFFu) {
        if (log_failure) {
            Serial.printf("[DIAG][RFID] missing map=%s ss=%u rst=%u version=0x%02X\n",
                          map.name,
                          static_cast<unsigned>(map.ss_pin),
                          static_cast<unsigned>(map.rst_pin),
                          static_cast<unsigned>(version));
        }
        return false;
    }

    g_rfid.PCD_SetAntennaGain(MFRC522::RxGain_max);
    g_rfid.PCD_AntennaOn();
    g_rfid_ready = true;
    g_rfid_map_index = static_cast<int>(index);
    g_rfid_version = version;
    g_next_rfid_keepalive_ms = millis() + kRfidKeepaliveMs;
    g_card_latched = false;
    g_card_uid.fill(0u);
    g_card_uid_len = 0u;
    Serial.printf("[DIAG][RFID] ready map=%s ss=%u rst=%u version=0x%02X\n",
                  map.name,
                  static_cast<unsigned>(map.ss_pin),
                  static_cast<unsigned>(map.rst_pin),
                  static_cast<unsigned>(version));
    return true;
}

void probe_rfid_reader(uint32_t now_ms, bool force, int preferred_index, const char* reason) {
    if (!force && !time_reached(now_ms, g_next_rfid_retry_ms)) {
        return;
    }
    g_next_rfid_retry_ms = now_ms + kRfidRetryMs;
    g_rfid_ready = false;
    g_rfid_map_index = -1;
    g_rfid_version = 0;

    if (preferred_index >= 0 && preferred_index < static_cast<int>(kRfidMaps.size()) &&
        probe_rfid_map(static_cast<size_t>(preferred_index), true)) {
        Serial.printf("[DIAG][RFID] probe reason=%s preferred=1\n", reason ? reason : "-");
        return;
    }

    for (size_t i = 0; i < kRfidMaps.size(); ++i) {
        if (static_cast<int>(i) == preferred_index) {
            continue;
        }
        if (probe_rfid_map(i, true)) {
            Serial.printf("[DIAG][RFID] probe reason=%s preferred=0\n", reason ? reason : "-");
            return;
        }
    }

    Serial.printf("[DIAG][RFID] no reader detected reason=%s\n", reason ? reason : "-");
}

void publish_rfid_token(const uint8_t* uid, size_t uid_len) {
    char token[(kRfidMaxUidBytes * 2u) + 1u] = {};
    size_t out = 0;
    for (size_t i = 0; i < uid_len && out + 2u < sizeof(token); ++i) {
        out += static_cast<size_t>(snprintf(token + out, sizeof(token) - out, "%02X", uid[i]));
    }
    const char* map_name = (g_rfid_ready && g_rfid_map_index >= 0 &&
                            g_rfid_map_index < static_cast<int>(kRfidMaps.size()))
                               ? kRfidMaps[static_cast<size_t>(g_rfid_map_index)].name
                               : "?";
    Serial.printf("[DIAG][RFID] tap map=%s token=%s len=%u\n",
                  map_name,
                  token,
                  static_cast<unsigned>(uid_len));
}

void poll_rfid(uint32_t now_ms) {
    if (!g_rfid_ready) {
        probe_rfid_reader(now_ms, false, -1, "retry");
        return;
    }

    if (time_reached(now_ms, g_next_rfid_keepalive_ms)) {
        g_next_rfid_keepalive_ms = now_ms + kRfidKeepaliveMs;
        const uint8_t version = g_rfid.PCD_ReadRegister(MFRC522::VersionReg);
        if (version == 0x00u || version == 0xFFu) {
            const char* map_name = (g_rfid_map_index >= 0 && g_rfid_map_index < static_cast<int>(kRfidMaps.size()))
                                       ? kRfidMaps[static_cast<size_t>(g_rfid_map_index)].name
                                       : "?";
            Serial.printf("[DIAG][RFID] comm lost map=%s version=0x%02X\n",
                          map_name,
                          static_cast<unsigned>(version));
            g_rfid_ready = false;
            g_rfid_map_index = -1;
            g_rfid_version = 0;
            g_card_latched = false;
            g_card_uid.fill(0u);
            g_card_uid_len = 0u;
            return;
        }
        g_rfid_version = version;
    }

    bool present = g_rfid.PICC_IsNewCardPresent();
    if (!present) {
        byte atqa[2] = {0u, 0u};
        byte atqa_len = sizeof(atqa);
        present = g_rfid.PICC_WakeupA(atqa, &atqa_len) == MFRC522::STATUS_OK;
    }

    if (!present) {
        if (g_card_latched && time_reached(now_ms, g_card_last_seen_ms + kRfidCardReleaseMs)) {
            g_card_latched = false;
            g_card_uid.fill(0u);
            g_card_uid_len = 0u;
            Serial.println("[DIAG][RFID] card released");
        }
        return;
    }

    g_card_last_seen_ms = now_ms;
    if (!g_rfid.PICC_ReadCardSerial()) {
        return;
    }

    const size_t uid_len = std::min<size_t>(g_rfid.uid.size, kRfidMaxUidBytes);
    std::array<uint8_t, kRfidMaxUidBytes> uid{};
    memcpy(uid.data(), g_rfid.uid.uidByte, uid_len);
    g_rfid.PICC_HaltA();
    g_rfid.PCD_StopCrypto1();

    const bool same_uid = g_card_latched && uid_len == g_card_uid_len &&
                          memcmp(uid.data(), g_card_uid.data(), uid_len) == 0;
    if (same_uid) {
        return;
    }

    g_card_latched = true;
    g_card_uid = uid;
    g_card_uid_len = uid_len;
    publish_rfid_token(uid.data(), uid_len);
}

void service_relay_pulses(uint32_t now_ms) {
    if (!g_io_ready) {
        return;
    }
    for (size_t i = 0; i < g_relay_pulse_deadline_ms.size(); ++i) {
        if (g_relay_pulse_deadline_ms[i] != 0u && time_reached(now_ms, g_relay_pulse_deadline_ms[i])) {
            g_relay_pulse_deadline_ms[i] = 0u;
            (void)set_relay_state(i, false, "pulse_done");
        }
    }
}

void apply_to_relay_target(const String& target, const std::function<void(size_t)>& fn) {
    if (target == "all") {
        for (size_t i = 0; i < kRelays.size(); ++i) {
            fn(i);
        }
        return;
    }

    if (target.length() == 1 && target[0] >= '1' && target[0] <= '3') {
        fn(static_cast<size_t>(target[0] - '1'));
        return;
    }

    Serial.printf("[DIAG] invalid relay target: %s\n", target.c_str());
}

void handle_command(String line) {
    line.trim();
    if (line.isEmpty()) {
        return;
    }

    if (line == "help") {
        print_help();
        return;
    }
    if (line == "status") {
        print_status("command");
        return;
    }
    if (line == "rfid probe") {
        probe_rfid_reader(millis(), true, -1, "command");
        return;
    }
    if (line == "rfid auto") {
        probe_rfid_reader(millis(), true, -1, "auto");
        return;
    }
    if (line.startsWith("rfid map ")) {
        const String name = line.substring(strlen("rfid map "));
        int preferred = -1;
        for (size_t i = 0; i < kRfidMaps.size(); ++i) {
            if (name.equalsIgnoreCase(kRfidMaps[i].name)) {
                preferred = static_cast<int>(i);
                break;
            }
        }
        if (preferred < 0) {
            Serial.printf("[DIAG] unknown RFID map: %s\n", name.c_str());
            return;
        }
        probe_rfid_reader(millis(), true, preferred, "map_select");
        return;
    }

    if (line.startsWith("relay ")) {
        const int first_space = line.indexOf(' ');
        const int second_space = line.indexOf(' ', first_space + 1);
        if (first_space < 0 || second_space < 0) {
            Serial.println("[DIAG] usage: relay <1|2|3|all> <open|close>");
            return;
        }
        const String target = line.substring(first_space + 1, second_space);
        const String action = line.substring(second_space + 1);
        if (!g_io_ready) {
            Serial.println("[DIAG] relay expander not ready");
            return;
        }
        apply_to_relay_target(target, [&](size_t index) {
            if (action == "open") {
                g_relay_pulse_deadline_ms[index] = 0u;
                (void)set_relay_state(index, false, "command");
            } else if (action == "close") {
                g_relay_pulse_deadline_ms[index] = 0u;
                (void)set_relay_state(index, true, "command");
            } else {
                Serial.printf("[DIAG] invalid relay action: %s\n", action.c_str());
            }
        });
        return;
    }

    if (line.startsWith("pulse ")) {
        const int first_space = line.indexOf(' ');
        const int second_space = line.indexOf(' ', first_space + 1);
        if (first_space < 0 || second_space < 0) {
            Serial.println("[DIAG] usage: pulse <1|2|3|all> <ms>");
            return;
        }
        const String target = line.substring(first_space + 1, second_space);
        const uint32_t pulse_ms = static_cast<uint32_t>(line.substring(second_space + 1).toInt());
        if (pulse_ms == 0u) {
            Serial.println("[DIAG] invalid pulse duration");
            return;
        }
        if (!g_io_ready) {
            Serial.println("[DIAG] relay expander not ready");
            return;
        }
        apply_to_relay_target(target, [&](size_t index) {
            if (set_relay_state(index, true, "pulse")) {
                g_relay_pulse_deadline_ms[index] = millis() + pulse_ms;
                Serial.printf("[DIAG][RELAY] %s pulse_ms=%lu\n",
                              kRelays[index].name,
                              static_cast<unsigned long>(pulse_ms));
            }
        });
        return;
    }

    Serial.printf("[DIAG] unknown command: %s\n", line.c_str());
    print_help();
}

void poll_serial() {
    while (Serial.available() > 0) {
        const char ch = static_cast<char>(Serial.read());
        if (ch == '\r') {
            continue;
        }
        if (ch == '\n') {
            handle_command(g_serial_line);
            g_serial_line = "";
            continue;
        }
        if (g_serial_line.length() < 160u) {
            g_serial_line += ch;
        }
    }
}

} // namespace

void setup() {
    Serial.begin(kSerialBaud);
    delay(250);
    print_banner();
    print_help();

    const uint32_t now_ms = millis();
    if (probe_io_expander(true)) {
        initialize_input_snapshots(now_ms);
    } else {
        g_next_io_retry_ms = now_ms + kIoRetryMs;
    }

    probe_rfid_reader(now_ms, true, -1, "boot");
    g_next_input_poll_ms = now_ms;
    g_next_status_ms = now_ms + kStatusPeriodMs;
    print_status("boot");
}

void loop() {
    const uint32_t now_ms = millis();
    poll_serial();
    poll_inputs(now_ms);
    service_relay_pulses(now_ms);
    poll_rfid(now_ms);

    if (time_reached(now_ms, g_next_status_ms)) {
        g_next_status_ms = now_ms + kStatusPeriodMs;
        print_status("periodic");
    }

    delay(10);
}
