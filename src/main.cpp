#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>
#include <esp_system.h>
#include <Preferences.h>
#include <WebServer.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <WiFi.h>

#include <array>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <cbmodules/module_manager.hpp>
#include <PCA95x5.h>
#include <slac/channel.hpp>
#include <slac/evse_fsm.hpp>
#include <slac/platform.hpp>
#include <slac/slac.hpp>
#include <lwip/etharp.h>
#include <lwip/ethip6.h>
#include <lwip/netif.h>
#include <lwip/pbuf.h>
#include <lwip/tcpip.h>
#include <netif/ethernet.h>

extern "C" {
#include "cbv2g/din/din_msgDefDatatypes.h"
#include "cbv2g/iso_2/iso2_msgDefDatatypes.h"
#include "jpv2g/config.h"
#include "jpv2g/codec.h"
#include "jpv2g/cbv2g_codec.h"
#include "jpv2g/constants.h"
#include "jpv2g/platform_compat.h"
#include "jpv2g/sdp.h"
#include "jpv2g/secc.h"
#include "jpv2g/transport.h"
#include "jpv2g/v2gtp.h"
#include "mcp2515.h"
}

namespace {

SemaphoreHandle_t g_serial_mutex = nullptr;
portMUX_TYPE g_serial_init_mux = portMUX_INITIALIZER_UNLOCKED;

void ensure_serial_mutex() {
    if (g_serial_mutex) {
        return;
    }
    portENTER_CRITICAL(&g_serial_init_mux);
    if (!g_serial_mutex) {
        g_serial_mutex = xSemaphoreCreateMutex();
    }
    portEXIT_CRITICAL(&g_serial_init_mux);
}

class LockedSerialProxy {
public:
    void begin(unsigned long baud) {
        ::Serial.begin(baud);
        ensure_serial_mutex();
    }

    int available() {
        return ::Serial.available();
    }

    int read() {
        return ::Serial.read();
    }

    template <typename... Args>
    size_t printf(const char* format, Args... args) {
        Lock guard;
        return ::Serial.printf(format, args...);
    }

    size_t println() {
        Lock guard;
        return ::Serial.println();
    }

    template <typename T>
    size_t println(const T& value) {
        Lock guard;
        return ::Serial.println(value);
    }

private:
    class Lock {
    public:
        Lock() {
            ensure_serial_mutex();
            if (g_serial_mutex && xTaskGetSchedulerState() != taskSCHEDULER_NOT_STARTED) {
                taken_ = xSemaphoreTake(g_serial_mutex, pdMS_TO_TICKS(100)) == pdTRUE;
            }
        }

        ~Lock() {
            if (taken_ && g_serial_mutex) {
                xSemaphoreGive(g_serial_mutex);
            }
        }

    private:
        bool taken_{false};
    };
};

LockedSerialProxy g_locked_serial;

#define Serial g_locked_serial

// Hardware profile aligned to Basic/src/defs.h
constexpr int PIN_QCA700X_INT = 2;
constexpr int PIN_QCA700X_CS = 10;
constexpr int PIN_QCA700X_RESET = 8;
constexpr int QCA_SPI_SCK = 7;
constexpr int QCA_SPI_MISO = 16;
constexpr int QCA_SPI_MOSI = 15;
constexpr uint32_t QCA_SPI_HZ = 8000000UL;

constexpr int CP_1_PWM_PIN = 38;
constexpr int CP_1_PWM_CHANNEL = 0;
constexpr int CP_1_PWM_FREQUENCY = 1000;
constexpr int CP_1_PWM_RESOLUTION = 12;
constexpr uint32_t CP_1_MAX_DUTY_CYCLE = (1u << CP_1_PWM_RESOLUTION) - 1u;
constexpr int CP_1_READ_PIN = 1;

constexpr int CP_T12_MV = 2300;
constexpr int CP_T9_MV = 2000;
constexpr int CP_T6_MV = 1700;
constexpr int CP_T3_MV = 1450;
constexpr int CP_T0_MV = 1250;
constexpr int CP_NEG_THRESHOLD_MV = 500;
constexpr int CP_HYSTERESIS_MV = 100;
constexpr int CP_SAMPLE_COUNT = 80;
constexpr int CP_SAMPLE_DELAY_US = 6;
constexpr int CP_TOPK = 12;

constexpr uint32_t CP_STABLE_MS = 300;
constexpr uint32_t CP_STATE_DEBOUNCE_MS = 80;
constexpr uint32_t CP_DISCONNECT_DEBOUNCE_MS = 1200;
constexpr uint32_t CP_CONNECTED_HOLD_MS = 1500;
constexpr uint32_t SLAC_HOLD_MS = 3000;
constexpr uint32_t CP_EF_PULSE_MS = 4000;
constexpr uint8_t EF_PULSE_AFTER_FAILURES = 2;
constexpr uint32_t STARTUP_LOG_DELAY_MS = 10000;
constexpr uint32_t QCA_INIT_RETRY_MS = 1000;
constexpr int HLC_FIRST_PACKET_TIMEOUT_MS = 20000;
constexpr int HLC_IDLE_TIMEOUT_MS = 60000;
constexpr uint32_t HLC_TASK_STACK_WORDS = 65536;
constexpr UBaseType_t HLC_CLIENT_QUEUE_DEPTH = 2;
constexpr UBaseType_t LWIP_TX_QUEUE_DEPTH = 12;
constexpr bool ENABLE_DECODED_LOGS = true;

constexpr uint16_t QCA7K_SPI_READ = (1u << 15);
constexpr uint16_t QCA7K_SPI_WRITE = (0u << 15);
constexpr uint16_t QCA7K_SPI_INTERNAL = (1u << 14);
constexpr uint16_t QCA7K_SPI_EXTERNAL = (0u << 14);

constexpr uint16_t SPI_REG_BFR_SIZE = 0x0100;
constexpr uint16_t SPI_REG_WRBUF_SPC_AVA = 0x0200;
constexpr uint16_t SPI_REG_RDBUF_BYTE_AVA = 0x0300;
constexpr uint16_t SPI_REG_SPI_CONFIG = 0x0400;
constexpr uint16_t SPI_REG_SIGNATURE = 0x1A00;

constexpr uint16_t SPI_INT_CPU_ON = (1u << 6);
constexpr uint16_t QCASPI_GOOD_SIGNATURE = 0xAA55;

constexpr uint16_t QCA7K_BUFFER_SIZE = 3163;
constexpr size_t RX_STREAM_CAPACITY = 4096;
constexpr size_t RX_CHUNK_CAPACITY = 4096;
constexpr size_t RX_QUEUE_CAPACITY = 8;

constexpr uint16_t PLC_PEER_MAC_DEFAULT[3] = {0x00B0, 0x5200, 0x0001};
constexpr float PRECHARGE_CURRENT_LIMIT_A = 2.0f;
constexpr float MODULE_MAX_VOLTAGE_V = 1000.0f;
constexpr float MODULE_MAX_CURRENT_A = 100.0f;
constexpr float MODULE_MAX_POWER_KW = 30.0f;
constexpr uint32_t MODULE_ALLOWLIST_LEASE_MS = 1200;
constexpr uint32_t MODULE_TELEMETRY_FRESH_MS = 10000;
constexpr uint32_t MODULE_RUNTIME_LOG_INTERVAL_MS = 1000;
constexpr uint32_t MODULE_SERVICE_INTERVAL_MS = 10;
constexpr uint32_t MODULE_SERVICE_SLAC_CRITICAL_MS = 50;

constexpr int RELAY_I2C_SDA = 12;
constexpr int RELAY_I2C_SCL = 11;
constexpr uint8_t RELAY_I2C_ADDR = 0x20;
constexpr int RELAY1_EXP_PIN = 0;
constexpr int RELAY2_EXP_PIN = 1;
constexpr int RELAY3_EXP_PIN = 2;
constexpr int SW4_EXP_PIN = 6;
constexpr bool RELAY_ACTIVE_HIGH = true;

constexpr int CAN_CS_PIN = 41;
constexpr int CAN_INT_PIN = 40;
constexpr int CAN_SPI_SCK = 48;
constexpr int CAN_SPI_MISO = 21;
constexpr int CAN_SPI_MOSI = 47;
constexpr uint32_t CAN_SPI_HZ = 8000000UL;
constexpr uint32_t CAN_BITRATE_KBPS = 125;

constexpr uint32_t CFG_MAGIC = 0x4342504Cu; // "CBPL"
constexpr uint16_t CFG_VERSION = 2u;

#ifndef CBPLC_DEFAULT_STANDALONE_MODE
#define CBPLC_DEFAULT_STANDALONE_MODE 1
#endif

#ifndef CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START
#define CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START 1
#endif

#ifndef CBPLC_DEFAULT_PLC_ID
#define CBPLC_DEFAULT_PLC_ID 1
#endif

#ifndef CBPLC_DEFAULT_CONNECTOR_ID
#define CBPLC_DEFAULT_CONNECTOR_ID 1
#endif

#ifndef CBPLC_DEFAULT_CONTROLLER_ID
#define CBPLC_DEFAULT_CONTROLLER_ID 1
#endif

#ifndef CBPLC_DEFAULT_LOCAL_MODULE_ADDRESS
#define CBPLC_DEFAULT_LOCAL_MODULE_ADDRESS 1
#endif

constexpr bool DEFAULT_STANDALONE_MODE = (CBPLC_DEFAULT_STANDALONE_MODE != 0);
constexpr bool DEFAULT_SLAC_REQUIRES_CONTROLLER_START = (CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START != 0);
constexpr uint8_t DEFAULT_PLC_ID = static_cast<uint8_t>(CBPLC_DEFAULT_PLC_ID);
constexpr uint8_t DEFAULT_CONNECTOR_ID = static_cast<uint8_t>(CBPLC_DEFAULT_CONNECTOR_ID);
constexpr uint8_t DEFAULT_CONTROLLER_ID = static_cast<uint8_t>(CBPLC_DEFAULT_CONTROLLER_ID);
constexpr uint8_t DEFAULT_LOCAL_MODULE_ADDRESS = static_cast<uint8_t>(CBPLC_DEFAULT_LOCAL_MODULE_ADDRESS);
constexpr uint32_t DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS = 1200u;
constexpr uint32_t DEFAULT_CONTROLLER_AUTH_TTL_MS = 2500u;
constexpr uint32_t DEFAULT_CONTROLLER_ALLOC_TTL_MS = 3000u;
constexpr uint32_t DEFAULT_SLAC_ARM_TIMEOUT_MS = 10000u;
constexpr uint32_t CONTROLLER_HB_SOFT_STOP_GRACE_MS = 3000u;
constexpr uint32_t CONTROLLER_HB_HARD_STOP_GRACE_MS = 15000u;
constexpr uint8_t MAX_RUNTIME_PLC_ID = 15u;
constexpr uint8_t MAX_RUNTIME_CONTROLLER_ID = 15u;
constexpr uint8_t MAX_MODULE_GROUP_ID = 7u;
constexpr uint8_t DEFAULT_LOCAL_MODULE_CAN_GROUP = 1u;
constexpr uint16_t MAXWELL_CAN_PROTOCOL_ID = 0x060u;
constexpr uint32_t MODULE_OWNERSHIP_CAN_ID_BASE = 0x18FF0000u;
constexpr uint32_t MODULE_OWNERSHIP_EPOCH_CAN_ID_BASE = 0x18FE0000u;
constexpr uint32_t MODULE_OWNERSHIP_CAN_ID_MASK = 0x1FFF0000u;

constexpr uint32_t CTRL_MSG_VERSION = 1u;

constexpr uint32_t CAN_ID_CTRL_HEARTBEAT = 0x18FF5000u;
constexpr uint32_t CAN_ID_CTRL_AUTH = 0x18FF5010u;
constexpr uint32_t CAN_ID_CTRL_SLAC = 0x18FF5020u;
constexpr uint32_t CAN_ID_CTRL_ALLOC_BEGIN = 0x18FF5030u;
constexpr uint32_t CAN_ID_CTRL_ALLOC_DATA = 0x18FF5040u;
constexpr uint32_t CAN_ID_CTRL_ALLOC_COMMIT = 0x18FF5050u;
constexpr uint32_t CAN_ID_CTRL_ALLOC_ABORT = 0x18FF5060u;
constexpr uint32_t CAN_ID_CTRL_AUX_RELAY = 0x18FF5070u;
constexpr uint32_t CAN_ID_CTRL_SESSION = 0x18FF5080u;

constexpr uint32_t CAN_ID_PLC_HEARTBEAT = 0x18FF6000u;
constexpr uint32_t CAN_ID_PLC_CP_STATUS = 0x18FF6010u;
constexpr uint32_t CAN_ID_PLC_SLAC_STATUS = 0x18FF6020u;
constexpr uint32_t CAN_ID_PLC_HLC_STATUS = 0x18FF6030u;
constexpr uint32_t CAN_ID_PLC_POWER_STATUS = 0x18FF6040u;
constexpr uint32_t CAN_ID_PLC_SESSION_STATUS = 0x18FF6050u;
constexpr uint32_t CAN_ID_PLC_IDENTITY_EVT = 0x18FF6060u;
constexpr uint32_t CAN_ID_PLC_CMD_ACK = 0x18FF6070u;
constexpr uint32_t CAN_ID_PLC_BMS_STATUS = 0x18FF6080u;

constexpr uint32_t PLC_TX_HEARTBEAT_MS = 500u;
constexpr uint32_t PLC_TX_CP_STATUS_MS = 200u;
constexpr uint32_t PLC_TX_SLAC_STATUS_MS = 500u;
constexpr uint32_t PLC_TX_HLC_STATUS_MS = 500u;
constexpr uint32_t PLC_TX_POWER_STATUS_MS = 200u;
constexpr uint32_t PLC_TX_SESSION_STATUS_MS = 500u;
constexpr uint32_t PLC_TX_BMS_STATUS_MS = 250u;

constexpr uint32_t CTRL_RX_QUEUE_CAPACITY = 128u;
constexpr size_t MAX_STAGED_ALLOC_MODULES = 32u;
constexpr uint8_t CTRL_AUTH_DENY = 0u;
constexpr uint8_t CTRL_AUTH_PENDING = 1u;
constexpr uint8_t CTRL_AUTH_GRANTED = 2u;

constexpr uint8_t CTRL_SLAC_DISARM = 0u;
constexpr uint8_t CTRL_SLAC_ARM = 1u;
constexpr uint8_t CTRL_SLAC_START_NOW = 2u;
constexpr uint8_t CTRL_SLAC_ABORT = 3u;

constexpr uint8_t CTRL_SESSION_NONE = 0u;
constexpr uint8_t CTRL_SESSION_STOP_SOFT = 1u;
constexpr uint8_t CTRL_SESSION_STOP_HARD = 2u;
constexpr uint8_t CTRL_SESSION_CLEAR = 3u;

constexpr uint8_t CP_PHASE_UNKNOWN = 0u;
constexpr uint8_t CP_PHASE_A = 1u;
constexpr uint8_t CP_PHASE_B = 2u;
constexpr uint8_t CP_PHASE_B1 = 3u;
constexpr uint8_t CP_PHASE_B2 = 4u;
constexpr uint8_t CP_PHASE_C = 5u;
constexpr uint8_t CP_PHASE_D = 6u;
constexpr uint8_t CP_PHASE_E = 7u;
constexpr uint8_t CP_PHASE_F = 8u;

constexpr uint8_t ACK_OK = 0u;
constexpr uint8_t ACK_BAD_CRC = 1u;
constexpr uint8_t ACK_BAD_VERSION = 2u;
constexpr uint8_t ACK_BAD_TARGET = 3u;
constexpr uint8_t ACK_BAD_SEQ = 4u;
constexpr uint8_t ACK_BAD_STATE = 5u;
constexpr uint8_t ACK_BAD_VALUE = 6u;

SPIClass g_can_spi(FSPI);
PCA9555 g_relay_expander;

extern SemaphoreHandle_t g_can_mutex;
void tap_controller_rx_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8], uint32_t now_ms);
uint8_t crc8_07(const uint8_t* data, size_t len);
uint32_t can_with_plc_id(uint32_t base_id);
bool is_controller_rx_base(uint32_t base_id);
bool is_local_module_can_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8]);
bool apply_new_allowlist(const std::vector<std::string>& next, const char* reason);
void stop_hlc_stack();
void stop_session(uint32_t now_ms);

class Mcp2515Transport : public cbmodules::CanTransport {
public:
    bool begin() {
        if (!g_can_mutex) {
            g_can_mutex = xSemaphoreCreateMutex();
            if (!g_can_mutex) {
                return false;
            }
        }
        pinMode(CAN_CS_PIN, OUTPUT);
        digitalWrite(CAN_CS_PIN, HIGH);
        pinMode(CAN_INT_PIN, INPUT_PULLUP);
        g_can_spi.begin(CAN_SPI_SCK, CAN_SPI_MISO, CAN_SPI_MOSI, CAN_CS_PIN);

        hal_ = {};
        hal_.ctx = this;
        hal_.cs_select = &Mcp2515Transport::hal_cs_select;
        hal_.cs_deselect = &Mcp2515Transport::hal_cs_deselect;
        hal_.spi_transfer = &Mcp2515Transport::hal_spi_transfer;
        hal_.delay_ms = &Mcp2515Transport::hal_delay_ms;
        hal_.millis = &Mcp2515Transport::hal_millis;

        const uint8_t osc_candidates[] = {8, 16};
        for (uint8_t osc : osc_candidates) {
            if (!configure(osc)) continue;
            active_osc_mhz_ = osc;
            inited_ = true;
            return true;
        }
        return false;
    }

    bool send(const cbmodules::CanFrame& frame) override {
        if (!inited_) return false;
        enr_can_frame_t out{};
        out.id = frame.id & 0x1FFFFFFFu;
        out.extended = frame.extended;
        out.dlc = static_cast<uint8_t>(std::min<uint8_t>(8, frame.dlc));
        for (uint8_t i = 0; i < out.dlc; ++i) out.data[i] = frame.data[i];
        if (!g_can_mutex) return false;
        if (xSemaphoreTake(g_can_mutex, pdMS_TO_TICKS(20)) != pdTRUE) return false;
        const bool ok = (mcp2515_send(&dev_, &out, 20) == ENR_OK);
        xSemaphoreGive(g_can_mutex);
        return ok;
    }

    bool receive(cbmodules::CanFrame& frame_out) override {
        if (!inited_) return false;
        if (!g_can_mutex) return false;
        uint8_t drain_budget = 24u;
        while (drain_budget-- > 0u) {
            if (xSemaphoreTake(g_can_mutex, pdMS_TO_TICKS(20)) != pdTRUE) return false;
            if (!mcp2515_receive_available(&dev_)) {
                xSemaphoreGive(g_can_mutex);
                return false;
            }
            enr_can_frame_t in{};
            if (mcp2515_receive(&dev_, &in) != ENR_OK) {
                xSemaphoreGive(g_can_mutex);
                return false;
            }
            xSemaphoreGive(g_can_mutex);

            cbmodules::CanFrame candidate{};
            candidate.id = in.id & 0x1FFFFFFFu;
            candidate.extended = in.extended;
            candidate.dlc = static_cast<uint8_t>(std::min<uint8_t>(8, in.dlc));
            for (uint8_t i = 0; i < candidate.dlc; ++i) candidate.data[i] = in.data[i];

            if (in.extended) {
                const uint32_t base = in.id & 0x1FFFFFF0u;
                if (is_controller_rx_base(base)) {
                    tap_controller_rx_frame(in.id, true, candidate.dlc, candidate.data, millis());
                    continue;
                }
            }

            if (!is_local_module_can_frame(in.id, in.extended, candidate.dlc, candidate.data)) {
                continue;
            }

            frame_out = candidate;
            return true;
        }
        return false;
    }

    uint32_t now_ms() const override {
        return millis();
    }

private:
    static void hal_cs_select(void* ctx) {
        (void)ctx;
        g_can_spi.beginTransaction(SPISettings(CAN_SPI_HZ, MSBFIRST, SPI_MODE0));
        digitalWrite(CAN_CS_PIN, LOW);
    }

    static void hal_cs_deselect(void* ctx) {
        (void)ctx;
        digitalWrite(CAN_CS_PIN, HIGH);
        g_can_spi.endTransaction();
    }

    static uint8_t hal_spi_transfer(void* ctx, uint8_t data) {
        (void)ctx;
        return g_can_spi.transfer(data);
    }

    static void hal_delay_ms(void* ctx, uint16_t ms) {
        (void)ctx;
        delay(ms);
    }

    static uint32_t hal_millis(void* ctx) {
        (void)ctx;
        return millis();
    }

    bool configure(uint8_t osc_mhz) {
        dev_ = {};
        if (!g_can_mutex) return false;
        if (xSemaphoreTake(g_can_mutex, pdMS_TO_TICKS(100)) != pdTRUE) return false;
        if (mcp2515_init(&dev_, &hal_, osc_mhz) != ENR_OK) {
            xSemaphoreGive(g_can_mutex);
            return false;
        }
        dev_.spi_hz = CAN_SPI_HZ;
        if (mcp2515_begin(&dev_, true, false) != ENR_OK) {
            xSemaphoreGive(g_can_mutex);
            return false;
        }
        if (mcp2515_set_bitrate_kbps(&dev_, CAN_BITRATE_KBPS) != ENR_OK) {
            xSemaphoreGive(g_can_mutex);
            return false;
        }
        const uint32_t accept_all_mask = 0u;
        const uint32_t accept_all_filter = 0u;
        if (mcp2515_set_filters_ext(&dev_, accept_all_mask, &accept_all_filter, 1) != ENR_OK) {
            xSemaphoreGive(g_can_mutex);
            return false;
        }
        xSemaphoreGive(g_can_mutex);
        return true;
    }

    mcp2515_t dev_{};
    mcp2515_hal_t hal_{};
    bool inited_{false};
    uint8_t active_osc_mhz_{0};
};

void lwip_ingress_ethernet_frame(const uint8_t* frame, uint16_t len);

class Qca7000Transport final : public slac::ITransport {
public:
    Qca7000Transport() : spi(HSPI) {
        spi_mutex = xSemaphoreCreateMutex();
    }

    ~Qca7000Transport() override {
        if (spi_mutex) {
            vSemaphoreDelete(spi_mutex);
            spi_mutex = nullptr;
        }
    }

    bool begin() {
        if (ready) {
            return true;
        }

        pinMode(PIN_QCA700X_CS, OUTPUT);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        pinMode(PIN_QCA700X_RESET, OUTPUT);
        digitalWrite(PIN_QCA700X_RESET, HIGH);
        pinMode(PIN_QCA700X_INT, INPUT);
        pinMode(QCA_SPI_SCK, OUTPUT);
        pinMode(QCA_SPI_MISO, INPUT);
        pinMode(QCA_SPI_MOSI, OUTPUT);

        // Basic/reference behavior: explicit hardware reset pulse before probing signature.
        digitalWrite(PIN_QCA700X_RESET, LOW);
        delay(10);
        digitalWrite(PIN_QCA700X_RESET, HIGH);
        delay(10);

        spi.begin(QCA_SPI_SCK, QCA_SPI_MISO, QCA_SPI_MOSI, PIN_QCA700X_CS);
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        spi.endTransaction();

        uint16_t sig = 0;
        if (!probe_signature(sig, 2000, 10)) {
            char buf[80];
            snprintf(buf, sizeof(buf), "QCA7000 signature mismatch (last=0x%04X)", sig);
            last_error = buf;
            return false;
        }

        ready = true;
        last_error.clear();
        return true;
    }

    void modem_reset() {
        uint16_t reg = read_register16(SPI_REG_SPI_CONFIG);
        reg = static_cast<uint16_t>(reg | SPI_INT_CPU_ON);
        write_register16(SPI_REG_SPI_CONFIG, reg);
    }

    bool is_ready() const {
        return ready;
    }

    void service_ingress_once() {
        if (!ready) {
            return;
        }
        poll_ingress();
    }

    IOResult read(uint8_t* buffer, int timeout_ms) override {
        if (!ready) {
            last_error = "transport not ready";
            return IOResult::Failure;
        }

        const uint32_t start_ms = millis();
        while (true) {
            RxFrame frame{};
            if (pop_frame(frame)) {
                memset(buffer, 0, ETH_FRAME_LEN);
                memcpy(buffer, frame.data.data(), frame.len);
                return IOResult::Ok;
            }

            poll_ingress();

            if (timeout_ms >= 0 && (uint32_t)(millis() - start_ms) >= static_cast<uint32_t>(timeout_ms)) {
                return IOResult::Timeout;
            }
            delay(1);
        }
    }

    IOResult write(const void* buffer, size_t size, int timeout_ms) override {
        if (!ready) {
            last_error = "transport not ready";
            return IOResult::Failure;
        }
        if (size == 0 || size > ETH_FRAME_LEN) {
            last_error = "invalid frame size";
            return IOResult::Failure;
        }

        const uint16_t total_len = static_cast<uint16_t>(size + 10u);
        const uint32_t start_ms = millis();
        while (true) {
            uint16_t wrbuf_space = read_register16(SPI_REG_WRBUF_SPC_AVA);
            if (wrbuf_space >= total_len) {
                break;
            }

            if (timeout_ms >= 0 && (uint32_t)(millis() - start_ms) >= static_cast<uint32_t>(timeout_ms)) {
                return IOResult::Timeout;
            }
            delay(1);
        }

        uint8_t hdr[8] = {
            0xAA, 0xAA, 0xAA, 0xAA,
            static_cast<uint8_t>(size & 0xFFu),
            static_cast<uint8_t>((size >> 8u) & 0xFFu),
            0x00, 0x00
        };

        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return IOResult::Failure;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_BFR_SIZE));
        spi.transfer16(total_len);
        digitalWrite(PIN_QCA700X_CS, HIGH);

        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_EXTERNAL));
        spi.transfer(hdr, sizeof(hdr));
        spi.transfer(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(buffer)), size);
        spi.transfer16(0x5555);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();

        return IOResult::Ok;
    }

    const std::string& get_error() const override {
        return last_error;
    }

private:
    struct RxFrame {
        std::array<uint8_t, ETH_FRAME_LEN> data{};
        uint16_t len{0};
    };

    bool probe_signature(uint16_t& last_sig, uint32_t timeout_ms, uint32_t poll_delay_ms) {
        const uint32_t start_ms = millis();
        bool primed = false;
        last_sig = 0;

        while ((millis() - start_ms) < timeout_ms) {
            if (!primed) {
                (void)read_register16(SPI_REG_SIGNATURE);
                primed = true;
                delay(poll_delay_ms);
                continue;
            }

            last_sig = read_register16(SPI_REG_SIGNATURE);
            if (last_sig == QCASPI_GOOD_SIGNATURE) {
                return true;
            }

            primed = false;
            delay(poll_delay_ms);
        }

        return false;
    }

    uint16_t read_register16(uint16_t reg) {
        const uint16_t tx = static_cast<uint16_t>(QCA7K_SPI_READ | QCA7K_SPI_INTERNAL | reg);
        uint16_t rx = 0;
        if (!lock_spi(50)) {
            return 0;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(tx);
        rx = spi.transfer16(0x0000);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        return rx;
    }

    void write_register16(uint16_t reg, uint16_t value) {
        const uint16_t tx = static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | reg);
        if (!lock_spi(50)) {
            return;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(tx);
        spi.transfer16(value);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
    }

    uint16_t read_burst(uint8_t* dst) {
        uint16_t available = read_register16(SPI_REG_RDBUF_BYTE_AVA);
        if (available == 0 || available > QCA7K_BUFFER_SIZE || available > RX_CHUNK_CAPACITY) {
            return 0;
        }

        if (!lock_spi(50)) {
            return 0;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_BFR_SIZE));
        spi.transfer16(available);
        digitalWrite(PIN_QCA700X_CS, HIGH);

        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_READ | QCA7K_SPI_EXTERNAL));
        spi.transfer(dst, available);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();

        return available;
    }

    bool push_frame(const uint8_t* frame, uint16_t len) {
        if (q_count >= RX_QUEUE_CAPACITY || len > ETH_FRAME_LEN) {
            return false;
        }
        auto& slot = frame_queue[q_tail];
        memcpy(slot.data.data(), frame, len);
        slot.len = len;
        q_tail = (q_tail + 1u) % RX_QUEUE_CAPACITY;
        q_count++;
        return true;
    }

    bool pop_frame(RxFrame& out) {
        if (q_count == 0) {
            return false;
        }
        out = frame_queue[q_head];
        q_head = (q_head + 1u) % RX_QUEUE_CAPACITY;
        q_count--;
        return true;
    }

    void dispatch_eth_frame(const uint8_t* frame, uint16_t len) {
        if (!frame || len < 14u) {
            return;
        }
        const uint16_t eth_type = static_cast<uint16_t>((frame[12] << 8) | frame[13]);
        if (eth_type == slac::defs::ETH_P_HOMEPLUG_GREENPHY) {
            push_frame(frame, len);
            return;
        }
        lwip_ingress_ethernet_frame(frame, len);
    }

    void process_rx_stream(uint16_t chunk_len) {
        if (chunk_len == 0) {
            return;
        }

        if (rx_stream_len + chunk_len > RX_STREAM_CAPACITY) {
            rx_stream_len = 0;
        }
        if (chunk_len > RX_STREAM_CAPACITY) {
            return;
        }
        memcpy(rx_stream.data() + rx_stream_len, rx_chunk.data(), chunk_len);
        rx_stream_len += chunk_len;

        size_t pos = 0;
        while ((rx_stream_len - pos) >= 14u) {
            uint8_t* frame = rx_stream.data() + pos;
            const bool sof_ok = (frame[4] == 0xAA && frame[5] == 0xAA && frame[6] == 0xAA && frame[7] == 0xAA);
            if (!sof_ok) {
                pos++;
                continue;
            }

            const uint16_t fl = static_cast<uint16_t>(frame[8] | (frame[9] << 8));
            const size_t total = static_cast<size_t>(fl) + 14u;
            if (fl < 60u || fl > ETH_FRAME_LEN || total > RX_STREAM_CAPACITY) {
                pos++;
                continue;
            }

            const size_t avail = rx_stream_len - pos;
            if (avail < total) {
                break;
            }

            if (frame[total - 2u] == 0x55 && frame[total - 1u] == 0x55) {
                dispatch_eth_frame(frame + 12u, fl);
                pos += total;
                continue;
            }

            pos++;
        }

        if (pos > 0) {
            if (pos < rx_stream_len) {
                memmove(rx_stream.data(), rx_stream.data() + pos, rx_stream_len - pos);
            }
            rx_stream_len -= pos;
        }
    }

    void poll_ingress() {
        const uint16_t chunk_len = read_burst(rx_chunk.data());
        if (chunk_len == 0) {
            return;
        }
        process_rx_stream(chunk_len);
    }

    SPIClass spi;
    SemaphoreHandle_t spi_mutex{nullptr};
    bool ready{false};
    std::string last_error;

    std::array<uint8_t, RX_STREAM_CAPACITY> rx_stream{};
    std::array<uint8_t, RX_CHUNK_CAPACITY> rx_chunk{};
    size_t rx_stream_len{0};

    std::array<RxFrame, RX_QUEUE_CAPACITY> frame_queue{};
    size_t q_head{0};
    size_t q_tail{0};
    size_t q_count{0};

    bool lock_spi(uint32_t timeout_ms) {
        if (!spi_mutex) {
            return false;
        }
        return xSemaphoreTake(spi_mutex, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
    }

    void unlock_spi() {
        if (spi_mutex) {
            xSemaphoreGive(spi_mutex);
        }
    }
};

char cp_state_from_mv(int mv) {
    if (mv <= CP_NEG_THRESHOLD_MV) return 'F';
    if (mv >= (CP_T12_MV + CP_HYSTERESIS_MV)) return 'A';
    if (mv >= (CP_T9_MV + CP_HYSTERESIS_MV)) return 'B';
    if (mv >= (CP_T6_MV + CP_HYSTERESIS_MV)) return 'C';
    if (mv >= (CP_T3_MV + CP_HYSTERESIS_MV)) return 'D';
    if (mv >= (CP_T0_MV + CP_HYSTERESIS_MV)) return 'E';
    return 'F';
}

bool cp_connected(char s) {
    return s == 'B' || s == 'C' || s == 'D';
}

uint32_t cp_pct_to_duty(uint16_t pct) {
    if (pct == 0) return 0;
    if (pct >= 100) return CP_1_MAX_DUTY_CYCLE;
    return (CP_1_MAX_DUTY_CYCLE * static_cast<uint32_t>(pct)) / 100u;
}

uint8_t cp_phase_code(char state, uint16_t duty_pct) {
    switch (state) {
        case 'A':
            return CP_PHASE_A;
        case 'B':
            if (duty_pct >= 100u) return CP_PHASE_B1;
            if (duty_pct > 0u) return CP_PHASE_B2;
            return CP_PHASE_B;
        case 'C':
            return CP_PHASE_C;
        case 'D':
            return CP_PHASE_D;
        case 'E':
            return CP_PHASE_E;
        case 'F':
            return CP_PHASE_F;
        default:
            return CP_PHASE_UNKNOWN;
    }
}

const char* cp_phase_label(char state, uint16_t duty_pct) {
    switch (cp_phase_code(state, duty_pct)) {
        case CP_PHASE_A:
            return "A";
        case CP_PHASE_B:
            return "B";
        case CP_PHASE_B1:
            return "B1";
        case CP_PHASE_B2:
            return "B2";
        case CP_PHASE_C:
            return "C";
        case CP_PHASE_D:
            return "D";
        case CP_PHASE_E:
            return "E";
        case CP_PHASE_F:
            return "F";
        default:
            return "U";
    }
}

static uint32_t g_last_ledc_duty = 0xFFFFFFFFu;

void write_ledc_duty(uint32_t duty) {
    if (duty != g_last_ledc_duty) {
        ledcWrite(CP_1_PWM_CHANNEL, duty);
        g_last_ledc_duty = duty;
    }
}

const char* evse_state_name(slac::evse::State s) {
    switch (s) {
    case slac::evse::State::Reset:
        return "Reset";
    case slac::evse::State::Idle:
        return "Idle";
    case slac::evse::State::WaitForMatchingStart:
        return "WaitForMatchingStart";
    case slac::evse::State::Matching:
        return "Matching";
    case slac::evse::State::Sounding:
        return "Sounding";
    case slac::evse::State::DoAttenChar:
        return "DoAttenChar";
    case slac::evse::State::WaitForSlacMatch:
        return "WaitForSlacMatch";
    case slac::evse::State::Matched:
        return "Matched";
    case slac::evse::State::SignalError:
        return "SignalError";
    case slac::evse::State::NoSlacPerformed:
        return "NoSlacPerformed";
    case slac::evse::State::MatchingFailed:
        return "MatchingFailed";
    }
    return "Unknown";
}

static uint16_t g_cp_sample_phase_us = 0;
int read_cp_mv_robust() {
    int topk[CP_TOPK];
    int tk = 0;
    int peak = 0;

    auto insert_topk = [&](int v) {
        if (tk < CP_TOPK) {
            int i = tk++;
            while (i > 0 && topk[i - 1] > v) {
                topk[i] = topk[i - 1];
                --i;
            }
            topk[i] = v;
            return;
        }
        if (v <= topk[0]) {
            return;
        }
        topk[0] = v;
        int i = 0;
        while ((i + 1) < tk && topk[i] > topk[i + 1]) {
            const int t = topk[i];
            topk[i] = topk[i + 1];
            topk[i + 1] = t;
            ++i;
        }
    };

    if (g_cp_sample_phase_us) {
        delayMicroseconds(g_cp_sample_phase_us);
    }
    (void)analogRead(CP_1_READ_PIN);
    for (int i = 0; i < CP_SAMPLE_COUNT; ++i) {
        delayMicroseconds(CP_SAMPLE_DELAY_US);
        const int v = analogReadMilliVolts(CP_1_READ_PIN);
        if (v > peak) {
            peak = v;
        }
        insert_topk(v);
        if ((i & 31) == 31) {
            taskYIELD();
        }
    }

    g_cp_sample_phase_us = static_cast<uint16_t>((g_cp_sample_phase_us + 53U) % 1000U);
    if (tk <= 0) return peak;

    int start = tk - ((tk / 6 > 3) ? (tk / 6) : 3);
    int end = tk - (tk >= 6 ? 1 : 0);
    if (start < 0) start = 0;
    if (end <= start) {
        start = (tk > 3) ? (tk - 3) : 0;
        end = tk;
    }

    int64_t sum = 0;
    int n = 0;
    for (int i = start; i < end; ++i) {
        sum += topk[i];
        ++n;
    }
    if (n > 0) return static_cast<int>(sum / n);
    return topk[tk - 1];
}

std::shared_ptr<Qca7000Transport> g_transport;
slac::Channel g_channel;
std::unique_ptr<slac::evse::EvseFsm> g_fsm;
uint8_t g_local_mac[ETH_ALEN]{};
uint8_t g_last_seen_ev_mac[ETH_ALEN]{};
bool g_last_seen_ev_mac_valid = false;
int g_last_cp_mv = 0;

struct RuntimeConfig {
    uint32_t magic{CFG_MAGIC};
    uint16_t version{CFG_VERSION};
    bool standalone_mode{DEFAULT_STANDALONE_MODE};
    bool slac_requires_controller_start{DEFAULT_SLAC_REQUIRES_CONTROLLER_START};
    uint8_t plc_id{DEFAULT_PLC_ID};
    uint8_t connector_id{DEFAULT_CONNECTOR_ID};
    uint8_t controller_id{DEFAULT_CONTROLLER_ID};
    uint8_t local_module_address{DEFAULT_LOCAL_MODULE_ADDRESS};
    uint32_t controller_heartbeat_timeout_ms{DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS};
    uint32_t controller_auth_ttl_ms{DEFAULT_CONTROLLER_AUTH_TTL_MS};
    uint32_t controller_alloc_ttl_ms{DEFAULT_CONTROLLER_ALLOC_TTL_MS};
    uint32_t controller_slac_arm_timeout_ms{DEFAULT_SLAC_ARM_TIMEOUT_MS};
};

enum class ControllerAuthState : uint8_t {
    Unknown = 0xFFu,
    Denied = CTRL_AUTH_DENY,
    Pending = CTRL_AUTH_PENDING,
    Granted = CTRL_AUTH_GRANTED,
};

struct ControllerRuntimeState {
    uint32_t last_heartbeat_ms{0};
    uint32_t last_auth_update_ms{0};
    uint32_t auth_expiry_ms{0};
    uint32_t slac_arm_expiry_ms{0};
    uint32_t alloc_txn_expiry_ms{0};
    uint32_t hb_lost_since_ms{0};
    bool heartbeat_seen{false};
    bool slac_armed{false};
    bool slac_start_latched{false};
    ControllerAuthState auth_state{ControllerAuthState::Unknown};
    bool alloc_txn_active{false};
    uint8_t alloc_txn_id{0};
    std::vector<std::string> staged_allowlist{};
    uint8_t last_seq_heartbeat{0};
    uint8_t last_seq_auth{0};
    uint8_t last_seq_slac{0};
    uint8_t last_seq_alloc{0};
    uint8_t last_seq_aux{0};
    uint8_t last_seq_session{0};
    bool seq_seen_heartbeat{false};
    bool seq_seen_auth{false};
    bool seq_seen_slac{false};
    bool seq_seen_alloc{false};
    bool seq_seen_aux{false};
    bool seq_seen_session{false};
    bool stop_active{false};
    bool stop_hard{false};
    bool stop_force_issued{false};
    uint8_t stop_reason{0};
    uint32_t stop_deadline_ms{0};
};

struct ControllerStatusTxSchedule {
    uint32_t next_heartbeat_ms{0};
    uint32_t next_cp_status_ms{0};
    uint32_t next_slac_status_ms{0};
    uint32_t next_hlc_status_ms{0};
    uint32_t next_power_status_ms{0};
    uint32_t next_session_status_ms{0};
    uint32_t next_bms_status_ms{0};
    uint8_t seq_heartbeat{0};
    uint8_t seq_cp{0};
    uint8_t seq_slac{0};
    uint8_t seq_hlc{0};
    uint8_t seq_power{0};
    uint8_t seq_session{0};
    uint8_t seq_bms{0};
    uint8_t seq_event{0};
    uint8_t seq_ack{0};
};

RuntimeConfig g_runtime_cfg{};
ControllerRuntimeState g_ctrl{};
ControllerStatusTxSchedule g_ctrl_tx{};
Preferences g_cfg_prefs{};
std::string g_local_group_id = "PLC_GROUP_1";
std::string g_local_module_id = "PLC1_MXR_01";
uint8_t g_local_module_group = 1u;

char g_cp_state = 'A';
char g_last_cp_state = 'A';
char g_cp_state_candidate = 'A';
uint32_t g_cp_candidate_since_ms = 0;
uint32_t g_cp_connected_since_ms = 0;
uint32_t g_cp_last_seen_connected_ms = 0;
uint32_t g_cp_next_spike_log_ms = 0;

uint16_t g_last_cp_duty_pct = 100;
bool g_ef_pulse_active = false;
uint32_t g_ef_pulse_until_ms = 0;

bool g_session_started = false;
bool g_bcd_entered = false;
bool g_session_matched = false;
uint8_t g_slac_failures_this_cp = 0;
uint32_t g_slac_hold_until_ms = 0;
slac::evse::State g_last_fsm_state = slac::evse::State::Reset;
uint32_t g_next_qca_init_ms = 0;

typedef struct {
    jpv2g_secc_t* secc;
    bool auth_seen;
    bool auth_ongoing_sent;
    bool precharge_seen;
    uint32_t precharge_count;
} HlcAppContext;

jpv2g_codec_ctx* g_codec = nullptr;
jpv2g_secc_t g_secc{};
HlcAppContext g_hlc_ctx{};
bool g_hlc_ready = false;
bool g_hlc_active = false;
int g_hlc_active_client_fd = -1;
uint8_t g_secc_ip[16] = {0};
uint32_t g_next_hlc_wait_log_ms = 0;
QueueHandle_t g_hlc_client_queue = nullptr;
TaskHandle_t g_hlc_worker_task = nullptr;
struct netif g_plc_netif{};
bool g_plc_netif_ready = false;
char g_plc_ifname[JPV2G_IFACE_NAME_MAX] = {0};
uint32_t g_next_lwip_drop_log_ms = 0;
struct LwipTxFrame {
    uint16_t len;
    uint8_t data[ETH_FRAME_LEN];
};
QueueHandle_t g_lwip_tx_queue = nullptr;
uint32_t g_next_lwip_tx_drop_log_ms = 0;
Mcp2515Transport g_can;
cbmodules::ModuleManager g_module_mgr{};
SemaphoreHandle_t g_module_mutex = nullptr;
SemaphoreHandle_t g_relay_mutex = nullptr;
bool g_module_ready = false;
bool g_relay_ready = false;
bool g_relay1_closed = false;
bool g_relay2_closed = false;
bool g_relay3_closed = false;
uint32_t g_relay2_deadline_ms = 0;
uint32_t g_relay3_deadline_ms = 0;
float g_last_module_target_v = 380.0f;
float g_last_module_target_i = PRECHARGE_CURRENT_LIMIT_A;
bool g_module_output_enabled = false;
std::vector<std::string> g_module_allowlist{};
double g_meter_wh_real = 0.0;
uint32_t g_meter_last_sample_ms = 0;
float g_last_measured_v = 0.0f;
float g_last_measured_i = 0.0f;
float g_last_bms_requested_v = 0.0f;
float g_last_bms_requested_i = 0.0f;
uint8_t g_last_bms_hlc_stage = 0u;
bool g_last_bms_valid = false;
bool g_last_bms_delivery_ready = false;
uint32_t g_last_bms_update_ms = 0;
bool g_last_bms_dirty = true;
int16_t g_last_ev_soc_pct = -1;
uint32_t g_last_module_runtime_log_ms = 0;

portMUX_TYPE g_ctrl_rx_mux = portMUX_INITIALIZER_UNLOCKED;
struct CtrlRxFrame {
    uint32_t id{0};
    bool extended{false};
    uint8_t dlc{0};
    uint8_t data[8]{};
    uint32_t rx_ms{0};
};
std::array<CtrlRxFrame, CTRL_RX_QUEUE_CAPACITY> g_ctrl_rx_queue{};
uint32_t g_ctrl_rx_head = 0;
uint32_t g_ctrl_rx_tail = 0;
uint32_t g_ctrl_rx_count = 0;

SemaphoreHandle_t g_can_mutex = nullptr;
String g_serial_cmd_buf{};
struct SerialControllerSeqState {
    uint8_t heartbeat{1u};
    uint8_t auth{1u};
    uint8_t slac{1u};
    uint8_t alloc{1u};
    uint8_t aux{1u};
    uint8_t session{1u};
};

SerialControllerSeqState g_serial_ctrl_seq{};
uint8_t g_serial_alloc_txn_id = 1u;

uint32_t can_with_plc_id(uint32_t base_id) {
    return (base_id & 0x1FFFFFF0u) | static_cast<uint32_t>(g_runtime_cfg.plc_id & 0x0Fu);
}

uint8_t normalize_plc_id(uint8_t plc_id) {
    return static_cast<uint8_t>(std::max<uint8_t>(1u, std::min<uint8_t>(MAX_RUNTIME_PLC_ID, plc_id)));
}

uint8_t normalize_connector_id(uint8_t connector_id, uint8_t plc_id) {
    if (connector_id == 0u) return plc_id;
    return connector_id;
}

uint8_t normalize_controller_id(uint8_t controller_id) {
    return static_cast<uint8_t>(std::max<uint8_t>(1u, std::min<uint8_t>(MAX_RUNTIME_CONTROLLER_ID, controller_id)));
}

uint8_t normalize_local_module_address(uint8_t module_address, uint8_t plc_id) {
    if (module_address == 0u) return plc_id;
    return module_address;
}

uint8_t runtime_local_group_numeric() {
    return static_cast<uint8_t>(std::max<uint8_t>(1u, std::min<uint8_t>(MAX_MODULE_GROUP_ID, g_runtime_cfg.plc_id)));
}

uint16_t runtime_module_manager_owner_id() {
    return static_cast<uint16_t>((static_cast<uint16_t>(g_runtime_cfg.controller_id & 0x0Fu) << 8) |
                                 static_cast<uint16_t>(g_runtime_cfg.plc_id));
}

void refresh_runtime_identity_cache() {
    g_local_module_group = runtime_local_group_numeric();

    char group_buf[24];
    snprintf(group_buf, sizeof(group_buf), "PLC_GROUP_%u", static_cast<unsigned>(g_runtime_cfg.plc_id));
    g_local_group_id = group_buf;

    char module_buf[32];
    snprintf(module_buf,
             sizeof(module_buf),
             "PLC%u_MXR_%02X",
             static_cast<unsigned>(g_runtime_cfg.plc_id),
             static_cast<unsigned>(g_runtime_cfg.local_module_address));
    g_local_module_id = module_buf;
}

const std::string& runtime_local_group_id() {
    return g_local_group_id;
}

const std::string& runtime_local_module_id() {
    return g_local_module_id;
}

bool is_controller_rx_base(uint32_t base_id) {
    return base_id == (CAN_ID_CTRL_HEARTBEAT & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_AUTH & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_SLAC & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_ALLOC_BEGIN & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_ALLOC_DATA & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_ALLOC_COMMIT & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_ALLOC_ABORT & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_AUX_RELAY & 0x1FFFFFF0u) ||
           base_id == (CAN_ID_CTRL_SESSION & 0x1FFFFFF0u);
}

bool is_local_module_can_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8]) {
    if (!extended) return false;
    const uint32_t raw_id = id & 0x1FFFFFFFu;
    const uint16_t proto = static_cast<uint16_t>((raw_id >> 20) & 0x1FFu);
    if (proto == MAXWELL_CAN_PROTOCOL_ID) {
        const uint8_t src = static_cast<uint8_t>((raw_id >> 3) & 0xFFu);
        return src == g_runtime_cfg.local_module_address;
    }

    if (!data || dlc < 8u) return false;
    const uint32_t family = raw_id & MODULE_OWNERSHIP_CAN_ID_MASK;
    if (family == MODULE_OWNERSHIP_CAN_ID_BASE || family == MODULE_OWNERSHIP_EPOCH_CAN_ID_BASE) {
        const auto type = static_cast<cbmodules::ModuleType>(data[1]);
        return type == cbmodules::ModuleType::MaxwellMxr && data[2] == g_runtime_cfg.local_module_address;
    }
    return false;
}

uint8_t crc8_07(const uint8_t* data, size_t len) {
    uint8_t crc = 0x00;
    if (!data) return crc;
    for (size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (uint8_t b = 0; b < 8; ++b) {
            if (crc & 0x80u) {
                crc = static_cast<uint8_t>((crc << 1) ^ 0x07u);
            } else {
                crc <<= 1;
            }
        }
    }
    return crc;
}

void tap_controller_rx_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8], uint32_t now_ms) {
    if (!extended || !data) return;
    if (dlc < 8u) return;
    const uint32_t target_plc = id & 0x0Fu;
    if (target_plc != static_cast<uint32_t>(g_runtime_cfg.plc_id & 0x0Fu)) return;
    if (data[0] != CTRL_MSG_VERSION) return;
    if ((data[2] & 0x0Fu) != (g_runtime_cfg.controller_id & 0x0Fu)) return;
    if (crc8_07(data, 7) != data[7]) return;
    portENTER_CRITICAL(&g_ctrl_rx_mux);
    if (g_ctrl_rx_count >= CTRL_RX_QUEUE_CAPACITY) {
        g_ctrl_rx_head = (g_ctrl_rx_head + 1u) % CTRL_RX_QUEUE_CAPACITY;
        g_ctrl_rx_count--;
    }
    CtrlRxFrame& slot = g_ctrl_rx_queue[g_ctrl_rx_tail];
    slot.id = id;
    slot.extended = extended;
    slot.dlc = static_cast<uint8_t>(std::min<uint8_t>(8u, dlc));
    memset(slot.data, 0, sizeof(slot.data));
    memcpy(slot.data, data, slot.dlc);
    slot.rx_ms = now_ms;
    g_ctrl_rx_tail = (g_ctrl_rx_tail + 1u) % CTRL_RX_QUEUE_CAPACITY;
    g_ctrl_rx_count++;
    portEXIT_CRITICAL(&g_ctrl_rx_mux);
}

bool pop_controller_rx_frame(CtrlRxFrame* out) {
    if (!out) return false;
    bool ok = false;
    portENTER_CRITICAL(&g_ctrl_rx_mux);
    if (g_ctrl_rx_count > 0) {
        *out = g_ctrl_rx_queue[g_ctrl_rx_head];
        g_ctrl_rx_head = (g_ctrl_rx_head + 1u) % CTRL_RX_QUEUE_CAPACITY;
        g_ctrl_rx_count--;
        ok = true;
    }
    portEXIT_CRITICAL(&g_ctrl_rx_mux);
    return ok;
}

bool can_send_frame_raw(uint32_t id, const uint8_t payload[8]) {
    if (!payload) return false;
    cbmodules::CanFrame f{};
    f.id = id & 0x1FFFFFFFu;
    f.extended = true;
    f.dlc = 8;
    memcpy(f.data, payload, 8);
    return g_can.send(f);
}

bool can_send_ctrl_frame(uint32_t base_id, uint8_t payload[8]) {
    if (!payload) return false;
    payload[7] = crc8_07(payload, 7);
    return can_send_frame_raw(can_with_plc_id(base_id), payload);
}

bool ctrl_seq_accept(uint8_t seq, uint8_t* last_seq, bool* seq_seen) {
    if (!last_seq || !seq_seen) return false;
    if (!*seq_seen) {
        *last_seq = seq;
        *seq_seen = true;
        return true;
    }
    const uint8_t prev = *last_seq;
    const uint8_t delta = static_cast<uint8_t>(seq - prev);
    if (delta == 0u) return false;
    if (delta > 127u) return false;
    *last_seq = seq;
    return true;
}

void controller_reset_command_seq_state(bool include_heartbeat) {
    if (include_heartbeat) {
        g_ctrl.last_seq_heartbeat = 0;
        g_ctrl.seq_seen_heartbeat = false;
    }
    g_ctrl.last_seq_auth = 0;
    g_ctrl.last_seq_slac = 0;
    g_ctrl.last_seq_alloc = 0;
    g_ctrl.last_seq_aux = 0;
    g_ctrl.last_seq_session = 0;
    g_ctrl.seq_seen_auth = false;
    g_ctrl.seq_seen_slac = false;
    g_ctrl.seq_seen_alloc = false;
    g_ctrl.seq_seen_aux = false;
    g_ctrl.seq_seen_session = false;
}

bool controller_heartbeat_alive(uint32_t now_ms) {
    if (g_runtime_cfg.standalone_mode) return true;
    if (!g_ctrl.heartbeat_seen) return false;
    if (now_ms < g_ctrl.last_heartbeat_ms) return false;
    const uint32_t age = now_ms - g_ctrl.last_heartbeat_ms;
    return age <= g_runtime_cfg.controller_heartbeat_timeout_ms;
}

bool controller_auth_granted(uint32_t now_ms) {
    if (g_runtime_cfg.standalone_mode) return true;
    if (g_ctrl.auth_state != ControllerAuthState::Granted) return false;
    if (g_ctrl.auth_expiry_ms == 0) return false;
    return static_cast<int32_t>(now_ms - g_ctrl.auth_expiry_ms) <= 0;
}

bool controller_allows_slac_start(uint32_t now_ms) {
    if (g_runtime_cfg.standalone_mode) return true;
    if (!g_runtime_cfg.slac_requires_controller_start) return controller_heartbeat_alive(now_ms);
    if (!controller_heartbeat_alive(now_ms)) return false;
    if (!g_ctrl.slac_start_latched) return false;
    if (g_ctrl.slac_arm_expiry_ms == 0) return false;
    return static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) <= 0;
}

bool controller_allows_energy(uint32_t now_ms) {
    if (g_runtime_cfg.standalone_mode) return true;
    if (!controller_heartbeat_alive(now_ms)) return false;
    if (!controller_auth_granted(now_ms)) return false;
    if (g_module_allowlist.empty()) return false;
    return true;
}

void send_ctrl_ack(uint8_t cmd_type, uint8_t seq, uint8_t status, uint8_t detail0, uint8_t detail1) {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_ack++;
    p[2] = cmd_type;
    p[3] = seq;
    p[4] = status;
    p[5] = detail0;
    p[6] = detail1;
    (void)can_send_ctrl_frame(CAN_ID_PLC_CMD_ACK, p);
    Serial.printf("[SERCTRL] ACK cmd=0x%02X seq=%u status=%u detail0=%u detail1=%u\n",
                  static_cast<unsigned>(cmd_type),
                  static_cast<unsigned>(seq),
                  static_cast<unsigned>(status),
                  static_cast<unsigned>(detail0),
                  static_cast<unsigned>(detail1));
}

float iso_pv_to_float(const iso2_PhysicalValueType* pv) {
    if (!pv) return 0.0f;
    float scale = 1.0f;
    if (pv->Multiplier > 0) {
        for (int8_t i = 0; i < pv->Multiplier; ++i) scale *= 10.0f;
    } else if (pv->Multiplier < 0) {
        for (int8_t i = 0; i < static_cast<int8_t>(-pv->Multiplier); ++i) scale *= 0.1f;
    }
    return static_cast<float>(pv->Value) * scale;
}

float din_pv_to_float(const din_PhysicalValueType* pv) {
    if (!pv) return 0.0f;
    float scale = 1.0f;
    if (pv->Multiplier > 0) {
        for (int8_t i = 0; i < pv->Multiplier; ++i) scale *= 10.0f;
    } else if (pv->Multiplier < 0) {
        for (int8_t i = 0; i < static_cast<int8_t>(-pv->Multiplier); ++i) scale *= 0.1f;
    }
    return static_cast<float>(pv->Value) * scale;
}

bool lock_modules(uint32_t timeout_ms) {
    if (!g_module_mutex) return false;
    return xSemaphoreTake(g_module_mutex, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
}

void unlock_modules() {
    if (g_module_mutex) xSemaphoreGive(g_module_mutex);
}

float sane_non_negative(float v) {
    if (!std::isfinite(v)) return 0.0f;
    return std::max(0.0f, v);
}

void clear_last_bms_request() {
    g_last_bms_requested_v = 0.0f;
    g_last_bms_requested_i = 0.0f;
    g_last_bms_hlc_stage = 0u;
    g_last_bms_valid = false;
    g_last_bms_delivery_ready = false;
    g_last_bms_update_ms = millis();
    g_last_bms_dirty = true;
}

void update_last_bms_request(float req_v, float req_i, uint8_t hlc_stage, bool delivery_ready, bool valid) {
    g_last_bms_requested_v = sane_non_negative(req_v);
    g_last_bms_requested_i = sane_non_negative(req_i);
    g_last_bms_hlc_stage = hlc_stage;
    g_last_bms_valid = valid;
    g_last_bms_delivery_ready = delivery_ready;
    g_last_bms_update_ms = millis();
    g_last_bms_dirty = true;
}

int16_t round_to_i16(float v) {
    if (!std::isfinite(v)) return 0;
    long iv = lroundf(v);
    if (iv > 32767L) return 32767;
    if (iv < -32768L) return -32768;
    return static_cast<int16_t>(iv);
}

void set_iso_physical(iso2_PhysicalValueType* pv, iso2_unitSymbolType unit, float value) {
    if (!pv) return;
    pv->Unit = unit;
    pv->Multiplier = 0;
    pv->Value = round_to_i16(sane_non_negative(value));
}

void set_din_physical(din_PhysicalValueType* pv, din_unitSymbolType unit, float value) {
    if (!pv) return;
    pv->Unit = unit;
    pv->Multiplier = 0;
    pv->Value = round_to_i16(sane_non_negative(value));
}

void set_iso_dc_status_ready(iso2_DC_EVSEStatusType* st) {
    if (!st) return;
    init_iso2_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = iso2_EVSENotificationType_None;
    st->EVSEStatusCode = iso2_DC_EVSEStatusCodeType_EVSE_Ready;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_iso_dc_status_not_ready(iso2_DC_EVSEStatusType* st) {
    if (!st) return;
    init_iso2_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = iso2_EVSENotificationType_None;
    st->EVSEStatusCode = iso2_DC_EVSEStatusCodeType_EVSE_NotReady;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_din_dc_status_ready(din_DC_EVSEStatusType* st) {
    if (!st) return;
    init_din_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = din_EVSENotificationType_None;
    st->EVSEStatusCode = din_DC_EVSEStatusCodeType_EVSE_Ready;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_din_dc_status_not_ready(din_DC_EVSEStatusType* st) {
    if (!st) return;
    init_din_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = din_EVSENotificationType_None;
    st->EVSEStatusCode = din_DC_EVSEStatusCodeType_EVSE_NotReady;
    st->EVSEIsolationStatus_isUsed = 0;
}

void update_real_meter_with_status(const cbmodules::GroupStatus& st, uint32_t now_ms) {
    const float v = sane_non_negative(st.combined_voltage_v);
    const float i = sane_non_negative(st.combined_current_a);

    if (g_meter_last_sample_ms != 0 && now_ms >= g_meter_last_sample_ms) {
        const uint32_t dt_ms = now_ms - g_meter_last_sample_ms;
        const double p_w = static_cast<double>(v) * static_cast<double>(i);
        g_meter_wh_real += (p_w * static_cast<double>(dt_ms)) / 3600000.0;
        if (!std::isfinite(g_meter_wh_real) || g_meter_wh_real < 0.0) {
            g_meter_wh_real = 0.0;
        }
    }
    g_meter_last_sample_ms = now_ms;
    g_last_measured_v = v;
    g_last_measured_i = i;
}

const char* module_lifecycle_name(cbmodules::ModuleLifecycle lc) {
    switch (lc) {
        case cbmodules::ModuleLifecycle::Missing: return "Missing";
        case cbmodules::ModuleLifecycle::Discovered: return "Discovered";
        case cbmodules::ModuleLifecycle::Validated: return "Validated";
        case cbmodules::ModuleLifecycle::Allocated: return "Allocated";
        case cbmodules::ModuleLifecycle::Draining: return "Draining";
        case cbmodules::ModuleLifecycle::Faulted: return "Faulted";
        case cbmodules::ModuleLifecycle::Quarantined: return "Quarantined";
        default: return "Unknown";
    }
}

void log_module_runtime_status(const cbmodules::GroupStatus& st, uint32_t now_ms) {
    if (g_last_module_runtime_log_ms != 0 &&
        now_ms >= g_last_module_runtime_log_ms &&
        (now_ms - g_last_module_runtime_log_ms) < MODULE_RUNTIME_LOG_INTERVAL_MS) {
        return;
    }
    g_last_module_runtime_log_ms = now_ms;

    Serial.printf("[MODRT][GRP] valid=%d en=%d assigned=%u active=%u sat=%d degr=%d reqV=%.1f reqI=%.1f appV=%.1f appI=%.1f cmbV=%.1f cmbI=%.2f availI=%.1f\n",
                  st.valid ? 1 : 0,
                  st.enable ? 1 : 0,
                  static_cast<unsigned>(st.assigned_modules),
                  static_cast<unsigned>(st.active_modules),
                  st.saturated ? 1 : 0,
                  st.degraded ? 1 : 0,
                  st.requested_voltage_v,
                  st.requested_current_a,
                  st.applied_voltage_v,
                  st.applied_current_a,
                  st.combined_voltage_v,
                  st.combined_current_a,
                  st.available_current_a);

    const auto states = g_module_mgr.module_states();
    for (const auto& s : states) {
        const uint32_t age_ms = (s.telemetry.last_update_ms == 0 || now_ms < s.telemetry.last_update_ms)
                                    ? UINT32_MAX
                                    : (now_ms - s.telemetry.last_update_ms);
        const bool fresh = age_ms != UINT32_MAX && age_ms <= MODULE_TELEMETRY_FRESH_MS;
        Serial.printf("[MODRT][MOD] id=%s lc=%s grp=%s lease=%d iso=%d off=%d fault=%d online=%d fresh=%d ageMs=%lu V=%.1f I=%.2f alarm=0x%08lX\n",
                      s.id.c_str(),
                      module_lifecycle_name(s.lifecycle),
                      s.group_id.c_str(),
                      s.lease_active ? 1 : 0,
                      s.isolation_latched ? 1 : 0,
                      s.telemetry.module_off ? 1 : 0,
                      s.telemetry.faulted ? 1 : 0,
                      s.telemetry.online ? 1 : 0,
                      fresh ? 1 : 0,
                      static_cast<unsigned long>(age_ms),
                      s.telemetry.voltage_v,
                      s.telemetry.current_a,
                      static_cast<unsigned long>(s.telemetry.alarms));
    }
}

void apply_fresh_measurements_from_states(cbmodules::GroupStatus* st, uint32_t now_ms) {
    if (!st) return;
    auto states = g_module_mgr.module_states();
    float sum_v = 0.0f;
    size_t count_v = 0;
    float sum_i = 0.0f;
    for (const auto& s : states) {
        if (s.group_id != runtime_local_group_id()) continue;
        if (s.lease_active || s.isolation_latched) continue;
        if (s.telemetry.faulted) continue;
        if (s.telemetry.last_update_ms == 0 || now_ms < s.telemetry.last_update_ms) continue;
        const uint32_t age_ms = now_ms - s.telemetry.last_update_ms;
        if (age_ms > MODULE_TELEMETRY_FRESH_MS) continue;

        const uint32_t v_ts = (s.telemetry.last_voltage_update_ms != 0)
                                  ? s.telemetry.last_voltage_update_ms
                                  : s.telemetry.last_update_ms;
        const uint32_t i_ts = (s.telemetry.last_current_update_ms != 0)
                                  ? s.telemetry.last_current_update_ms
                                  : s.telemetry.last_update_ms;

        if (v_ts != 0 && now_ms >= v_ts) {
            const uint32_t v_age_ms = now_ms - v_ts;
            if (v_age_ms <= MODULE_TELEMETRY_FRESH_MS) {
                const float v = sane_non_negative(s.telemetry.voltage_v);
                if (v > 0.1f) {
                    sum_v += v;
                    count_v++;
                }
            }
        }

        if (i_ts != 0 && now_ms >= i_ts) {
            const uint32_t i_age_ms = now_ms - i_ts;
            if (i_age_ms <= MODULE_TELEMETRY_FRESH_MS) {
                const float i = sane_non_negative(s.telemetry.current_a);
                sum_i += i;
            }
        }
    }
    st->combined_current_a = sum_i;
    st->combined_voltage_v = (count_v > 0) ? (sum_v / static_cast<float>(count_v)) : 0.0f;
    st->combined_power_kw = (st->combined_voltage_v * st->combined_current_a) / 1000.0f;
}

bool snapshot_group_status(cbmodules::GroupStatus* out, uint32_t* out_now_ms = nullptr) {
    if (!out || !g_module_ready) return false;
    if (!lock_modules(30)) return false;

    const uint32_t tick_ms = millis();
    g_module_mgr.poll_rx(24);
    g_module_mgr.tick(tick_ms);
    const uint32_t now_ms = millis();
    *out = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(out, now_ms);

    update_real_meter_with_status(*out, now_ms);

    unlock_modules();
    if (out_now_ms) *out_now_ms = now_ms;
    return out->valid;
}

int64_t real_meter_wh_i64() {
    const double v = std::isfinite(g_meter_wh_real) ? g_meter_wh_real : 0.0;
    if (v <= 0.0) return 0;
    const long long iv = static_cast<long long>(llround(v));
    return (iv < 0) ? 0 : static_cast<int64_t>(iv);
}

void update_last_ev_soc_pct(int soc_pct) {
    if (soc_pct < 0 || soc_pct > 100) {
        g_last_ev_soc_pct = -1;
        return;
    }
    g_last_ev_soc_pct = static_cast<int16_t>(soc_pct);
}

uint8_t encode_last_ev_soc_pct() {
    if (g_last_ev_soc_pct < 0 || g_last_ev_soc_pct > 100) {
        return 0xFFu;
    }
    return static_cast<uint8_t>(g_last_ev_soc_pct);
}

void update_last_ev_soc_from_request(jpv2g_message_type_t type, const jpv2g_secc_request_t* req) {
    if (!req || !req->body) return;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        switch (type) {
            case JPV2G_CHARGE_PARAMETER_DISCOVERY_REQ: {
                const auto* rq = static_cast<const iso2_ChargeParameterDiscoveryReqType*>(req->body);
                if (rq->DC_EVChargeParameter_isUsed) {
                    update_last_ev_soc_pct(static_cast<int>(rq->DC_EVChargeParameter.DC_EVStatus.EVRESSSOC));
                }
                break;
            }
            case JPV2G_CABLE_CHECK_REQ: {
                const auto* rq = static_cast<const iso2_CableCheckReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_PRE_CHARGE_REQ: {
                const auto* rq = static_cast<const iso2_PreChargeReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_CURRENT_DEMAND_REQ: {
                const auto* rq = static_cast<const iso2_CurrentDemandReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_WELDING_DETECTION_REQ: {
                const auto* rq = static_cast<const iso2_WeldingDetectionReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            default:
                break;
        }
        return;
    }
    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        switch (type) {
            case JPV2G_CHARGE_PARAMETER_DISCOVERY_REQ: {
                const auto* rq = static_cast<const din_ChargeParameterDiscoveryReqType*>(req->body);
                if (rq->DC_EVChargeParameter_isUsed) {
                    update_last_ev_soc_pct(static_cast<int>(rq->DC_EVChargeParameter.DC_EVStatus.EVRESSSOC));
                }
                break;
            }
            case JPV2G_CABLE_CHECK_REQ: {
                const auto* rq = static_cast<const din_CableCheckReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_PRE_CHARGE_REQ: {
                const auto* rq = static_cast<const din_PreChargeReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_CURRENT_DEMAND_REQ: {
                const auto* rq = static_cast<const din_CurrentDemandReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            case JPV2G_WELDING_DETECTION_REQ: {
                const auto* rq = static_cast<const din_WeldingDetectionReqType*>(req->body);
                update_last_ev_soc_pct(static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                break;
            }
            default:
                break;
        }
    }
}

void fill_iso_meter_info_real(iso2_MeterInfoType* meter) {
    if (!meter) return;
    init_iso2_MeterInfoType(meter);
    static constexpr char kMeterId[] = "CBMOD001";
    const size_t n = std::min(sizeof(kMeterId) - 1, static_cast<size_t>(iso2_MeterID_CHARACTER_SIZE - 1));
    memcpy(meter->MeterID.characters, kMeterId, n);
    meter->MeterID.charactersLen = static_cast<uint16_t>(n);
    meter->MeterReading = static_cast<uint64_t>(real_meter_wh_i64());
    meter->MeterReading_isUsed = 1;
    meter->MeterStatus = 0;
    meter->MeterStatus_isUsed = 1;
}

bool lock_relay(uint32_t timeout_ms) {
    if (!g_relay_mutex) return false;
    return xSemaphoreTake(g_relay_mutex, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
}

void unlock_relay() {
    if (g_relay_mutex) xSemaphoreGive(g_relay_mutex);
}

bool relay1_init() {
    if (!g_relay_mutex) {
        g_relay_mutex = xSemaphoreCreateMutex();
        if (!g_relay_mutex) return false;
    }
    if (!lock_relay(100)) return false;
    if (g_relay_ready) {
        unlock_relay();
        return true;
    }

    Wire.begin(RELAY_I2C_SDA, RELAY_I2C_SCL);
    Wire.setTimeOut(50);
    Wire.beginTransmission(RELAY_I2C_ADDR);
    const uint8_t rc = Wire.endTransmission();
    if (rc != 0) {
        Serial.printf("[RELAY] probe failed addr=0x%02X rc=%u\n",
                      static_cast<unsigned>(RELAY_I2C_ADDR),
                      static_cast<unsigned>(rc));
        unlock_relay();
        return false;
    }

    g_relay_expander.attach(Wire, RELAY_I2C_ADDR);
    const auto p1 = static_cast<PCA95x5::Port::Port>(RELAY1_EXP_PIN);
    const auto p2 = static_cast<PCA95x5::Port::Port>(RELAY2_EXP_PIN);
    const auto p3 = static_cast<PCA95x5::Port::Port>(RELAY3_EXP_PIN);
    const auto sw4 = static_cast<PCA95x5::Port::Port>(SW4_EXP_PIN);

    const bool off_level = RELAY_ACTIVE_HIGH ? false : true;
    (void)g_relay_expander.direction(p1, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(p2, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(p3, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(sw4, PCA95x5::Direction::IN);

    (void)g_relay_expander.write(p1, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    (void)g_relay_expander.write(p2, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    (void)g_relay_expander.write(p3, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    g_relay1_closed = false;
    g_relay2_closed = false;
    g_relay3_closed = false;
    g_relay_ready = true;
    unlock_relay();
    Serial.printf("[RELAY] ready addr=0x%02X pin=P00/P01/P02, SW4=P06\n", static_cast<unsigned>(RELAY_I2C_ADDR));
    return true;
}

bool relay_set_impl(int pin, bool* state_cache, bool closed, const char* reason, const char* label) {
    if (!g_relay_ready && !relay1_init()) return false;
    if (!state_cache) return false;
    if (!lock_relay(30)) return false;
    if (*state_cache == closed) {
        unlock_relay();
        return true;
    }
    const auto port = static_cast<PCA95x5::Port::Port>(pin);
    const bool level = RELAY_ACTIVE_HIGH ? closed : !closed;
    const bool ok = g_relay_expander.direction(port, PCA95x5::Direction::OUT) &&
                    g_relay_expander.write(port, level ? PCA95x5::Level::H : PCA95x5::Level::L);
    if (ok) {
        *state_cache = closed;
        Serial.printf("[RELAY] %s -> %s (%s)\n", label ? label : "Relay", closed ? "CLOSED" : "OPEN",
                      reason ? reason : "-");
    }
    unlock_relay();
    return ok;
}

bool relay1_set(bool closed, const char* reason) {
    return relay_set_impl(RELAY1_EXP_PIN, &g_relay1_closed, closed, reason, "Relay1");
}

bool relay2_set(bool closed, const char* reason) {
    return relay_set_impl(RELAY2_EXP_PIN, &g_relay2_closed, closed, reason, "Relay2");
}

bool relay3_set(bool closed, const char* reason) {
    return relay_set_impl(RELAY3_EXP_PIN, &g_relay3_closed, closed, reason, "Relay3");
}

bool read_sw4_pressed() {
    if (!g_relay_ready && !relay1_init()) return false;
    if (!lock_relay(20)) return false;
    const auto sw4 = static_cast<PCA95x5::Port::Port>(SW4_EXP_PIN);
    (void)g_relay_expander.direction(sw4, PCA95x5::Direction::IN);
    const auto lv = g_relay_expander.read(sw4);
    unlock_relay();
    return lv == PCA95x5::Level::L;
}

void normalize_runtime_config(RuntimeConfig* cfg) {
    if (!cfg) return;
    if (cfg->magic != CFG_MAGIC || cfg->version != CFG_VERSION) {
        *cfg = RuntimeConfig{};
        return;
    }
    cfg->plc_id = normalize_plc_id(static_cast<uint8_t>(cfg->plc_id & 0x0Fu));
    cfg->connector_id = normalize_connector_id(cfg->connector_id, cfg->plc_id);
    cfg->controller_id = normalize_controller_id(static_cast<uint8_t>(cfg->controller_id & 0x0Fu));
    cfg->local_module_address = normalize_local_module_address(cfg->local_module_address, cfg->plc_id);
    cfg->controller_heartbeat_timeout_ms =
        std::max<uint32_t>(500u, std::min<uint32_t>(cfg->controller_heartbeat_timeout_ms, 10000u));
    cfg->controller_auth_ttl_ms =
        std::max<uint32_t>(500u, std::min<uint32_t>(cfg->controller_auth_ttl_ms, 30000u));
    cfg->controller_alloc_ttl_ms =
        std::max<uint32_t>(500u, std::min<uint32_t>(cfg->controller_alloc_ttl_ms, 30000u));
    cfg->controller_slac_arm_timeout_ms =
        std::max<uint32_t>(1000u, std::min<uint32_t>(cfg->controller_slac_arm_timeout_ms, 60000u));
}

void print_runtime_config() {
    Serial.printf("[CFG] standalone=%d slac_ctrl=%d plc_id=%u connector_id=%u controller_id=%u module_addr=0x%02X local_group=%u owner_id=0x%04X hb_to=%lu auth_ttl=%lu alloc_ttl=%lu slac_arm_to=%lu\n",
                  g_runtime_cfg.standalone_mode ? 1 : 0,
                  g_runtime_cfg.slac_requires_controller_start ? 1 : 0,
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  static_cast<unsigned>(g_runtime_cfg.controller_id),
                  static_cast<unsigned>(g_runtime_cfg.local_module_address),
                  static_cast<unsigned>(g_local_module_group),
                  static_cast<unsigned>(runtime_module_manager_owner_id()),
                  static_cast<unsigned long>(g_runtime_cfg.controller_heartbeat_timeout_ms),
                  static_cast<unsigned long>(g_runtime_cfg.controller_auth_ttl_ms),
                  static_cast<unsigned long>(g_runtime_cfg.controller_alloc_ttl_ms),
                  static_cast<unsigned long>(g_runtime_cfg.controller_slac_arm_timeout_ms));
}

bool load_runtime_config_from_nvs() {
    g_runtime_cfg = RuntimeConfig{};
    if (!g_cfg_prefs.begin("cbplc_cfg", false)) {
        Serial.println("[CFG] NVS open failed, using defaults");
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        return false;
    }
    RuntimeConfig nvs_cfg{};
    const size_t got = g_cfg_prefs.getBytes("runtime_cfg", &nvs_cfg, sizeof(nvs_cfg));
    if (got != sizeof(nvs_cfg)) {
        Serial.println("[CFG] no persisted config, writing defaults");
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        (void)g_cfg_prefs.putBytes("runtime_cfg", &g_runtime_cfg, sizeof(g_runtime_cfg));
        g_cfg_prefs.end();
        return false;
    }
    g_cfg_prefs.end();
    normalize_runtime_config(&nvs_cfg);
    g_runtime_cfg = nvs_cfg;
    refresh_runtime_identity_cache();
    Serial.println("[CFG] loaded from NVS");
    return true;
}

bool save_runtime_config_to_nvs() {
    RuntimeConfig cfg = g_runtime_cfg;
    normalize_runtime_config(&cfg);
    if (!g_cfg_prefs.begin("cbplc_cfg", false)) {
        Serial.println("[CFG] NVS open failed for write");
        return false;
    }
    const size_t put = g_cfg_prefs.putBytes("runtime_cfg", &cfg, sizeof(cfg));
    g_cfg_prefs.end();
    if (put != sizeof(cfg)) {
        Serial.println("[CFG] NVS write failed");
        return false;
    }
    g_runtime_cfg = cfg;
    refresh_runtime_identity_cache();
    Serial.println("[CFG] saved to NVS");
    return true;
}

bool parse_bool_from_arg(const String& value) {
    const String v = value;
    return (v == "1" || v == "true" || v == "on" || v == "yes");
}

void launch_sw4_setup_portal_if_requested() {
    const bool pressed = read_sw4_pressed();
    Serial.printf("[CFG] SW4 boot state: %s\n", pressed ? "PRESSED" : "released");
    if (!pressed) {
        return;
    }

    WiFi.mode(WIFI_AP);
    WiFi.setSleep(false);
    const String ssid = String("CBPLC-SETUP-") +
                        String(g_local_mac[4], HEX) + String(g_local_mac[5], HEX);
    const bool ap_ok = WiFi.softAP(ssid.c_str());
    if (!ap_ok) {
        Serial.println("[CFG] setup AP start failed, continuing normal boot");
        WiFi.mode(WIFI_OFF);
        return;
    }

    IPAddress ip = WiFi.softAPIP();
    Serial.printf("[CFG] setup portal AP=%s ip=%s (120s timeout)\n", ssid.c_str(), ip.toString().c_str());

    WebServer server(80);
    bool saved = false;

    server.on("/", HTTP_GET, [&]() {
        String html;
        html.reserve(1800);
        html += "<html><body><h2>CB PLC Setup</h2>";
        html += "<form method='POST' action='/save'>";
        html += "Standalone mode <input name='standalone' value='";
        html += (g_runtime_cfg.standalone_mode ? "1" : "0");
        html += "'/><br/>";
        html += "SLAC requires controller start <input name='slac_ctrl' value='";
        html += (g_runtime_cfg.slac_requires_controller_start ? "1" : "0");
        html += "'/><br/>";
        html += "PLC ID (1-15) <input name='plc_id' value='" + String(g_runtime_cfg.plc_id) + "'/><br/>";
        html += "Connector ID <input name='connector_id' value='" + String(g_runtime_cfg.connector_id) + "'/><br/>";
        html += "Controller ID (1-15) <input name='controller_id' value='" + String(g_runtime_cfg.controller_id) + "'/><br/>";
        html += "Local module address <input name='module_addr' value='" + String(g_runtime_cfg.local_module_address) + "'/><br/>";
        html += "Heartbeat timeout ms <input name='hb_ms' value='" + String(g_runtime_cfg.controller_heartbeat_timeout_ms) + "'/><br/>";
        html += "Auth TTL ms <input name='auth_ms' value='" + String(g_runtime_cfg.controller_auth_ttl_ms) + "'/><br/>";
        html += "Alloc TTL ms <input name='alloc_ms' value='" + String(g_runtime_cfg.controller_alloc_ttl_ms) + "'/><br/>";
        html += "SLAC arm timeout ms <input name='slac_arm_ms' value='" + String(g_runtime_cfg.controller_slac_arm_timeout_ms) + "'/><br/>";
        html += "<button type='submit'>Save & Reboot</button></form></body></html>";
        server.send(200, "text/html", html);
    });

    server.on("/save", HTTP_POST, [&]() {
        RuntimeConfig next = g_runtime_cfg;
        if (server.hasArg("standalone")) next.standalone_mode = parse_bool_from_arg(server.arg("standalone"));
        if (server.hasArg("slac_ctrl")) next.slac_requires_controller_start = parse_bool_from_arg(server.arg("slac_ctrl"));
        if (server.hasArg("plc_id")) next.plc_id = static_cast<uint8_t>(std::max<long>(0, server.arg("plc_id").toInt()));
        if (server.hasArg("connector_id")) next.connector_id = static_cast<uint8_t>(std::max<long>(0, server.arg("connector_id").toInt()));
        if (server.hasArg("controller_id")) next.controller_id = static_cast<uint8_t>(std::max<long>(0, server.arg("controller_id").toInt()));
        if (server.hasArg("module_addr")) next.local_module_address = static_cast<uint8_t>(std::max<long>(0, server.arg("module_addr").toInt()));
        if (server.hasArg("hb_ms")) next.controller_heartbeat_timeout_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("hb_ms").toInt()));
        if (server.hasArg("auth_ms")) next.controller_auth_ttl_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("auth_ms").toInt()));
        if (server.hasArg("alloc_ms")) next.controller_alloc_ttl_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("alloc_ms").toInt()));
        if (server.hasArg("slac_arm_ms")) next.controller_slac_arm_timeout_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("slac_arm_ms").toInt()));
        normalize_runtime_config(&next);
        g_runtime_cfg = next;
        const bool ok = save_runtime_config_to_nvs();
        server.send(ok ? 200 : 500, "text/plain", ok ? "Saved. Rebooting..." : "Save failed");
        saved = ok;
    });

    server.begin();
    const uint32_t portal_start = millis();
    while ((millis() - portal_start) < 120000u && !saved) {
        server.handleClient();
        delay(2);
    }
    server.stop();
    WiFi.softAPdisconnect(true);
    WiFi.mode(WIFI_OFF);
    if (saved) {
        delay(200);
        ESP.restart();
    }
}

bool init_module_manager() {
    if (!g_module_mutex) {
        g_module_mutex = xSemaphoreCreateMutex();
        if (!g_module_mutex) return false;
    }
    if (!lock_modules(100)) return false;

    if (!g_can.begin()) {
        unlock_modules();
        Serial.println("[MOD] MCP2515 init failed");
        return false;
    }

    cbmodules::ModuleManagerConfig cfg;
    cfg.telemetry_stale_ms = 12000;
    cfg.startup_scan_default_timeout_ms = 3000;
    cfg.startup_probe_retry_ms = 200;
    cfg.runtime_probe_interval_ms = 200;
    cfg.module_quarantine_ms = 2000;
    cfg.drain_timeout_ms = 2500;
    cfg.can_bandwidth_cap_kbps = 5.0f;
    cfg.can_max_frames_per_second = 300.0f;
    cfg.can_max_tx_per_tick = 40;
    cfg.can_probe_tx_max_per_tick = 12;
    cfg.can_control_tx_reserve_per_tick = 8;
    cfg.can_control_bit_reserve_kbps = 1.0f;
    cfg.command_keepalive_ms = 600;
    cfg.command_keepalive_stable_ms = 1500;
    cfg.command_settle_window_ms = 2000;
    cfg.preventive_current_margin_pct = 0.025f;
    cfg.preventive_power_margin_pct = 0.025f;
    cfg.safety.enable_telemetry_stale_trip = false;
    cfg.safety.enable_multi_controller_guard = true;
    cfg.controller_id = runtime_module_manager_owner_id();

    g_module_mgr.~ModuleManager();
    new (&g_module_mgr) cbmodules::ModuleManager(cfg);
    g_module_mgr.set_transport(&g_can);

    cbmodules::ModuleSpec m1;
    m1.id = runtime_local_module_id();
    m1.type = cbmodules::ModuleType::MaxwellMxr;
    m1.slot_id = g_runtime_cfg.connector_id;
    m1.slot_index = g_runtime_cfg.connector_id;
    m1.address = g_runtime_cfg.local_module_address;
    m1.group = DEFAULT_LOCAL_MODULE_CAN_GROUP;
    m1.rated_current_a = MODULE_MAX_CURRENT_A;
    m1.rated_power_kw = MODULE_MAX_POWER_KW;
    m1.min_operating_voltage_v = 200.0f;
    m1.max_operating_voltage_v = MODULE_MAX_VOLTAGE_V;
    g_module_mgr.set_inventory({m1});

    Serial.printf("[MOD] startup scan begin (connector=%u plc=%u module=%s addr=0x%02X logical_group=%u module_can_group=%u owner_id=0x%04X)\n",
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  runtime_local_module_id().c_str(),
                  static_cast<unsigned>(g_runtime_cfg.local_module_address),
                  static_cast<unsigned>(g_local_module_group),
                  static_cast<unsigned>(DEFAULT_LOCAL_MODULE_CAN_GROUP),
                  static_cast<unsigned>(runtime_module_manager_owner_id()));
    const auto report = g_module_mgr.startup_scan_validate(5000);
    Serial.printf("[MOD] scan complete=%d expected=%u discovered=%u validated=%u missing=%u mismatch=%u\n",
                  report.complete ? 1 : 0,
                  static_cast<unsigned>(report.expected_count),
                  static_cast<unsigned>(report.discovered_count),
                  static_cast<unsigned>(report.validated_count),
                  static_cast<unsigned>(report.missing_count),
                  static_cast<unsigned>(report.mismatch_count));

    cbmodules::GroupConfig group;
    group.id = runtime_local_group_id();
    group.min_modules = 1;
    group.max_modules = 1;
    group.efficient_module_usage = true;
    group.hard_safety_clamp = true;
    group.max_group_current_a = MODULE_MAX_CURRENT_A;
    group.max_group_power_kw = MODULE_MAX_POWER_KW;

    const bool upsert_ok = g_module_mgr.upsert_group(group);
    const bool alloc_ok = g_module_mgr.allocate_module(runtime_local_module_id(), runtime_local_group_id());
    if (alloc_ok) {
        g_module_mgr.set_module_hard_limits(runtime_local_module_id(), MODULE_MAX_CURRENT_A, MODULE_MAX_POWER_KW);
    }
    g_module_mgr.set_group_target(runtime_local_group_id(), cbmodules::GroupTarget{false, 0.0f, 0.0f});
    g_module_mgr.tick(millis());
    g_module_ready = upsert_ok && alloc_ok;
    unlock_modules();

    Serial.printf("[MOD] setup upsert=%d allocate=%d ready=%d\n",
                  upsert_ok ? 1 : 0,
                  alloc_ok ? 1 : 0,
                  g_module_ready ? 1 : 0);
    return g_module_ready;
}

bool modules_apply_target(bool enable, float target_v, float target_i, const char* reason) {
    if (!g_module_ready) return false;
    if (!lock_modules(40)) return false;

    g_module_mgr.poll_rx(24);
    g_module_mgr.tick(millis());

    cbmodules::GroupTarget tgt{};
    tgt.enable = enable;
    tgt.voltage_v = std::max(0.0f, std::min(MODULE_MAX_VOLTAGE_V, target_v));
    tgt.current_a = std::max(0.0f, std::min(MODULE_MAX_CURRENT_A, target_i));
    if (!enable) {
        tgt.current_a = 0.0f;
    }

    const bool ok = g_module_mgr.apply_group_setpoint_allowlist(
        runtime_local_group_id(), tgt, g_module_allowlist, MODULE_ALLOWLIST_LEASE_MS);
    g_module_mgr.poll_rx(8);
    const uint32_t now_ms = millis();
    g_module_mgr.tick(now_ms);
    auto st = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(&st, now_ms);
    update_real_meter_with_status(st, now_ms);
    unlock_modules();

    if (ok) {
        g_last_module_target_v = tgt.voltage_v;
        g_last_module_target_i = tgt.current_a;
        g_module_output_enabled = enable;
    } else {
        Serial.printf("[MOD] apply failed (%s) en=%d v=%.1f i=%.1f\n",
                      reason ? reason : "-", enable ? 1 : 0, target_v, target_i);
    }
    return ok;
}

void request_modules_off(const char* reason) {
    if (g_module_allowlist.empty()) {
        g_module_output_enabled = false;
        g_last_module_target_i = 0.0f;
        return;
    }
    const bool ok = modules_apply_target(false, 0.0f, 0.0f, reason);
    g_module_output_enabled = false;
    g_last_module_target_i = 0.0f;
    if (!ok) {
        Serial.printf("[MOD] off request failed (%s)\n", reason ? reason : "-");
    }
}

void service_module_manager_once(uint32_t now_ms) {
    if (!g_module_ready) return;
    static uint32_t next_due_ms = 0;
    const bool slac_critical = g_session_started && !g_session_matched;
    const uint32_t interval = slac_critical ? MODULE_SERVICE_SLAC_CRITICAL_MS : MODULE_SERVICE_INTERVAL_MS;
    if (next_due_ms != 0 && static_cast<int32_t>(now_ms - next_due_ms) < 0) {
        return;
    }
    next_due_ms = now_ms + interval;
    if (!lock_modules(4)) return;
    g_module_mgr.poll_rx(slac_critical ? 4u : 16u);
    g_module_mgr.tick(now_ms);
    const uint32_t sample_ms = millis();
    auto st = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(&st, sample_ms);
    update_real_meter_with_status(st, sample_ms);
    log_module_runtime_status(st, sample_ms);
    unlock_modules();
}

void controller_poll_can_raw_once() {
    if (g_module_ready) return;
    cbmodules::CanFrame frame{};
    uint32_t budget = 16u;
    while (budget-- > 0u && g_can.receive(frame)) {
    }
}

bool module_id_known(const std::string& module_id) {
    if (module_id.empty()) return false;
    if (!g_module_ready) return false;
    if (!lock_modules(10)) return false;
    const auto states = g_module_mgr.module_states();
    unlock_modules();
    for (const auto& s : states) {
        if (s.id == module_id) return true;
    }
    return false;
}

std::string module_id_from_addr(uint8_t addr) {
    if (addr == g_runtime_cfg.local_module_address) return runtime_local_module_id();
    return std::string();
}

void controller_force_safe_stop(const char* reason) {
    request_modules_off(reason ? reason : "ControllerSafeStop");
    (void)relay1_set(false, reason ? reason : "ControllerSafeStop");
    stop_hlc_stack();
    stop_session(millis());
}

void controller_reset_runtime_state() {
    g_ctrl.slac_armed = false;
    g_ctrl.slac_start_latched = false;
    g_ctrl.slac_arm_expiry_ms = 0;
    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.auth_expiry_ms = 0;
    g_ctrl.heartbeat_seen = false;
    g_ctrl.last_heartbeat_ms = 0;
    g_ctrl.hb_lost_since_ms = 0;
    g_ctrl.alloc_txn_active = false;
    g_ctrl.staged_allowlist.clear();
    g_ctrl.alloc_txn_expiry_ms = 0;
    controller_reset_command_seq_state(true);
    g_ctrl.stop_active = false;
    g_ctrl.stop_hard = false;
    g_ctrl.stop_force_issued = false;
    g_ctrl.stop_reason = 0;
    g_ctrl.stop_deadline_ms = 0;
    g_last_seen_ev_mac_valid = false;
}

void controller_apply_mode_transition(bool next_standalone) {
    const bool mode_changed = (g_runtime_cfg.standalone_mode != next_standalone);
    g_runtime_cfg.standalone_mode = next_standalone;
    if (!mode_changed) {
        return;
    }

    const char* reason = next_standalone ? "ModeStandalone" : "ModeController";
    controller_force_safe_stop(reason);
    if (!next_standalone && !g_module_allowlist.empty()) {
        (void)apply_new_allowlist({}, reason);
    } else if (next_standalone && g_module_ready) {
        (void)apply_new_allowlist({runtime_local_module_id()}, reason);
    }
    controller_reset_runtime_state();
}

bool controller_stop_is_complete() {
    return (!g_relay1_closed) && (!g_module_output_enabled) && (!g_session_started) &&
           (!g_hlc_active) && g_module_allowlist.empty();
}

void controller_begin_stop(uint32_t now_ms, bool hard, uint32_t timeout_ms, uint8_t reason_code) {
    g_ctrl.stop_active = true;
    g_ctrl.stop_hard = hard;
    g_ctrl.stop_force_issued = false;
    g_ctrl.stop_reason = reason_code;
    g_ctrl.stop_deadline_ms = now_ms + timeout_ms;

    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.auth_expiry_ms = now_ms + 200u;
    g_ctrl.slac_armed = false;
    g_ctrl.slac_start_latched = false;
    g_ctrl.slac_arm_expiry_ms = 0;

    // Release module ownership for multi-PLC handover safety.
    (void)apply_new_allowlist({}, hard ? "CtrlStopHard" : "CtrlStopSoft");
    clear_last_bms_request();
    request_modules_off(hard ? "CtrlStopHard" : "CtrlStopSoft");
    (void)relay1_set(false, hard ? "CtrlStopHard" : "CtrlStopSoft");
}

void controller_apply_stop_policy(uint32_t now_ms) {
    if (!g_ctrl.stop_active) return;

    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.auth_expiry_ms = now_ms + 200u;
    g_ctrl.slac_armed = false;
    g_ctrl.slac_start_latched = false;
    g_ctrl.slac_arm_expiry_ms = 0;

    if (!g_module_allowlist.empty()) {
        (void)apply_new_allowlist({}, g_ctrl.stop_hard ? "CtrlStopHardRelease" : "CtrlStopSoftRelease");
    }
    clear_last_bms_request();
    request_modules_off(g_ctrl.stop_hard ? "CtrlStopHard" : "CtrlStopSoft");
    (void)relay1_set(false, g_ctrl.stop_hard ? "CtrlStopHard" : "CtrlStopSoft");

    if (g_ctrl.stop_hard) {
        if (!g_ctrl.stop_force_issued) {
            controller_force_safe_stop("CtrlStopHard");
            g_ctrl.stop_force_issued = true;
        }
    } else if (g_ctrl.stop_deadline_ms != 0 &&
               static_cast<int32_t>(now_ms - g_ctrl.stop_deadline_ms) > 0) {
        controller_force_safe_stop("CtrlStopSoftTimeout");
        g_ctrl.stop_hard = true;
        g_ctrl.stop_force_issued = true;
    }

    if (controller_stop_is_complete()) {
        g_ctrl.stop_active = false;
        g_ctrl.stop_hard = false;
        g_ctrl.stop_force_issued = false;
        g_ctrl.stop_deadline_ms = 0;
    }
}

void controller_publish_identity_segment(uint8_t event_kind, const uint8_t* data, size_t len, uint8_t event_id) {
    if (!data || len == 0) return;
    const size_t seg_count = (len + 1u) / 2u;
    for (size_t seg = 0; seg < seg_count; ++seg) {
        uint8_t p[8]{};
        p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
        p[1] = g_ctrl_tx.seq_event++;
        p[2] = event_kind;
        p[3] = event_id;
        p[4] = static_cast<uint8_t>((static_cast<uint8_t>(seg_count) << 4) | static_cast<uint8_t>(seg & 0x0Fu));
        const size_t off = seg * 2u;
        const size_t n = std::min<size_t>(2u, len - off);
        for (size_t i = 0; i < n; ++i) {
            p[5 + i] = data[off + i];
        }
        (void)can_send_ctrl_frame(CAN_ID_PLC_IDENTITY_EVT, p);
    }
}

void controller_publish_local_identity_once() {
    controller_publish_identity_segment(1u, g_local_mac, sizeof(g_local_mac), 1u);
    const uint16_t owner_id = runtime_module_manager_owner_id();
    const uint8_t identity[7] = {
        static_cast<uint8_t>(g_runtime_cfg.plc_id),
        static_cast<uint8_t>(g_runtime_cfg.connector_id),
        static_cast<uint8_t>(g_runtime_cfg.controller_id),
        static_cast<uint8_t>(g_runtime_cfg.local_module_address),
        static_cast<uint8_t>(g_local_module_group),
        static_cast<uint8_t>((owner_id >> 8) & 0xFFu),
        static_cast<uint8_t>(owner_id & 0xFFu),
    };
    controller_publish_identity_segment(5u, identity, sizeof(identity), 5u);
}

void controller_publish_ev_mac_if_changed(const uint8_t ev_mac[ETH_ALEN]) {
    if (!ev_mac) return;
    if (g_last_seen_ev_mac_valid && memcmp(g_last_seen_ev_mac, ev_mac, ETH_ALEN) == 0) {
        return;
    }
    memcpy(g_last_seen_ev_mac, ev_mac, ETH_ALEN);
    g_last_seen_ev_mac_valid = true;
    controller_publish_identity_segment(2u, ev_mac, ETH_ALEN, 2u);
}

void serial_print_identity(const char* label, const uint8_t* data, size_t len) {
    if (!label || !data || len == 0u) return;
    Serial.printf("[HLC] %s ", label);
    for (size_t i = 0; i < len; ++i) {
        Serial.printf("%02X", data[i]);
    }
    Serial.println();
}

void controller_publish_evccid(const uint8_t* data, size_t len) {
    if (!data || len == 0) return;
    controller_publish_identity_segment(3u, data, len, 3u);
    serial_print_identity("EVCCID", data, len);
}

void controller_publish_emaid(const uint8_t* data, size_t len) {
    if (!data || len == 0) return;
    controller_publish_identity_segment(4u, data, len, 4u);
    serial_print_identity("EMAID", data, len);
}

bool apply_new_allowlist(const std::vector<std::string>& next, const char* reason) {
    for (const auto& id : next) {
        if (!module_id_known(id)) {
            return false;
        }
    }
    if (next == g_module_allowlist) {
        return true;
    }

    const char* why = reason ? reason : "AllowlistChange";
    bool off_prev_ok = true;
    bool off_next_ok = true;

    if (g_module_ready) {
        if (!lock_modules(80)) {
            return false;
        }
        const uint32_t now_ms = millis();
        g_module_mgr.poll_rx(24);
        g_module_mgr.tick(now_ms);

        cbmodules::GroupTarget off{};
        off.enable = false;
        off.voltage_v = 0.0f;
        off.current_a = 0.0f;

        if (!g_module_allowlist.empty()) {
            off_prev_ok = g_module_mgr.apply_group_setpoint_allowlist(
                runtime_local_group_id(), off, g_module_allowlist, MODULE_ALLOWLIST_LEASE_MS);
            g_module_mgr.poll_rx(8);
            g_module_mgr.tick(millis());
        }

        // Apply destination allowlist with OFF target so removed modules are released now.
        off_next_ok = g_module_mgr.apply_group_setpoint_allowlist(
            runtime_local_group_id(), off, next, MODULE_ALLOWLIST_LEASE_MS);
        g_module_mgr.poll_rx(8);
        const uint32_t sample_ms = millis();
        g_module_mgr.tick(sample_ms);
        auto st = g_module_mgr.group_status(runtime_local_group_id());
        apply_fresh_measurements_from_states(&st, sample_ms);
        update_real_meter_with_status(st, sample_ms);
        unlock_modules();
    }

    (void)relay1_set(false, why);
    g_module_allowlist = next;
    g_module_output_enabled = false;
    g_last_module_target_i = 0.0f;

    if (!off_prev_ok || !off_next_ok) {
        Serial.printf("[CTRL] allowlist transition warning (%s): off_prev=%d off_next=%d\n",
                      why,
                      off_prev_ok ? 1 : 0,
                      off_next_ok ? 1 : 0);
    }
    return off_prev_ok && off_next_ok;
}

void controller_handle_heartbeat(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    const bool resync_needed = !controller_heartbeat_alive(f.rx_ms);
    if (resync_needed) {
        controller_reset_command_seq_state(true);
    }
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_heartbeat, &g_ctrl.seq_seen_heartbeat)) {
        send_ctrl_ack(0x10u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t hb_100ms = f.data[3];
    if (hb_100ms > 0u) {
        g_runtime_cfg.controller_heartbeat_timeout_ms =
            std::max<uint32_t>(500u, std::min<uint32_t>(10000u, static_cast<uint32_t>(hb_100ms) * 100u));
    }
    g_ctrl.last_heartbeat_ms = f.rx_ms;
    g_ctrl.heartbeat_seen = true;
    send_ctrl_ack(0x10u, seq, ACK_OK, 0, 0);
}

void controller_handle_auth(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_auth, &g_ctrl.seq_seen_auth)) {
        Serial.printf("[CTRLRX] AUTH reject bad-seq=%u last=%u seen=%d state=%d hb=%d\n",
                      static_cast<unsigned>(seq),
                      static_cast<unsigned>(g_ctrl.last_seq_auth),
                      g_ctrl.seq_seen_auth ? 1 : 0,
                      static_cast<int>(g_ctrl.auth_state),
                      controller_heartbeat_alive(f.rx_ms) ? 1 : 0);
        send_ctrl_ack(0x11u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t auth = f.data[3];
    const uint8_t ttl_100ms = f.data[4];
    if (auth > CTRL_AUTH_GRANTED) {
        send_ctrl_ack(0x11u, seq, ACK_BAD_VALUE, auth, 0);
        return;
    }
    g_ctrl.auth_state = static_cast<ControllerAuthState>(auth);
    g_ctrl.last_auth_update_ms = f.rx_ms;
    const uint32_t ttl_ms = (ttl_100ms > 0u) ? (static_cast<uint32_t>(ttl_100ms) * 100u)
                                             : g_runtime_cfg.controller_auth_ttl_ms;
    g_ctrl.auth_expiry_ms = f.rx_ms + ttl_ms;
    send_ctrl_ack(0x11u, seq, ACK_OK, auth, 0);
}

void controller_handle_slac(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_slac, &g_ctrl.seq_seen_slac)) {
        Serial.printf("[CTRLRX] SLAC reject bad-seq=%u last=%u seen=%d\n",
                      static_cast<unsigned>(seq),
                      static_cast<unsigned>(g_ctrl.last_seq_slac),
                      g_ctrl.seq_seen_slac ? 1 : 0);
        send_ctrl_ack(0x12u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t cmd = f.data[3];
    const uint8_t arm_100ms = f.data[4];
    const uint32_t arm_ms = (arm_100ms > 0u) ? (static_cast<uint32_t>(arm_100ms) * 100u)
                                             : g_runtime_cfg.controller_slac_arm_timeout_ms;
    if (cmd == CTRL_SLAC_DISARM) {
        g_ctrl.slac_armed = false;
        g_ctrl.slac_start_latched = false;
        g_ctrl.slac_arm_expiry_ms = 0;
        controller_force_safe_stop("CtrlDisarm");
        Serial.printf("[CTRLRX] SLAC disarm seq=%u\n", static_cast<unsigned>(seq));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    if (cmd == CTRL_SLAC_ARM) {
        if (g_ctrl.stop_active && controller_stop_is_complete()) {
            g_ctrl.stop_active = false;
            g_ctrl.stop_hard = false;
            g_ctrl.stop_force_issued = false;
            g_ctrl.stop_deadline_ms = 0;
        }
        g_ctrl.slac_armed = true;
        g_ctrl.slac_start_latched = false;
        g_ctrl.slac_arm_expiry_ms = f.rx_ms + arm_ms;
        const uint32_t now_dbg = millis();
        Serial.printf("[CTRLRX] SLAC arm seq=%u expiry_ms=%lu\n",
                      static_cast<unsigned>(seq),
                      static_cast<unsigned long>(g_ctrl.slac_arm_expiry_ms));
        Serial.printf("[CTRLRX] SLAC arm timing rx=%lu now=%lu delta=%ld\n",
                      static_cast<unsigned long>(f.rx_ms),
                      static_cast<unsigned long>(now_dbg),
                      static_cast<long>(now_dbg - f.rx_ms));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    if (cmd == CTRL_SLAC_START_NOW) {
        if (g_ctrl.stop_active && controller_stop_is_complete()) {
            g_ctrl.stop_active = false;
            g_ctrl.stop_hard = false;
            g_ctrl.stop_force_issued = false;
            g_ctrl.stop_deadline_ms = 0;
        }
        g_ctrl.slac_armed = true;
        g_ctrl.slac_start_latched = true;
        g_ctrl.slac_arm_expiry_ms = f.rx_ms + arm_ms;
        const uint32_t now_dbg = millis();
        Serial.printf("[CTRLRX] SLAC start seq=%u expiry_ms=%lu\n",
                      static_cast<unsigned>(seq),
                      static_cast<unsigned long>(g_ctrl.slac_arm_expiry_ms));
        Serial.printf("[CTRLRX] SLAC start timing rx=%lu now=%lu delta=%ld\n",
                      static_cast<unsigned long>(f.rx_ms),
                      static_cast<unsigned long>(now_dbg),
                      static_cast<long>(now_dbg - f.rx_ms));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    if (cmd == CTRL_SLAC_ABORT) {
        g_ctrl.slac_start_latched = false;
        g_ctrl.slac_armed = false;
        g_ctrl.slac_arm_expiry_ms = 0;
        controller_force_safe_stop("CtrlAbort");
        Serial.printf("[CTRLRX] SLAC abort seq=%u\n", static_cast<unsigned>(seq));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    send_ctrl_ack(0x12u, seq, ACK_BAD_VALUE, cmd, 0);
}

void controller_handle_alloc_begin(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_alloc, &g_ctrl.seq_seen_alloc)) {
        send_ctrl_ack(0x13u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t txn_id = f.data[3];
    const uint8_t expected = f.data[4];
    const uint8_t ttl_100ms = f.data[5];
    if (expected == 0u || expected > MAX_STAGED_ALLOC_MODULES) {
        send_ctrl_ack(0x13u, seq, ACK_BAD_VALUE, expected, 0);
        return;
    }
    g_ctrl.alloc_txn_active = true;
    g_ctrl.alloc_txn_id = txn_id;
    g_ctrl.staged_allowlist.clear();
    g_ctrl.staged_allowlist.reserve(expected);
    const uint32_t ttl_ms = (ttl_100ms > 0u) ? (static_cast<uint32_t>(ttl_100ms) * 100u)
                                             : g_runtime_cfg.controller_alloc_ttl_ms;
    g_ctrl.alloc_txn_expiry_ms = f.rx_ms + ttl_ms;
    send_ctrl_ack(0x13u, seq, ACK_OK, txn_id, expected);
}

void controller_handle_alloc_data(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_alloc, &g_ctrl.seq_seen_alloc)) {
        send_ctrl_ack(0x14u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    if (!g_ctrl.alloc_txn_active) {
        send_ctrl_ack(0x14u, seq, ACK_BAD_STATE, 0, 0);
        return;
    }
    const uint8_t txn_id = f.data[3];
    const uint8_t module_addr = f.data[5];
    if (txn_id != g_ctrl.alloc_txn_id) {
        send_ctrl_ack(0x14u, seq, ACK_BAD_VALUE, txn_id, g_ctrl.alloc_txn_id);
        return;
    }
    std::string id = module_id_from_addr(module_addr);
    if (id.empty()) {
        send_ctrl_ack(0x14u, seq, ACK_BAD_VALUE, module_addr, 0);
        return;
    }
    if (std::find(g_ctrl.staged_allowlist.begin(), g_ctrl.staged_allowlist.end(), id) ==
        g_ctrl.staged_allowlist.end()) {
        g_ctrl.staged_allowlist.push_back(id);
    }
    send_ctrl_ack(0x14u, seq, ACK_OK, module_addr, static_cast<uint8_t>(g_ctrl.staged_allowlist.size()));
}

void controller_handle_alloc_commit(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_alloc, &g_ctrl.seq_seen_alloc)) {
        send_ctrl_ack(0x15u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    if (!g_ctrl.alloc_txn_active) {
        send_ctrl_ack(0x15u, seq, ACK_BAD_STATE, 0, 0);
        return;
    }
    const uint8_t txn_id = f.data[3];
    if (txn_id != g_ctrl.alloc_txn_id) {
        send_ctrl_ack(0x15u, seq, ACK_BAD_VALUE, txn_id, g_ctrl.alloc_txn_id);
        return;
    }
    if (g_ctrl.staged_allowlist.empty()) {
        send_ctrl_ack(0x15u, seq, ACK_BAD_VALUE, 0, 0);
        return;
    }
    const bool ok = apply_new_allowlist(g_ctrl.staged_allowlist, "CtrlAllocCommit");
    g_ctrl.alloc_txn_active = false;
    g_ctrl.staged_allowlist.clear();
    g_ctrl.alloc_txn_expiry_ms = 0;
    send_ctrl_ack(0x15u, seq, ok ? ACK_OK : ACK_BAD_VALUE, static_cast<uint8_t>(g_module_allowlist.size()), 0);
}

void controller_handle_alloc_abort(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_alloc, &g_ctrl.seq_seen_alloc)) {
        send_ctrl_ack(0x16u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    g_ctrl.alloc_txn_active = false;
    g_ctrl.staged_allowlist.clear();
    g_ctrl.alloc_txn_expiry_ms = 0;
    send_ctrl_ack(0x16u, seq, ACK_OK, 0, 0);
}

void controller_handle_aux_relay(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_aux, &g_ctrl.seq_seen_aux)) {
        send_ctrl_ack(0x17u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t en_mask = f.data[3];
    const uint8_t st_mask = f.data[4];
    const uint8_t hold_100ms = f.data[5];
    const uint32_t hold_ms = static_cast<uint32_t>(hold_100ms) * 100u;
    bool ok = true;

    if (en_mask & 0x01u) {
        const bool relay2_ok = relay2_set((st_mask & 0x01u) != 0u, "CtrlRelay2");
        ok = ok && relay2_ok;
        if (relay2_ok) {
            g_relay2_deadline_ms = (hold_ms > 0u) ? (f.rx_ms + hold_ms) : 0u;
        }
    }
    if (en_mask & 0x02u) {
        const bool relay3_ok = relay3_set((st_mask & 0x02u) != 0u, "CtrlRelay3");
        ok = ok && relay3_ok;
        if (relay3_ok) {
            g_relay3_deadline_ms = (hold_ms > 0u) ? (f.rx_ms + hold_ms) : 0u;
        }
    }
    send_ctrl_ack(0x17u, seq, ok ? ACK_OK : ACK_BAD_STATE, en_mask, st_mask);
}

void controller_handle_session(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_session, &g_ctrl.seq_seen_session)) {
        send_ctrl_ack(0x18u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t action = f.data[3];
    const uint8_t timeout_100ms = f.data[4];
    const uint8_t reason = f.data[5];
    const uint32_t timeout_ms = (timeout_100ms > 0u)
                                    ? static_cast<uint32_t>(timeout_100ms) * 100u
                                    : 3000u;

    if (action == CTRL_SESSION_CLEAR || action == CTRL_SESSION_NONE) {
        g_ctrl.stop_active = false;
        g_ctrl.stop_hard = false;
        g_ctrl.stop_force_issued = false;
        g_ctrl.stop_deadline_ms = 0;
        send_ctrl_ack(0x18u, seq, ACK_OK, action, controller_stop_is_complete() ? 1u : 0u);
        return;
    }
    if (action != CTRL_SESSION_STOP_SOFT && action != CTRL_SESSION_STOP_HARD) {
        send_ctrl_ack(0x18u, seq, ACK_BAD_VALUE, action, 0);
        return;
    }

    controller_begin_stop(
        f.rx_ms,
        action == CTRL_SESSION_STOP_HARD,
        std::max<uint32_t>(500u, std::min<uint32_t>(30000u, timeout_ms)),
        reason);
    controller_apply_stop_policy(f.rx_ms);
    send_ctrl_ack(0x18u, seq, ACK_OK, action, controller_stop_is_complete() ? 1u : 0u);
}

void controller_process_rx_frame(const CtrlRxFrame& f) {
    if (!f.extended || f.dlc < 8u) return;
    const uint8_t calc_crc = crc8_07(f.data, 7);
    if (calc_crc != f.data[7]) {
        send_ctrl_ack(0x01u, f.data[1], ACK_BAD_CRC, calc_crc, f.data[7]);
        return;
    }
    if (f.data[0] != CTRL_MSG_VERSION) {
        send_ctrl_ack(0x01u, f.data[1], ACK_BAD_VERSION, f.data[0], CTRL_MSG_VERSION);
        return;
    }
    if ((f.data[2] & 0x0Fu) != (g_runtime_cfg.controller_id & 0x0Fu)) {
        send_ctrl_ack(0x01u, f.data[1], ACK_BAD_TARGET, f.data[2], g_runtime_cfg.controller_id);
        return;
    }

    const uint32_t base = f.id & 0x1FFFFFF0u;
    if (base == (CAN_ID_CTRL_HEARTBEAT & 0x1FFFFFF0u)) {
        controller_handle_heartbeat(f);
    } else if (base == (CAN_ID_CTRL_AUTH & 0x1FFFFFF0u)) {
        controller_handle_auth(f);
    } else if (base == (CAN_ID_CTRL_SLAC & 0x1FFFFFF0u)) {
        controller_handle_slac(f);
    } else if (base == (CAN_ID_CTRL_ALLOC_BEGIN & 0x1FFFFFF0u)) {
        controller_handle_alloc_begin(f);
    } else if (base == (CAN_ID_CTRL_ALLOC_DATA & 0x1FFFFFF0u)) {
        controller_handle_alloc_data(f);
    } else if (base == (CAN_ID_CTRL_ALLOC_COMMIT & 0x1FFFFFF0u)) {
        controller_handle_alloc_commit(f);
    } else if (base == (CAN_ID_CTRL_ALLOC_ABORT & 0x1FFFFFF0u)) {
        controller_handle_alloc_abort(f);
    } else if (base == (CAN_ID_CTRL_AUX_RELAY & 0x1FFFFFF0u)) {
        controller_handle_aux_relay(f);
    } else if (base == (CAN_ID_CTRL_SESSION & 0x1FFFFFF0u)) {
        controller_handle_session(f);
    }
}

void controller_service_rx() {
    CtrlRxFrame f{};
    uint32_t budget = 24u;
    while (budget-- > 0u && pop_controller_rx_frame(&f)) {
        controller_process_rx_frame(f);
    }
}

void controller_service_watchdogs(uint32_t now_ms) {
    controller_apply_stop_policy(now_ms);

    if (g_ctrl.alloc_txn_active && g_ctrl.alloc_txn_expiry_ms != 0 &&
        static_cast<int32_t>(now_ms - g_ctrl.alloc_txn_expiry_ms) > 0) {
        g_ctrl.alloc_txn_active = false;
        g_ctrl.staged_allowlist.clear();
        g_ctrl.alloc_txn_expiry_ms = 0;
        send_ctrl_ack(0x16u, 0, ACK_BAD_STATE, 0xEEu, 0);
    }
    if (g_relay2_deadline_ms != 0 && static_cast<int32_t>(now_ms - g_relay2_deadline_ms) > 0) {
        g_relay2_deadline_ms = 0;
        (void)relay2_set(false, "Relay2Timeout");
    }
    if (g_relay3_deadline_ms != 0 && static_cast<int32_t>(now_ms - g_relay3_deadline_ms) > 0) {
        g_relay3_deadline_ms = 0;
        (void)relay3_set(false, "Relay3Timeout");
    }
    if (!g_runtime_cfg.standalone_mode) {
        const bool hb_alive = controller_heartbeat_alive(now_ms);
        if (!hb_alive) {
            if (g_ctrl.heartbeat_seen) {
                if (g_ctrl.hb_lost_since_ms == 0u) {
                    g_ctrl.hb_lost_since_ms = now_ms;
                }
                // Preserve a freshly received SLAC start until the first heartbeat is
                // actually established. Only clear controller authorization after a
                // real heartbeat loss.
                g_ctrl.slac_start_latched = false;
                g_ctrl.slac_armed = false;

                const uint32_t lost_ms =
                    (now_ms >= g_ctrl.hb_lost_since_ms) ? (now_ms - g_ctrl.hb_lost_since_ms) : 0u;
                const bool energy_path_active = g_module_output_enabled || g_relay1_closed;
                if ((energy_path_active && lost_ms >= CONTROLLER_HB_SOFT_STOP_GRACE_MS) ||
                    ((g_session_started || g_hlc_active) && lost_ms >= CONTROLLER_HB_HARD_STOP_GRACE_MS)) {
                    controller_force_safe_stop("CtrlHeartbeatTimeout");
                    g_ctrl.auth_state = ControllerAuthState::Unknown;
                }
            }
        } else {
            g_ctrl.hb_lost_since_ms = 0u;
        }
        if (g_ctrl.auth_expiry_ms != 0 && static_cast<int32_t>(now_ms - g_ctrl.auth_expiry_ms) > 0) {
            if (g_ctrl.auth_state == ControllerAuthState::Granted) {
                g_ctrl.auth_state = ControllerAuthState::Denied;
                request_modules_off("CtrlAuthExpired");
                (void)relay1_set(false, "CtrlAuthExpired");
            }
        }
        if (g_ctrl.slac_arm_expiry_ms != 0 && static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) > 0) {
            g_ctrl.slac_armed = false;
            g_ctrl.slac_start_latched = false;
            g_ctrl.slac_arm_expiry_ms = 0;
        }
    }
}

void controller_tx_heartbeat(uint32_t now_ms) {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_heartbeat++;
    p[2] = static_cast<uint8_t>(g_runtime_cfg.standalone_mode ? 1u : 0u);
    p[3] = g_cp_state;
    p[4] = static_cast<uint8_t>(g_hlc_ready ? 1u : 0u) |
           static_cast<uint8_t>(g_hlc_active ? 2u : 0u) |
           static_cast<uint8_t>(g_module_output_enabled ? 4u : 0u);
    p[5] = static_cast<uint8_t>((now_ms / 1000u) & 0xFFu);
    p[6] = static_cast<uint8_t>(((now_ms / 1000u) >> 8) & 0xFFu);
    (void)can_send_ctrl_frame(CAN_ID_PLC_HEARTBEAT, p);
}

void controller_tx_cp_status(uint32_t now_ms) {
    const int cp_mv = g_last_cp_mv;
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_cp++;
    p[2] = static_cast<uint8_t>(g_cp_state);
    p[3] = static_cast<uint8_t>(g_last_cp_duty_pct);
    p[4] = static_cast<uint8_t>(cp_mv & 0xFF);
    p[5] = static_cast<uint8_t>((cp_mv >> 8) & 0xFF);
    const uint32_t since = g_cp_connected_since_ms ? (now_ms - g_cp_connected_since_ms) : 0u;
    p[6] = static_cast<uint8_t>((since / 100u) & 0xFFu);
    p[7] = cp_phase_code(g_cp_state, g_last_cp_duty_pct);
    (void)can_send_ctrl_frame(CAN_ID_PLC_CP_STATUS, p);
}

void controller_tx_slac_status() {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_slac++;
    p[2] = static_cast<uint8_t>(g_session_started ? 1u : 0u);
    p[3] = static_cast<uint8_t>(g_session_matched ? 1u : 0u);
    p[4] = static_cast<uint8_t>(g_fsm ? static_cast<uint8_t>(g_fsm->get_state()) : 0xFFu);
    p[5] = g_slac_failures_this_cp;
    p[6] = static_cast<uint8_t>(g_ctrl.slac_armed ? 1u : 0u) |
           static_cast<uint8_t>(g_ctrl.slac_start_latched ? 2u : 0u);
    (void)can_send_ctrl_frame(CAN_ID_PLC_SLAC_STATUS, p);
}

void controller_tx_hlc_status() {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_hlc++;
    p[2] = static_cast<uint8_t>(g_hlc_ready ? 1u : 0u) |
           static_cast<uint8_t>(g_hlc_active ? 2u : 0u);
    p[3] = static_cast<uint8_t>(g_ctrl.auth_state);
    p[4] = static_cast<uint8_t>(controller_heartbeat_alive(millis()) ? 1u : 0u);
    p[5] = static_cast<uint8_t>(controller_auth_granted(millis()) ? 1u : 0u);
    p[6] = static_cast<uint8_t>(g_hlc_ctx.precharge_seen ? 1u : 0u);
    (void)can_send_ctrl_frame(CAN_ID_PLC_HLC_STATUS, p);
}

void controller_tx_power_status() {
    cbmodules::GroupStatus st{};
    if (!snapshot_group_status(&st)) return;
    const uint16_t v01 = static_cast<uint16_t>(std::max(0.0f, std::min(6553.5f, st.combined_voltage_v)) * 10.0f);
    const uint16_t i01 = static_cast<uint16_t>(std::max(0.0f, std::min(6553.5f, st.combined_current_a)) * 10.0f);
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_power++;
    p[2] = static_cast<uint8_t>(v01 & 0xFFu);
    p[3] = static_cast<uint8_t>((v01 >> 8) & 0xFFu);
    p[4] = static_cast<uint8_t>(i01 & 0xFFu);
    p[5] = static_cast<uint8_t>((i01 >> 8) & 0xFFu);
    p[6] = static_cast<uint8_t>((st.active_modules & 0x0Fu) | ((st.assigned_modules & 0x0Fu) << 4));
    (void)can_send_ctrl_frame(CAN_ID_PLC_POWER_STATUS, p);
}

void controller_tx_session_status(uint32_t now_ms) {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_session++;
    p[2] = static_cast<uint8_t>(g_session_started ? 1u : 0u) |
           static_cast<uint8_t>(g_session_matched ? 2u : 0u);
    p[3] = static_cast<uint8_t>(g_relay1_closed ? 1u : 0u) |
           static_cast<uint8_t>(g_relay2_closed ? 2u : 0u) |
           static_cast<uint8_t>(g_relay3_closed ? 4u : 0u) |
           static_cast<uint8_t>(g_ctrl.stop_active ? 8u : 0u) |
           static_cast<uint8_t>(g_ctrl.stop_hard ? 16u : 0u) |
           static_cast<uint8_t>(controller_stop_is_complete() ? 32u : 0u);
    const uint32_t wh = static_cast<uint32_t>(std::max<int64_t>(0, std::min<int64_t>(real_meter_wh_i64(), 65535)));
    p[4] = static_cast<uint8_t>(wh & 0xFFu);
    p[5] = static_cast<uint8_t>((wh >> 8) & 0xFFu);
    p[6] = encode_last_ev_soc_pct();
    (void)can_send_ctrl_frame(CAN_ID_PLC_SESSION_STATUS, p);
}

void controller_tx_bms_status() {
    const uint16_t v01 = static_cast<uint16_t>(std::max(0.0f, std::min(6553.5f, g_last_bms_requested_v)) * 10.0f);
    const uint16_t i01 = static_cast<uint16_t>(std::max(0.0f, std::min(6553.5f, g_last_bms_requested_i)) * 10.0f);
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_ctrl_tx.seq_bms++;
    p[2] = static_cast<uint8_t>(v01 & 0xFFu);
    p[3] = static_cast<uint8_t>((v01 >> 8) & 0xFFu);
    p[4] = static_cast<uint8_t>(i01 & 0xFFu);
    p[5] = static_cast<uint8_t>((i01 >> 8) & 0xFFu);
    p[6] = static_cast<uint8_t>(g_last_bms_valid ? 1u : 0u) |
           static_cast<uint8_t>(g_last_bms_delivery_ready ? 2u : 0u) |
           static_cast<uint8_t>((g_last_bms_hlc_stage & 0x3Fu) << 2u);
    (void)can_send_ctrl_frame(CAN_ID_PLC_BMS_STATUS, p);
    g_last_bms_dirty = false;
}

void controller_service_periodic_tx(uint32_t now_ms) {
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_heartbeat_ms) >= 0) {
        controller_tx_heartbeat(now_ms);
        g_ctrl_tx.next_heartbeat_ms = now_ms + PLC_TX_HEARTBEAT_MS;
    }
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_cp_status_ms) >= 0) {
        controller_tx_cp_status(now_ms);
        g_ctrl_tx.next_cp_status_ms = now_ms + PLC_TX_CP_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_slac_status_ms) >= 0) {
        controller_tx_slac_status();
        g_ctrl_tx.next_slac_status_ms = now_ms + PLC_TX_SLAC_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_hlc_status_ms) >= 0) {
        controller_tx_hlc_status();
        g_ctrl_tx.next_hlc_status_ms = now_ms + PLC_TX_HLC_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_power_status_ms) >= 0) {
        controller_tx_power_status();
        g_ctrl_tx.next_power_status_ms = now_ms + PLC_TX_POWER_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_ctrl_tx.next_session_status_ms) >= 0) {
        controller_tx_session_status(now_ms);
        g_ctrl_tx.next_session_status_ms = now_ms + PLC_TX_SESSION_STATUS_MS;
    }
    if (g_last_bms_dirty || static_cast<int32_t>(now_ms - g_ctrl_tx.next_bms_status_ms) >= 0) {
        controller_tx_bms_status();
        g_ctrl_tx.next_bms_status_ms = now_ms + PLC_TX_BMS_STATUS_MS;
    }
}

uint32_t parse_u32_token(const String& token, uint32_t fallback) {
    if (token.isEmpty()) return fallback;
    char* end = nullptr;
    const unsigned long v = strtoul(token.c_str(), &end, 0);
    if (end == token.c_str()) return fallback;
    return static_cast<uint32_t>(v);
}

void serial_inject_controller_frame(uint32_t base_id, uint8_t payload[8]) {
    if (!payload) return;
    payload[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    uint8_t* seq = &g_serial_ctrl_seq.session;
    switch (base_id & 0x1FFFFFF0u) {
        case (CAN_ID_CTRL_HEARTBEAT & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.heartbeat;
            break;
        case (CAN_ID_CTRL_AUTH & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.auth;
            break;
        case (CAN_ID_CTRL_SLAC & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.slac;
            break;
        case (CAN_ID_CTRL_ALLOC_BEGIN & 0x1FFFFFF0u):
        case (CAN_ID_CTRL_ALLOC_DATA & 0x1FFFFFF0u):
        case (CAN_ID_CTRL_ALLOC_COMMIT & 0x1FFFFFF0u):
        case (CAN_ID_CTRL_ALLOC_ABORT & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.alloc;
            break;
        case (CAN_ID_CTRL_AUX_RELAY & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.aux;
            break;
        case (CAN_ID_CTRL_SESSION & 0x1FFFFFF0u):
        default:
            seq = &g_serial_ctrl_seq.session;
            break;
    }
    payload[1] = (*seq)++;
    payload[2] = static_cast<uint8_t>(g_runtime_cfg.controller_id & 0x0Fu);
    payload[7] = crc8_07(payload, 7);
    tap_controller_rx_frame(can_with_plc_id(base_id), true, 8, payload, millis());
}

void serial_ctrl_send_heartbeat(uint32_t timeout_ms) {
    uint8_t p[8]{};
    p[3] = static_cast<uint8_t>(std::max<uint32_t>(1u, std::min<uint32_t>(255u, timeout_ms / 100u)));
    serial_inject_controller_frame(CAN_ID_CTRL_HEARTBEAT, p);
}

void serial_ctrl_send_auth(uint8_t auth_state, uint32_t ttl_ms) {
    uint8_t p[8]{};
    p[3] = auth_state;
    p[4] = static_cast<uint8_t>(std::max<uint32_t>(1u, std::min<uint32_t>(255u, ttl_ms / 100u)));
    serial_inject_controller_frame(CAN_ID_CTRL_AUTH, p);
}

void serial_ctrl_send_slac(uint8_t cmd, uint32_t arm_ms) {
    uint8_t p[8]{};
    p[3] = cmd;
    p[4] = static_cast<uint8_t>(std::max<uint32_t>(1u, std::min<uint32_t>(255u, arm_ms / 100u)));
    serial_inject_controller_frame(CAN_ID_CTRL_SLAC, p);
}

void serial_ctrl_send_alloc_begin(uint8_t txn_id, uint8_t expected_modules, uint32_t ttl_ms) {
    uint8_t p[8]{};
    p[3] = txn_id;
    p[4] = expected_modules;
    p[5] = static_cast<uint8_t>(std::max<uint32_t>(1u, std::min<uint32_t>(255u, ttl_ms / 100u)));
    serial_inject_controller_frame(CAN_ID_CTRL_ALLOC_BEGIN, p);
}

void serial_ctrl_send_alloc_data(uint8_t txn_id, uint8_t index, uint8_t module_addr) {
    uint8_t p[8]{};
    p[3] = txn_id;
    p[4] = index;
    p[5] = module_addr;
    serial_inject_controller_frame(CAN_ID_CTRL_ALLOC_DATA, p);
}

void serial_ctrl_send_alloc_commit(uint8_t txn_id) {
    uint8_t p[8]{};
    p[3] = txn_id;
    serial_inject_controller_frame(CAN_ID_CTRL_ALLOC_COMMIT, p);
}

void serial_ctrl_send_alloc_abort() {
    uint8_t p[8]{};
    serial_inject_controller_frame(CAN_ID_CTRL_ALLOC_ABORT, p);
}

void serial_ctrl_send_relay(uint8_t enable_mask, uint8_t state_mask, uint32_t hold_ms) {
    uint8_t p[8]{};
    p[3] = enable_mask;
    p[4] = state_mask;
    p[5] = static_cast<uint8_t>(std::min<uint32_t>(255u, hold_ms / 100u));
    serial_inject_controller_frame(CAN_ID_CTRL_AUX_RELAY, p);
}

void serial_ctrl_send_session(uint8_t action, uint32_t timeout_ms, uint8_t reason_code) {
    uint8_t p[8]{};
    p[3] = action;
    p[4] = static_cast<uint8_t>(std::max<uint32_t>(1u, std::min<uint32_t>(255u, timeout_ms / 100u)));
    p[5] = reason_code;
    serial_inject_controller_frame(CAN_ID_CTRL_SESSION, p);
}

void serial_ctrl_print_help() {
    Serial.println("CTRL commands:");
    Serial.println("  CTRL HB [timeout_ms]");
    Serial.println("  CTRL AUTH <deny|pending|grant> [ttl_ms]");
    Serial.println("  CTRL SLAC <disarm|arm|start|abort> [arm_ms]");
    Serial.println("  CTRL RESET");
    Serial.println("  CTRL ALLOC1");
    Serial.println("  CTRL ALLOC BEGIN <txn> <expected> [ttl_ms]");
    Serial.println("  CTRL ALLOC DATA <txn> <index> <module_addr>");
    Serial.println("  CTRL ALLOC COMMIT <txn>");
    Serial.println("  CTRL ALLOC ABORT");
    Serial.println("  CTRL RELAY <enable_mask> <state_mask> [hold_ms]");
    Serial.println("  CTRL STOP <soft|hard|clear> [timeout_ms]");
    Serial.println("  CTRL MODE <standalone0|1> <plc_id 1..15> [controller_id 1..15]");
    Serial.println("  CTRL OWNERSHIP <connector_id> <module_addr>");
    Serial.println("  CTRL SAVE");
    Serial.println("  CTRL STATUS");
}

void serial_ctrl_handle_line(String line) {
    line.trim();
    if (line.isEmpty()) return;
    if (!line.startsWith("CTRL")) return;

    std::vector<String> t;
    int start = 0;
    while (start < line.length()) {
        while (start < line.length() && line[start] == ' ') start++;
        if (start >= line.length()) break;
        int end = start;
        while (end < line.length() && line[end] != ' ') end++;
        t.push_back(line.substring(start, end));
        start = end + 1;
    }
    if (t.size() < 2) {
        serial_ctrl_print_help();
        return;
    }

    String op = t[1];
    op.toUpperCase();

    if (op == "HELP") {
        serial_ctrl_print_help();
        return;
    }
    if (op == "HB") {
        const uint32_t timeout_ms = (t.size() >= 3) ? parse_u32_token(t[2], g_runtime_cfg.controller_heartbeat_timeout_ms)
                                                    : g_runtime_cfg.controller_heartbeat_timeout_ms;
        serial_ctrl_send_heartbeat(timeout_ms);
        Serial.printf("[SERCTRL] HB timeout_ms=%lu\n", static_cast<unsigned long>(timeout_ms));
        return;
    }
    if (op == "AUTH") {
        if (t.size() < 3) {
            Serial.println("[SERCTRL] AUTH missing state");
            return;
        }
        String s = t[2];
        s.toLowerCase();
        uint8_t st = CTRL_AUTH_PENDING;
        if (s == "deny" || s == "denied" || s == "0") st = CTRL_AUTH_DENY;
        else if (s == "grant" || s == "granted" || s == "2") st = CTRL_AUTH_GRANTED;
        const uint32_t ttl_ms = (t.size() >= 4) ? parse_u32_token(t[3], g_runtime_cfg.controller_auth_ttl_ms)
                                                : g_runtime_cfg.controller_auth_ttl_ms;
        serial_ctrl_send_auth(st, ttl_ms);
        Serial.printf("[SERCTRL] AUTH state=%u ttl_ms=%lu\n", static_cast<unsigned>(st), static_cast<unsigned long>(ttl_ms));
        return;
    }
    if (op == "SLAC") {
        if (t.size() < 3) {
            Serial.println("[SERCTRL] SLAC missing command");
            return;
        }
        String s = t[2];
        s.toLowerCase();
        uint8_t cmd = CTRL_SLAC_ARM;
        if (s == "disarm" || s == "0") cmd = CTRL_SLAC_DISARM;
        else if (s == "arm" || s == "1") cmd = CTRL_SLAC_ARM;
        else if (s == "start" || s == "start_now" || s == "2") cmd = CTRL_SLAC_START_NOW;
        else if (s == "abort" || s == "3") cmd = CTRL_SLAC_ABORT;
        const uint32_t arm_ms = (t.size() >= 4) ? parse_u32_token(t[3], g_runtime_cfg.controller_slac_arm_timeout_ms)
                                                : g_runtime_cfg.controller_slac_arm_timeout_ms;
        serial_ctrl_send_slac(cmd, arm_ms);
        Serial.printf("[SERCTRL] SLAC cmd=%u arm_ms=%lu\n", static_cast<unsigned>(cmd), static_cast<unsigned long>(arm_ms));
        return;
    }
    if (op == "RESET") {
        controller_reset_runtime_state();
        g_serial_ctrl_seq = {};
        controller_force_safe_stop("CtrlReset");
        Serial.println("[SERCTRL] RESET done");
        return;
    }
    if (op == "ALLOC1") {
        const uint8_t txn = g_serial_alloc_txn_id++;
        serial_ctrl_send_alloc_begin(txn, 1u, g_runtime_cfg.controller_alloc_ttl_ms);
        serial_ctrl_send_alloc_data(txn, 0u, g_runtime_cfg.local_module_address);
        serial_ctrl_send_alloc_commit(txn);
        Serial.printf("[SERCTRL] ALLOC1 txn=%u module=0x%02X\n",
                      static_cast<unsigned>(txn),
                      static_cast<unsigned>(g_runtime_cfg.local_module_address));
        return;
    }
    if (op == "ALLOC") {
        if (t.size() < 3) {
            Serial.println("[SERCTRL] ALLOC missing subcommand");
            return;
        }
        String sub = t[2];
        sub.toUpperCase();
        if (sub == "BEGIN") {
            if (t.size() < 5) {
                Serial.println("[SERCTRL] ALLOC BEGIN <txn> <expected> [ttl_ms]");
                return;
            }
            const uint8_t txn = static_cast<uint8_t>(parse_u32_token(t[3], 1u) & 0xFFu);
            const uint8_t expected = static_cast<uint8_t>(parse_u32_token(t[4], 1u) & 0xFFu);
            const uint32_t ttl_ms = (t.size() >= 6) ? parse_u32_token(t[5], g_runtime_cfg.controller_alloc_ttl_ms)
                                                    : g_runtime_cfg.controller_alloc_ttl_ms;
            serial_ctrl_send_alloc_begin(txn, expected, ttl_ms);
            Serial.printf("[SERCTRL] ALLOC BEGIN txn=%u expected=%u ttl_ms=%lu\n",
                          static_cast<unsigned>(txn), static_cast<unsigned>(expected), static_cast<unsigned long>(ttl_ms));
            return;
        }
        if (sub == "DATA") {
            if (t.size() < 6) {
                Serial.println("[SERCTRL] ALLOC DATA <txn> <index> <module_addr>");
                return;
            }
            const uint8_t txn = static_cast<uint8_t>(parse_u32_token(t[3], 0u) & 0xFFu);
            const uint8_t idx = static_cast<uint8_t>(parse_u32_token(t[4], 0u) & 0xFFu);
            const uint8_t addr = static_cast<uint8_t>(parse_u32_token(t[5], 0u) & 0xFFu);
            serial_ctrl_send_alloc_data(txn, idx, addr);
            Serial.printf("[SERCTRL] ALLOC DATA txn=%u idx=%u addr=0x%02X\n",
                          static_cast<unsigned>(txn), static_cast<unsigned>(idx), static_cast<unsigned>(addr));
            return;
        }
        if (sub == "COMMIT") {
            if (t.size() < 4) {
                Serial.println("[SERCTRL] ALLOC COMMIT <txn>");
                return;
            }
            const uint8_t txn = static_cast<uint8_t>(parse_u32_token(t[3], 0u) & 0xFFu);
            serial_ctrl_send_alloc_commit(txn);
            Serial.printf("[SERCTRL] ALLOC COMMIT txn=%u\n", static_cast<unsigned>(txn));
            return;
        }
        if (sub == "ABORT") {
            serial_ctrl_send_alloc_abort();
            Serial.println("[SERCTRL] ALLOC ABORT");
            return;
        }
        Serial.println("[SERCTRL] unknown ALLOC subcommand");
        return;
    }
    if (op == "RELAY") {
        if (t.size() < 4) {
            Serial.println("[SERCTRL] RELAY <enable_mask> <state_mask> [hold_ms]");
            return;
        }
        const uint8_t en = static_cast<uint8_t>(parse_u32_token(t[2], 0u) & 0xFFu);
        const uint8_t st = static_cast<uint8_t>(parse_u32_token(t[3], 0u) & 0xFFu);
        const uint32_t hold_ms = (t.size() >= 5) ? parse_u32_token(t[4], 0u) : 0u;
        serial_ctrl_send_relay(en, st, hold_ms);
        Serial.printf("[SERCTRL] RELAY en=0x%02X st=0x%02X hold_ms=%lu\n",
                      static_cast<unsigned>(en), static_cast<unsigned>(st), static_cast<unsigned long>(hold_ms));
        return;
    }
    if (op == "STOP") {
        if (t.size() < 3) {
            Serial.println("[SERCTRL] STOP <soft|hard|clear> [timeout_ms]");
            return;
        }
        String mode = t[2];
        mode.toLowerCase();
        uint8_t action = CTRL_SESSION_STOP_HARD;
        if (mode == "soft" || mode == "1") action = CTRL_SESSION_STOP_SOFT;
        else if (mode == "hard" || mode == "2") action = CTRL_SESSION_STOP_HARD;
        else if (mode == "clear" || mode == "0" || mode == "3") action = CTRL_SESSION_CLEAR;
        else {
            Serial.println("[SERCTRL] STOP invalid mode");
            return;
        }
        const uint32_t timeout_ms = (t.size() >= 4) ? parse_u32_token(t[3], 3000u) : 3000u;
        serial_ctrl_send_session(action, timeout_ms, 1u);
        Serial.printf("[SERCTRL] STOP action=%u timeout_ms=%lu\n",
                      static_cast<unsigned>(action), static_cast<unsigned long>(timeout_ms));
        return;
    }
    if (op == "MODE") {
        if (t.size() < 4) {
            Serial.println("[SERCTRL] MODE <standalone0|1> <plc_id> [controller_id]");
            return;
        }
        const bool next_standalone = parse_u32_token(t[2], 0u) ? true : false;
        controller_apply_mode_transition(next_standalone);
        g_runtime_cfg.plc_id = static_cast<uint8_t>(parse_u32_token(t[3], g_runtime_cfg.plc_id));
        if (t.size() >= 5) {
            g_runtime_cfg.controller_id = static_cast<uint8_t>(parse_u32_token(t[4], g_runtime_cfg.controller_id));
        }
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        Serial.printf("[SERCTRL] MODE standalone=%d plc_id=%u connector_id=%u controller_id=%u module_addr=0x%02X (use CTRL SAVE then reboot)\n",
                      g_runtime_cfg.standalone_mode ? 1 : 0,
                      static_cast<unsigned>(g_runtime_cfg.plc_id),
                      static_cast<unsigned>(g_runtime_cfg.connector_id),
                      static_cast<unsigned>(g_runtime_cfg.controller_id),
                      static_cast<unsigned>(g_runtime_cfg.local_module_address));
        return;
    }
    if (op == "OWNERSHIP") {
        if (t.size() < 4) {
            Serial.println("[SERCTRL] OWNERSHIP <connector_id> <module_addr>");
            return;
        }
        g_runtime_cfg.connector_id = static_cast<uint8_t>(parse_u32_token(t[2], g_runtime_cfg.connector_id));
        g_runtime_cfg.local_module_address = static_cast<uint8_t>(parse_u32_token(t[3], g_runtime_cfg.local_module_address));
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        Serial.printf("[SERCTRL] OWNERSHIP connector_id=%u module_addr=0x%02X local_group=%u module_id=%s (save then reboot to rebind runtime inventory)\n",
                      static_cast<unsigned>(g_runtime_cfg.connector_id),
                      static_cast<unsigned>(g_runtime_cfg.local_module_address),
                      static_cast<unsigned>(g_local_module_group),
                      runtime_local_module_id().c_str());
        return;
    }
    if (op == "SAVE") {
        const bool ok = save_runtime_config_to_nvs();
        Serial.printf("[SERCTRL] SAVE %s\n", ok ? "OK" : "FAIL");
        return;
    }
    if (op == "STATUS") {
        Serial.printf("[SERCTRL] STATUS standalone=%d plc_id=%u connector_id=%u controller_id=%u module_addr=0x%02X local_group=%u module_id=%s cp=%s duty=%u hb=%d auth=%u allow_slac=%d allow_energy=%d armed=%d start=%d alloc_sz=%u stop_active=%d stop_hard=%d stop_done=%d\n",
                      g_runtime_cfg.standalone_mode ? 1 : 0,
                      static_cast<unsigned>(g_runtime_cfg.plc_id),
                      static_cast<unsigned>(g_runtime_cfg.connector_id),
                      static_cast<unsigned>(g_runtime_cfg.controller_id),
                      static_cast<unsigned>(g_runtime_cfg.local_module_address),
                      static_cast<unsigned>(g_local_module_group),
                      runtime_local_module_id().c_str(),
                      cp_phase_label(g_cp_state, g_last_cp_duty_pct),
                      static_cast<unsigned>(g_last_cp_duty_pct),
                      controller_heartbeat_alive(millis()) ? 1 : 0,
                      static_cast<unsigned>(g_ctrl.auth_state),
                      controller_allows_slac_start(millis()) ? 1 : 0,
                      controller_allows_energy(millis()) ? 1 : 0,
                      g_ctrl.slac_armed ? 1 : 0,
                      g_ctrl.slac_start_latched ? 1 : 0,
                      static_cast<unsigned>(g_module_allowlist.size()),
                      g_ctrl.stop_active ? 1 : 0,
                      g_ctrl.stop_hard ? 1 : 0,
                      controller_stop_is_complete() ? 1 : 0);
        return;
    }

    Serial.printf("[SERCTRL] unknown command: %s\n", line.c_str());
}

void service_serial_commands() {
    while (Serial.available() > 0) {
        const int ch = Serial.read();
        if (ch < 0) break;
        if (ch == '\r') continue;
        if (ch == '\n') {
            if (!g_serial_cmd_buf.isEmpty()) {
                serial_ctrl_handle_line(g_serial_cmd_buf);
                g_serial_cmd_buf = "";
            }
            continue;
        }
        if (g_serial_cmd_buf.length() < 256) {
            g_serial_cmd_buf += static_cast<char>(ch);
        }
    }
}

void ensure_lwip_socket_stack_ready() {
    static bool initialized = false;
    if (initialized) return;
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    initialized = true;
    Serial.println("[NET] lwIP socket stack initialized (WiFi STA mode)");
}

void build_link_local_from_mac(const uint8_t mac[6], uint8_t out_ip[16]) {
    memset(out_ip, 0, 16);
    out_ip[0] = 0xFE;
    out_ip[1] = 0x80;
    out_ip[8] = static_cast<uint8_t>(mac[0] ^ 0x02U);
    out_ip[9] = mac[1];
    out_ip[10] = mac[2];
    out_ip[11] = 0xFF;
    out_ip[12] = 0xFE;
    out_ip[13] = mac[3];
    out_ip[14] = mac[4];
    out_ip[15] = mac[5];
}

bool ensure_lwip_tx_queue_ready() {
    if (g_lwip_tx_queue) {
        return true;
    }
    g_lwip_tx_queue = xQueueCreate(LWIP_TX_QUEUE_DEPTH, sizeof(LwipTxFrame));
    if (!g_lwip_tx_queue) {
        Serial.println("[NET] lwIP TX queue create failed");
        return false;
    }
    return true;
}

err_t plc_netif_linkoutput(struct netif* netif, struct pbuf* p) {
    (void)netif;
    if (!p || !g_lwip_tx_queue) {
        return ERR_IF;
    }

    const uint16_t total = static_cast<uint16_t>(p->tot_len);
    if (total == 0 || total > ETH_FRAME_LEN) {
        return ERR_IF;
    }

    LwipTxFrame frame{};
    frame.len = total;
    if (pbuf_copy_partial(p, frame.data, total, 0) != total) {
        return ERR_BUF;
    }
    if (xQueueSend(g_lwip_tx_queue, &frame, 0) != pdTRUE) {
        const uint32_t now = millis();
        if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
            Serial.println("[NET] lwIP TX queue full, dropping frame");
            g_next_lwip_tx_drop_log_ms = now + 2000;
        }
        return ERR_MEM;
    }
    return ERR_OK;
}

err_t plc_netif_init(struct netif* netif) {
    if (!netif) return ERR_ARG;

    netif->name[0] = 'p';
    netif->name[1] = 'l';
    netif->mtu = 1500;
    netif->hwaddr_len = ETH_ALEN;
    memcpy(netif->hwaddr, g_local_mac, ETH_ALEN);
    netif->flags = NETIF_FLAG_BROADCAST | NETIF_FLAG_ETHARP | NETIF_FLAG_ETHERNET;
#if LWIP_IPV6_MLD
    netif->flags |= NETIF_FLAG_MLD6;
#endif
    netif->output = etharp_output;
    netif->output_ip6 = ethip6_output;
    netif->linkoutput = plc_netif_linkoutput;
    return ERR_OK;
}

bool init_plc_lwip_netif() {
    if (g_plc_netif_ready) {
        return true;
    }
    ensure_lwip_socket_stack_ready();
    if (!ensure_lwip_tx_queue_ready()) {
        return false;
    }

    memset(&g_plc_netif, 0, sizeof(g_plc_netif));
    err_t rc = ERR_OK;
    struct netif* added = nullptr;
    LOCK_TCPIP_CORE();
#if LWIP_IPV4
    ip4_addr_t ipaddr;
    ip4_addr_t netmask;
    ip4_addr_t gw;
    IP4_ADDR(&ipaddr, 0, 0, 0, 0);
    IP4_ADDR(&netmask, 0, 0, 0, 0);
    IP4_ADDR(&gw, 0, 0, 0, 0);
    added = netif_add(&g_plc_netif, &ipaddr, &netmask, &gw, nullptr, plc_netif_init, tcpip_input);
#else
    added = netif_add_noaddr(&g_plc_netif, nullptr, plc_netif_init, tcpip_input);
#endif
    if (!added) {
        rc = ERR_IF;
    } else {
        netif_set_default(&g_plc_netif);
        netif_set_up(&g_plc_netif);
        netif_set_link_up(&g_plc_netif);
#if LWIP_IPV6
        netif_create_ip6_linklocal_address(&g_plc_netif, 1);
#endif
    }
    UNLOCK_TCPIP_CORE();

    if (rc != ERR_OK) {
        Serial.printf("[NET] netif add failed err=%d\n", static_cast<int>(rc));
        return false;
    }

    snprintf(g_plc_ifname, sizeof(g_plc_ifname), "%c%c%u",
             g_plc_netif.name[0], g_plc_netif.name[1], static_cast<unsigned>(g_plc_netif.num));
    g_plc_netif_ready = true;
    Serial.printf("[NET] PLC lwIP netif ready: %s\n", g_plc_ifname);
    return true;
}

void lwip_ingress_ethernet_frame(const uint8_t* frame, uint16_t len) {
    if (!g_plc_netif_ready || !frame || len < 14U || len > ETH_FRAME_LEN) {
        return;
    }

    struct pbuf* p = pbuf_alloc(PBUF_RAW, len, PBUF_POOL);
    if (!p) {
        const uint32_t now = millis();
        if (g_next_lwip_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_drop_log_ms) >= 0) {
            Serial.println("[NET] lwIP RX drop: pbuf_alloc failed");
            g_next_lwip_drop_log_ms = now + 2000;
        }
        return;
    }

    if (pbuf_take(p, frame, len) != ERR_OK) {
        pbuf_free(p);
        return;
    }

    const err_t rc = g_plc_netif.input(p, &g_plc_netif);
    if (rc != ERR_OK) {
        pbuf_free(p);
        const uint32_t now = millis();
        if (g_next_lwip_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_drop_log_ms) >= 0) {
            Serial.printf("[NET] lwIP RX drop: input err=%d\n", rc);
            g_next_lwip_drop_log_ms = now + 2000;
        }
    }
}

void service_lwip_tx_queue_once() {
    if (!g_lwip_tx_queue || !g_transport || !g_transport->is_ready()) {
        return;
    }
    LwipTxFrame frame{};
    while (xQueueReceive(g_lwip_tx_queue, &frame, 0) == pdTRUE) {
        if (frame.len == 0 || frame.len > ETH_FRAME_LEN) {
            continue;
        }
        const auto rc = g_transport->write(frame.data, frame.len, 20);
        if (rc != slac::ITransport::IOResult::Ok) {
            const uint32_t now = millis();
            if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
                Serial.printf("[NET] lwIP TX write failed rc=%d\n", static_cast<int>(rc));
                g_next_lwip_tx_drop_log_ms = now + 2000;
            }
        }
    }
}

void hlc_reset_session_state(HlcAppContext* ctx) {
    if (!ctx) return;
    ctx->auth_seen = false;
    ctx->auth_ongoing_sent = false;
    ctx->precharge_seen = false;
    ctx->precharge_count = 0;
    clear_last_bms_request();
}

int hlc_handle_authorization(HlcAppContext* ctx,
                             const jpv2g_secc_request_t* req,
                             uint8_t* out,
                             size_t out_len,
                             size_t* written) {
    if (!ctx || !ctx->secc || !req) return -EINVAL;
    ctx->auth_seen = true;
    const uint8_t* sid = ctx->secc->session_id;

    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_responseCodeType rc = iso2_responseCodeType_OK;
        iso2_EVSEProcessingType proc = iso2_EVSEProcessingType_Finished;
        if (!g_runtime_cfg.standalone_mode) {
            const uint32_t now_ms = millis();
            const bool hb_ok = controller_heartbeat_alive(now_ms);
            const bool granted = controller_auth_granted(now_ms);
            if (!hb_ok || g_ctrl.auth_state == ControllerAuthState::Denied) {
                rc = iso2_responseCodeType_FAILED;
                proc = iso2_EVSEProcessingType_Finished;
                ctx->auth_ongoing_sent = false;
            } else if (granted) {
                rc = iso2_responseCodeType_OK;
                if (!ctx->auth_ongoing_sent) {
                    proc = iso2_EVSEProcessingType_Ongoing;
                    ctx->auth_ongoing_sent = true;
                } else {
                    proc = iso2_EVSEProcessingType_Finished;
                }
            } else {
                rc = iso2_responseCodeType_OK;
                proc = iso2_EVSEProcessingType_Ongoing;
                ctx->auth_ongoing_sent = false;
            }
        }
        Serial.printf("[HLC][AUTH][ISO] rc=%d proc=%d standalone=%d ctrl_auth=%d hb=%d\n",
                      static_cast<int>(rc),
                      static_cast<int>(proc),
                      g_runtime_cfg.standalone_mode ? 1 : 0,
                      static_cast<int>(g_ctrl.auth_state),
                      controller_heartbeat_alive(millis()) ? 1 : 0);
        return jpv2g_cbv2g_encode_authorization_res(
            sid,
            rc,
            proc,
            out,
            out_len,
            written);
    }

    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        din_responseCodeType rc = din_responseCodeType_OK;
        din_EVSEProcessingType proc = din_EVSEProcessingType_Finished;
        if (!g_runtime_cfg.standalone_mode) {
            const uint32_t now_ms = millis();
            const bool hb_ok = controller_heartbeat_alive(now_ms);
            const bool granted = controller_auth_granted(now_ms);
            if (!hb_ok || g_ctrl.auth_state == ControllerAuthState::Denied) {
                rc = din_responseCodeType_FAILED;
                proc = din_EVSEProcessingType_Finished;
                ctx->auth_ongoing_sent = false;
            } else if (granted) {
                rc = din_responseCodeType_OK;
                if (!ctx->auth_ongoing_sent) {
                    proc = din_EVSEProcessingType_Ongoing;
                    ctx->auth_ongoing_sent = true;
                } else {
                    proc = din_EVSEProcessingType_Finished;
                }
            } else {
                rc = din_responseCodeType_OK;
                proc = din_EVSEProcessingType_Ongoing;
                ctx->auth_ongoing_sent = false;
            }
        }
        Serial.printf("[HLC][AUTH][DIN] rc=%d proc=%d standalone=%d ctrl_auth=%d hb=%d\n",
                      static_cast<int>(rc),
                      static_cast<int>(proc),
                      g_runtime_cfg.standalone_mode ? 1 : 0,
                      static_cast<int>(g_ctrl.auth_state),
                      controller_heartbeat_alive(millis()) ? 1 : 0);
        return jpv2g_cbv2g_encode_din_contract_authentication_res(
            sid,
            rc,
            proc,
            out,
            out_len,
            written);
    }

    return jpv2g_secc_default_handle(ctx->secc, JPV2G_AUTHORIZATION_REQ, req, out, out_len, written);
}

int hlc_handle_precharge(HlcAppContext* ctx,
                         const jpv2g_secc_request_t* req,
                         uint8_t* out,
                         size_t out_len,
                         size_t* written) {
    if (!ctx || !ctx->secc || !req) return -EINVAL;

    float req_v = g_last_module_target_v;
    float req_i = PRECHARGE_CURRENT_LIMIT_A;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2 && req->body) {
        const auto* rq = static_cast<const iso2_PreChargeReqType*>(req->body);
        req_v = iso_pv_to_float(&rq->EVTargetVoltage);
        req_i = std::min(PRECHARGE_CURRENT_LIMIT_A, iso_pv_to_float(&rq->EVTargetCurrent));
    } else if (req->protocol == JPV2G_PROTOCOL_DIN70121 && req->body) {
        const auto* rq = static_cast<const din_PreChargeReqType*>(req->body);
        req_v = din_pv_to_float(&rq->EVTargetVoltage);
        req_i = std::min(PRECHARGE_CURRENT_LIMIT_A, din_pv_to_float(&rq->EVTargetCurrent));
    }
    req_i = sane_non_negative(req_i);
    req_v = sane_non_negative(req_v);
    const bool allow_energy = controller_allows_energy(millis());

    if (!ctx->precharge_seen) {
        Serial.println("[HLC] PreChargeReq received");
    }
    ctx->precharge_seen = true;
    ctx->precharge_count++;
    update_last_bms_request(req_v, req_i, 1u, false, true);

    if (allow_energy) {
        (void)relay1_set(true, "PreCharge");
        (void)modules_apply_target(true, req_v, req_i, "PreCharge");
    } else {
        request_modules_off("PreChargeBlocked");
        (void)relay1_set(false, "PreChargeBlocked");
        req_i = 0.0f;
    }

    cbmodules::GroupStatus st{};
    (void)snapshot_group_status(&st);
    float present_v = sane_non_negative(st.combined_voltage_v);
    if (present_v <= 0.1f) {
        present_v = sane_non_negative(g_last_measured_v);
    }

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_PreChargeResType res;
        init_iso2_PreChargeResType(&res);
        if (allow_energy) {
            set_iso_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_iso_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        set_iso_physical(&res.EVSEPresentVoltage, iso2_unitSymbolType_V, present_v);
        return jpv2g_cbv2g_encode_pre_charge_res(
            sid,
            iso2_responseCodeType_OK,
            &res,
            out,
            out_len,
            written);
    }

    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        din_PreChargeResType res;
        init_din_PreChargeResType(&res);
        if (allow_energy) {
            set_din_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_din_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        set_din_physical(&res.EVSEPresentVoltage, din_unitSymbolType_V, present_v);
        return jpv2g_cbv2g_encode_din_pre_charge_res(
            sid,
            din_responseCodeType_OK,
            &res,
            out,
            out_len,
            written);
    }

    return -ENOTSUP;
}

int hlc_handle_current_demand(HlcAppContext* ctx,
                              const jpv2g_secc_request_t* req,
                              uint8_t* out,
                              size_t out_len,
                              size_t* written) {
    if (!ctx || !ctx->secc || !req) return -EINVAL;

    float req_v = g_last_module_target_v;
    float req_i = g_last_module_target_i;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2 && req->body) {
        const auto* rq = static_cast<const iso2_CurrentDemandReqType*>(req->body);
        req_v = iso_pv_to_float(&rq->EVTargetVoltage);
        req_i = iso_pv_to_float(&rq->EVTargetCurrent);
    } else if (req->protocol == JPV2G_PROTOCOL_DIN70121 && req->body) {
        const auto* rq = static_cast<const din_CurrentDemandReqType*>(req->body);
        req_v = din_pv_to_float(&rq->EVTargetVoltage);
        req_i = din_pv_to_float(&rq->EVTargetCurrent);
    }

    req_v = sane_non_negative(req_v);
    req_i = sane_non_negative(req_i);
    const bool allow_energy = controller_allows_energy(millis());
    const bool enable = allow_energy && (req_i > 0.05f);
    update_last_bms_request(req_v, req_i, 2u, enable, true);
    (void)relay1_set(enable, enable ? "CurrentDemandStart" : "CurrentDemandStop");
    (void)modules_apply_target(enable, req_v, req_i, allow_energy ? "CurrentDemand" : "CurrentDemandBlocked");

    cbmodules::GroupStatus st{};
    (void)snapshot_group_status(&st);
    float present_v = sane_non_negative(st.combined_voltage_v);
    float present_i = sane_non_negative(st.combined_current_a);
    if (present_v <= 0.1f) present_v = sane_non_negative(g_last_measured_v);
    if (present_i <= 0.05f) present_i = sane_non_negative(g_last_measured_i);
    if (!enable) present_i = 0.0f;

    const float req_power_kw = (req_v * req_i) / 1000.0f;
    const float avail_i = sane_non_negative(st.available_current_a);
    const float avail_p = sane_non_negative(st.available_power_kw);
    const bool current_limited = st.saturated || (avail_i > 0.0f && req_i > (avail_i + 0.1f));
    const bool power_limited = st.saturated || (avail_p > 0.0f && req_power_kw > (avail_p + 0.1f));
    const bool voltage_limited = req_v > (MODULE_MAX_VOLTAGE_V + 0.5f);

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_CurrentDemandResType res;
        init_iso2_CurrentDemandResType(&res);
        if (allow_energy) {
            set_iso_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_iso_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        set_iso_physical(&res.EVSEPresentVoltage, iso2_unitSymbolType_V, present_v);
        set_iso_physical(&res.EVSEPresentCurrent, iso2_unitSymbolType_A, present_i);
        res.EVSECurrentLimitAchieved = current_limited ? 1 : 0;
        res.EVSEVoltageLimitAchieved = voltage_limited ? 1 : 0;
        res.EVSEPowerLimitAchieved = power_limited ? 1 : 0;
        fill_iso_meter_info_real(&res.MeterInfo);
        res.MeterInfo_isUsed = 1;
        res.EVSEID.charactersLen = 0;
        return jpv2g_cbv2g_encode_current_demand_res(
            sid,
            iso2_responseCodeType_OK,
            &res,
            out,
            out_len,
            written);
    }

    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        din_CurrentDemandResType res;
        init_din_CurrentDemandResType(&res);
        if (allow_energy) {
            set_din_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_din_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        set_din_physical(&res.EVSEPresentVoltage, din_unitSymbolType_V, present_v);
        set_din_physical(&res.EVSEPresentCurrent, din_unitSymbolType_A, present_i);
        res.EVSECurrentLimitAchieved = current_limited ? 1 : 0;
        res.EVSEVoltageLimitAchieved = voltage_limited ? 1 : 0;
        res.EVSEPowerLimitAchieved = power_limited ? 1 : 0;
        return jpv2g_cbv2g_encode_din_current_demand_res(
            sid,
            din_responseCodeType_OK,
            &res,
            out,
            out_len,
            written);
    }

    return -ENOTSUP;
}

void hlc_apply_power_side_effects(HlcAppContext* ctx,
                                  jpv2g_message_type_t type,
                                  const jpv2g_secc_request_t* req) {
    if (!ctx || !req) return;

    if (type == JPV2G_POWER_DELIVERY_REQ) {
        bool enable = true;
        if (req->protocol == JPV2G_PROTOCOL_ISO15118_2 && req->body) {
            const auto* rq = static_cast<const iso2_PowerDeliveryReqType*>(req->body);
            enable = (rq->ChargeProgress != iso2_chargeProgressType_Stop);
        } else if (req->protocol == JPV2G_PROTOCOL_DIN70121 && req->body) {
            const auto* rq = static_cast<const din_PowerDeliveryReqType*>(req->body);
            enable = (rq->ReadyToChargeState != 0);
        }
        if (!controller_allows_energy(millis())) {
            enable = false;
        }
        if (!enable) {
            update_last_bms_request(g_last_module_target_v, 0.0f, 3u, false, true);
        }
        (void)relay1_set(enable, enable ? "PowerDeliveryStart" : "PowerDeliveryStop");
        const float hold_i = enable ? std::max(PRECHARGE_CURRENT_LIMIT_A, g_last_module_target_i) : 0.0f;
        (void)modules_apply_target(enable, g_last_module_target_v, hold_i, "PowerDelivery");
        return;
    }

    if (type == JPV2G_SESSION_STOP_REQ) {
        clear_last_bms_request();
        request_modules_off("SessionStop");
        (void)relay1_set(false, "SessionStop");
    }
}

int hlc_handle_request(jpv2g_message_type_t type,
                       const void* decoded,
                       uint8_t* out,
                       size_t out_len,
                       size_t* written,
                       void* user_ctx) {
    HlcAppContext* ctx = static_cast<HlcAppContext*>(user_ctx);
    if (!ctx || !ctx->secc || !decoded) return -EINVAL;
    const jpv2g_secc_request_t* req = static_cast<const jpv2g_secc_request_t*>(decoded);
    update_last_ev_soc_from_request(type, req);
    if (type == JPV2G_SESSION_SETUP_REQ && req->body) {
        if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
            const auto* rq = static_cast<const iso2_SessionSetupReqType*>(req->body);
            controller_publish_evccid(rq->EVCCID.bytes, rq->EVCCID.bytesLen);
        } else if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
            const auto* rq = static_cast<const din_SessionSetupReqType*>(req->body);
            controller_publish_evccid(rq->EVCCID.bytes, rq->EVCCID.bytesLen);
        }
    }
    if (type == JPV2G_AUTHORIZATION_REQ) {
        return hlc_handle_authorization(ctx, req, out, out_len, written);
    }
    if (type == JPV2G_PRE_CHARGE_REQ) {
        return hlc_handle_precharge(ctx, req, out, out_len, written);
    }
    if (type == JPV2G_CURRENT_DEMAND_REQ) {
        return hlc_handle_current_demand(ctx, req, out, out_len, written);
    }
    const int rc = jpv2g_secc_default_handle(ctx->secc, type, req, out, out_len, written);
    if (rc == 0) {
        hlc_apply_power_side_effects(ctx, type, req);
    }
    return rc;
}

void hlc_worker_task_main(void* arg) {
    (void)arg;
    for (;;) {
        int client_fd = -1;
        if (!g_hlc_client_queue) {
            vTaskDelay(pdMS_TO_TICKS(100));
            continue;
        }
        if (xQueueReceive(g_hlc_client_queue, &client_fd, pdMS_TO_TICKS(1000)) != pdTRUE) {
            continue;
        }

        if (client_fd < 0) {
            continue;
        }

        if (!g_hlc_ready) {
            jpv2g_socket_close(client_fd);
            continue;
        }

        g_hlc_active = true;
        hlc_reset_session_state(&g_hlc_ctx);
        Serial.println("[HLC] EVCC connected, processing session");
        g_hlc_active_client_fd = client_fd;
        const int rc = jpv2g_secc_handle_client_detect(&g_secc, client_fd, HLC_FIRST_PACKET_TIMEOUT_MS, HLC_IDLE_TIMEOUT_MS);
        if (g_hlc_active_client_fd == client_fd) {
            g_hlc_active_client_fd = -1;
        }
        jpv2g_socket_close(client_fd);
        g_hlc_active = false;

        clear_last_bms_request();
        request_modules_off("ClientSessionDone");
        (void)relay1_set(false, "ClientSessionDone");

        if (g_hlc_ctx.precharge_seen) {
            Serial.printf("[HLC] precharge reached, count=%lu\n", static_cast<unsigned long>(g_hlc_ctx.precharge_count));
        }
        Serial.printf("[HLC] client session done rc=%d\n", rc);
    }
}

bool ensure_hlc_worker_ready() {
    if (!g_hlc_client_queue) {
        g_hlc_client_queue = xQueueCreate(HLC_CLIENT_QUEUE_DEPTH, sizeof(int));
        if (!g_hlc_client_queue) {
            Serial.println("[HLC] queue create failed");
            return false;
        }
    }
    if (!g_hlc_worker_task) {
        const BaseType_t ok = xTaskCreatePinnedToCore(
            hlc_worker_task_main,
            "hlc_worker",
            HLC_TASK_STACK_WORDS,
            nullptr,
            2,
            &g_hlc_worker_task,
            ARDUINO_RUNNING_CORE);
        if (ok != pdPASS || !g_hlc_worker_task) {
            Serial.println("[HLC] worker task create failed");
            return false;
        }
        Serial.printf("[HLC] worker task started stack_bytes=%lu\n", static_cast<unsigned long>(HLC_TASK_STACK_WORDS));
    }
    return true;
}

void stop_hlc_stack() {
    if (g_hlc_active_client_fd >= 0) {
        const int fd = g_hlc_active_client_fd;
        g_hlc_active_client_fd = -1;
        jpv2g_socket_close(fd);
    }
    if (g_hlc_client_queue) {
        int queued_fd = -1;
        while (xQueueReceive(g_hlc_client_queue, &queued_fd, 0) == pdTRUE) {
            if (queued_fd >= 0) {
                jpv2g_socket_close(queued_fd);
            }
        }
    }
    if (g_hlc_ready) {
        jpv2g_secc_stop(&g_secc);
        if (g_codec) {
            jpv2g_codec_free(g_codec);
            g_codec = nullptr;
        }
        memset(&g_secc, 0, sizeof(g_secc));
    } else {
        if (g_codec) {
            jpv2g_codec_free(g_codec);
            g_codec = nullptr;
        }
    }
    memset(&g_hlc_ctx, 0, sizeof(g_hlc_ctx));
    g_hlc_ready = false;
    g_hlc_active = false;
    request_modules_off("StopHlc");
    (void)relay1_set(false, "StopHlc");
}

bool init_hlc_stack() {
    if (g_hlc_ready) return true;
    ensure_lwip_socket_stack_ready();
    if (!g_plc_netif_ready && !init_plc_lwip_netif()) {
        Serial.println("[HLC] PLC netif unavailable");
        return false;
    }

    int rc = jpv2g_codec_init(&g_codec);
    if (rc != 0 || !g_codec) {
        Serial.printf("[HLC] codec init failed rc=%d\n", rc);
        return false;
    }

    jpv2g_secc_config_t cfg;
    jpv2g_secc_config_default(&cfg);
    cfg.network_interface[0] = '\0';
    cfg.use_tls = false;
    cfg.tcp_port = 15118;
    cfg.tls_port = 15118;
    Serial.printf("[HLC] bind iface=%s (plc=%s)\n",
                  cfg.network_interface[0] ? cfg.network_interface : "<default>",
                  g_plc_ifname[0] ? g_plc_ifname : "<none>");

    rc = jpv2g_secc_init(&g_secc, &cfg, g_codec);
    if (rc != 0) {
        Serial.printf("[HLC] secc init failed rc=%d\n", rc);
        jpv2g_codec_free(g_codec);
        g_codec = nullptr;
        return false;
    }
    jpv2g_secc_set_decoded_logs(ENABLE_DECODED_LOGS);

    g_hlc_ctx.secc = &g_secc;
    hlc_reset_session_state(&g_hlc_ctx);
    g_secc.handle_request = hlc_handle_request;
    g_secc.user_ctx = &g_hlc_ctx;
    if (!ensure_hlc_worker_ready()) {
        stop_hlc_stack();
        return false;
    }

    rc = jpv2g_secc_start_udp(&g_secc);
    if (rc != 0) {
        Serial.printf("[HLC] SDP UDP start failed rc=%d\n", rc);
        stop_hlc_stack();
        return false;
    }
    rc = jpv2g_secc_start_tcp(&g_secc);
    if (rc != 0) {
        Serial.printf("[HLC] TCP start failed rc=%d\n", rc);
        stop_hlc_stack();
        return false;
    }

    g_hlc_ready = true;
    g_next_hlc_wait_log_ms = millis();
    Serial.println("[HLC] stack ready (SDP+TCP on 15118)");
    return true;
}

void service_sdp_once() {
    if (!g_hlc_ready) return;

    uint8_t in_buf[256];
    struct sockaddr_in6 from;
    socklen_t from_len = sizeof(from);
    int rc = jpv2g_udp_server_recv(&g_secc.udp, in_buf, sizeof(in_buf), &from, &from_len, 0);
    if (rc <= 0) {
        return;
    }

    jpv2g_v2gtp_t msg;
    if (jpv2g_v2gtp_parse(in_buf, static_cast<size_t>(rc), &msg) != 0) return;
    if (msg.payload_type != JPV2G_PAYLOAD_SDP_REQ) return;

    jpv2g_sdp_req_t req;
    if (jpv2g_sdp_req_decode(msg.payload, msg.payload_length, &req) != 0) return;
    if (req.transport_protocol != JPV2G_TRANSPORT_TCP) return;

    jpv2g_sdp_res_t res;
    memset(&res, 0, sizeof(res));
    memcpy(res.secc_ip, g_secc_ip, sizeof(res.secc_ip));
    res.secc_port = static_cast<uint16_t>(g_secc.cfg.tcp_port);
    res.security = g_secc.cfg.use_tls ? JPV2G_SECURITY_WITH_TLS : JPV2G_SECURITY_WITHOUT_TLS;
    res.transport_protocol = JPV2G_TRANSPORT_TCP;

    uint8_t payload[32];
    size_t payload_len = 0;
    if (jpv2g_sdp_res_encode(&res, payload, sizeof(payload), &payload_len) != 0) return;

    uint8_t out[64];
    size_t out_len = 0;
    if (jpv2g_v2gtp_build(JPV2G_PAYLOAD_SDP_RES, payload, payload_len, out, sizeof(out), &out_len) != 0) return;
    (void)jpv2g_udp_server_sendto(&g_secc.udp, out, out_len, &from, from_len);

    Serial.printf("[SDP] req sec=0x%02X tp=0x%02X -> res port=%u sec=0x%02X\n",
                  req.security, req.transport_protocol, res.secc_port, res.security);
}

void service_hlc_tcp_once() {
    if (!g_hlc_ready) return;

    int client_fd = -1;
    struct sockaddr_in6 client_addr;
    socklen_t client_len = sizeof(client_addr);
    int rc = jpv2g_tcp_server_accept(&g_secc.tcp, &client_fd, &client_addr, &client_len, 0);
    if (rc == -EAGAIN || rc == -ETIMEDOUT) {
        const uint32_t now = millis();
        if (static_cast<int32_t>(now - g_next_hlc_wait_log_ms) >= 0) {
            Serial.println("[HLC] waiting for EVCC TCP client...");
            g_next_hlc_wait_log_ms = now + 5000;
        }
        return;
    }
    if (rc != 0) {
        Serial.printf("[HLC] accept failed rc=%d\n", rc);
        return;
    }

    if (!g_hlc_client_queue) {
        Serial.println("[HLC] client queue unavailable");
        jpv2g_socket_close(client_fd);
        return;
    }
    if (xQueueSend(g_hlc_client_queue, &client_fd, 0) != pdTRUE) {
        Serial.println("[HLC] client queue full, dropping session");
        jpv2g_socket_close(client_fd);
        return;
    }
    Serial.println("[HLC] EVCC connected, handed off to worker");
}

void apply_cp_output(char state, uint32_t now_ms) {
    const bool connected = cp_connected(state);
    const bool connected_hold =
        (!connected && g_cp_last_seen_connected_ms != 0 &&
         static_cast<int32_t>(now_ms - g_cp_last_seen_connected_ms) <= static_cast<int32_t>(CP_CONNECTED_HOLD_MS));
    const bool pwm_connected = connected || connected_hold;
    if (g_ef_pulse_active && static_cast<int32_t>(now_ms - g_ef_pulse_until_ms) >= 0) {
        g_ef_pulse_active = false;
    }

    const bool hold_active = static_cast<int32_t>(now_ms - g_slac_hold_until_ms) < 0;
    const bool controller_permits_pwm =
        g_runtime_cfg.standalone_mode || g_session_started || g_hlc_active || controller_allows_slac_start(now_ms);
    const bool pwm_ok = pwm_connected && !hold_active && !g_ef_pulse_active && controller_permits_pwm;
    uint16_t pct = g_ef_pulse_active ? 0u : (pwm_ok ? 5u : 100u);
    if (pct != g_last_cp_duty_pct) {
        Serial.printf("[CP] duty -> %u%% (phase=%s state=%c hold=%d ef=%d conn_hold=%d ctrl_pwm=%d)\n",
                      pct,
                      cp_phase_label(state, pct),
                      state,
                      hold_active ? 1 : 0,
                      g_ef_pulse_active ? 1 : 0,
                      connected_hold ? 1 : 0,
                      controller_permits_pwm ? 1 : 0);
        g_last_cp_duty_pct = pct;
    }
    write_ledc_duty(cp_pct_to_duty(g_last_cp_duty_pct));
}

void start_session(uint32_t now_ms) {
    if (!g_fsm) {
        return;
    }
    g_fsm->start(now_ms);
    g_session_started = true;
    g_bcd_entered = false;
    g_session_matched = false;
    g_last_fsm_state = g_fsm->get_state();
    Serial.println("[SLAC] session start");
}

void stop_session(uint32_t now_ms) {
    if (g_fsm && g_session_started) {
        g_fsm->leave_bcd(now_ms);
    }
    g_session_started = false;
    g_bcd_entered = false;
    g_session_matched = false;
    g_last_fsm_state = slac::evse::State::Reset;
    g_last_ev_soc_pct = -1;
}

void enter_hold_with_optional_ef_pulse(uint32_t now_ms, const char* reason) {
    if (g_slac_failures_this_cp < 0xFF) {
        g_slac_failures_this_cp++;
    }
    if (EF_PULSE_AFTER_FAILURES > 0 && g_slac_failures_this_cp >= EF_PULSE_AFTER_FAILURES) {
        g_ef_pulse_active = true;
        g_ef_pulse_until_ms = now_ms + CP_EF_PULSE_MS;
        Serial.printf("[CP] E/F pulse for %lu ms\n", static_cast<unsigned long>(CP_EF_PULSE_MS));
    }
    g_slac_hold_until_ms = now_ms + SLAC_HOLD_MS;
    stop_session(now_ms);
    Serial.printf("[SLAC] enter hold (%s) %lu ms\n", reason ? reason : "-", static_cast<unsigned long>(SLAC_HOLD_MS));
}

bool try_init_qca_and_fsm(uint32_t now_ms) {
    if (!g_transport) {
        g_transport = std::make_shared<Qca7000Transport>();
    }

    if (!g_transport->begin()) {
        Serial.printf("[QCA] init failed: %s\n", g_transport->get_error().c_str());
        g_next_qca_init_ms = now_ms + QCA_INIT_RETRY_MS;
        return false;
    }
    Serial.println("[QCA] transport ready");

    g_transport->modem_reset();
    delay(150);

    if (!g_channel.open(g_transport, g_local_mac)) {
        Serial.printf("[SLAC] channel open failed: %s\n", g_channel.get_error().c_str());
        g_next_qca_init_ms = now_ms + QCA_INIT_RETRY_MS;
        return false;
    }

    if (!g_plc_netif_ready) {
        (void)init_plc_lwip_netif();
    }

    g_fsm = std::make_unique<slac::evse::EvseFsm>(g_channel, [](const std::string& msg) {
        Serial.printf("[FSM] %s\n", msg.c_str());
    });

    const uint8_t plc_peer_mac[ETH_ALEN] = {
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[0] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[0] >> 0) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[1] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[1] >> 0) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[2] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[2] >> 0) & 0xFF),
    };
    g_fsm->set_plc_peer_mac(plc_peer_mac);
    g_next_qca_init_ms = 0;
    Serial.println("[APP] ready, waiting for CP B/C/D");
    return true;
}

void process_cp_and_fsm(uint32_t now_ms) {
    const int cp_mv = read_cp_mv_robust();
    g_last_cp_mv = cp_mv;
    const char raw_cp_state = cp_state_from_mv(cp_mv);
    if (cp_connected(raw_cp_state)) {
        g_cp_last_seen_connected_ms = now_ms;
    }
    char filtered_cp_state = raw_cp_state;
    const bool was_connected_state = cp_connected(g_cp_state);
    if (was_connected_state && !cp_connected(raw_cp_state) && g_cp_last_seen_connected_ms != 0) {
        const bool within_hold =
            static_cast<int32_t>(now_ms - g_cp_last_seen_connected_ms) <= static_cast<int32_t>(CP_CONNECTED_HOLD_MS);
        if (within_hold) {
            filtered_cp_state = g_cp_state;
            if (raw_cp_state == 'A' &&
                (g_cp_next_spike_log_ms == 0 || static_cast<int32_t>(now_ms - g_cp_next_spike_log_ms) >= 0)) {
                Serial.printf("[CP] hold A-disconnect spike raw=%c stable=%c mv=%d hold_ms=%lu\n",
                              raw_cp_state,
                              g_cp_state,
                              cp_mv,
                              static_cast<unsigned long>(CP_CONNECTED_HOLD_MS));
                g_cp_next_spike_log_ms = now_ms + 10000;
            }
        }
    }
    if (filtered_cp_state != g_cp_state_candidate) {
        g_cp_state_candidate = filtered_cp_state;
        g_cp_candidate_since_ms = now_ms;
    }
    if (g_cp_state_candidate != g_cp_state) {
        const bool was_connected = cp_connected(g_cp_state);
        const bool candidate_connected = cp_connected(g_cp_state_candidate);
        const uint32_t debounce_ms =
            (was_connected && !candidate_connected) ? CP_DISCONNECT_DEBOUNCE_MS : CP_STATE_DEBOUNCE_MS;
        if (static_cast<int32_t>(now_ms - g_cp_candidate_since_ms) >= static_cast<int32_t>(debounce_ms)) {
            g_cp_state = g_cp_state_candidate;
        }
    }
    const bool connected = cp_connected(g_cp_state);

    if (g_cp_state != g_last_cp_state) {
        Serial.printf("[CP] state %c -> %c (%d mV)\n", g_last_cp_state, g_cp_state, cp_mv);
        const bool old_connected = cp_connected(g_last_cp_state);
        if (connected && !old_connected) {
            g_cp_connected_since_ms = now_ms;
            g_cp_last_seen_connected_ms = now_ms;
            g_slac_failures_this_cp = 0;
            g_slac_hold_until_ms = 0;
        } else if (!connected && old_connected) {
            g_cp_connected_since_ms = 0;
            g_cp_last_seen_connected_ms = 0;
            g_slac_hold_until_ms = 0;
            g_slac_failures_this_cp = 0;
            g_ctrl.slac_start_latched = false;
            stop_session(now_ms);
            stop_hlc_stack();
            request_modules_off("CPDisconnect");
            (void)relay1_set(false, "CPDisconnect");
        }
        g_last_cp_state = g_cp_state;
    }

    apply_cp_output(g_cp_state, now_ms);

    if (!connected) {
        return;
    }

    const bool slac_start_ok = controller_allows_slac_start(now_ms);
    if (!g_session_started && g_cp_connected_since_ms != 0 &&
        (now_ms - g_cp_connected_since_ms) >= CP_STABLE_MS &&
        static_cast<int32_t>(now_ms - g_slac_hold_until_ms) >= 0 &&
        slac_start_ok) {
        start_session(now_ms);
        if (!g_runtime_cfg.standalone_mode) {
            g_ctrl.slac_start_latched = false;
        }
    } else if (!g_runtime_cfg.standalone_mode && !g_session_started &&
               g_cp_connected_since_ms != 0 && !slac_start_ok) {
        static uint32_t next_wait_log_ms = 0;
        if (next_wait_log_ms == 0 || static_cast<int32_t>(now_ms - next_wait_log_ms) >= 0) {
            Serial.printf("[CTRL] waiting SLAC start auth (hb=%d armed=%d start=%d)\n",
                          controller_heartbeat_alive(now_ms) ? 1 : 0,
                          g_ctrl.slac_armed ? 1 : 0,
                          g_ctrl.slac_start_latched ? 1 : 0);
            next_wait_log_ms = now_ms + 2000;
        }
    }

    if (!g_session_started || !g_fsm) {
        return;
    }

    for (int i = 0; i < 4; ++i) {
        const int timeout_ms = (i == 0) ? 2 : 0;
        const bool got = g_fsm->poll_channel_once(timeout_ms, now_ms);
        if (!got) break;
    }
    g_fsm->poll(now_ms);

    // The EVSE FSM requires an explicit LinkDetected event after CM_SLAC_MATCH.REQ.
    // In this MCU test app, use "match request received" as the trigger source.
    if (g_fsm->get_state() == slac::evse::State::WaitForSlacMatch && g_fsm->received_slac_match()) {
        Serial.println("[SLAC] match request received, signaling link detected");
        g_fsm->notify_link_detected(now_ms);
    }

    const slac::evse::State s = g_fsm->get_state();
    if (s != g_last_fsm_state) {
        Serial.printf("[FSM] %s -> %s\n", evse_state_name(g_last_fsm_state), evse_state_name(s));
        g_last_fsm_state = s;
    }

    if (!g_bcd_entered && s == slac::evse::State::Idle) {
        g_fsm->enter_bcd(now_ms);
        g_bcd_entered = true;
        Serial.println("[FSM] EnterBCD");
    }

    if (s == slac::evse::State::Matched && !g_session_matched) {
        g_session_matched = true;
        uint8_t ev_mac[ETH_ALEN]{};
        if (g_fsm->get_ev_mac(ev_mac)) {
            Serial.printf("[SLAC] EV_MAC %02X:%02X:%02X:%02X:%02X:%02X\n",
                          ev_mac[0], ev_mac[1], ev_mac[2], ev_mac[3], ev_mac[4], ev_mac[5]);
            controller_publish_ev_mac_if_changed(ev_mac);
        }
        Serial.println("[SLAC] MATCHED - starting HLC stack");
    } else if (s == slac::evse::State::MatchingFailed || s == slac::evse::State::NoSlacPerformed) {
        enter_hold_with_optional_ef_pulse(now_ms, "fsm terminal failure");
        stop_hlc_stack();
        request_modules_off("SlacFailure");
        (void)relay1_set(false, "SlacFailure");
    }

    if (g_session_matched && !g_hlc_ready) {
        (void)init_hlc_stack();
    }

    if (g_session_matched && g_hlc_ready) {
        service_sdp_once();
        service_hlc_tcp_once();
    }
}

} // namespace

void setup() {
    Serial.begin(115200);
    delay(200);

    Serial.println();
    Serial.println("cbslac + jpv2g ESP32-S3 SLAC/HLC (to PreCharge)");

    (void)load_runtime_config_from_nvs();
    refresh_runtime_identity_cache();
    print_runtime_config();

    esp_read_mac(g_local_mac, ESP_MAC_WIFI_STA);
    Serial.printf("[NET] local MAC %02X:%02X:%02X:%02X:%02X:%02X\n",
                  g_local_mac[0], g_local_mac[1], g_local_mac[2], g_local_mac[3], g_local_mac[4], g_local_mac[5]);

    (void)relay1_init();
    (void)relay1_set(false, "Boot");
    (void)relay2_set(false, "Boot");
    (void)relay3_set(false, "Boot");
    launch_sw4_setup_portal_if_requested();

    Serial.printf("[BOOT] startup log hold %lu ms\n", static_cast<unsigned long>(STARTUP_LOG_DELAY_MS));
    const uint32_t hold_start = millis();
    uint32_t next_tick = hold_start + 1000;
    while ((millis() - hold_start) < STARTUP_LOG_DELAY_MS) {
        const uint32_t now = millis();
        if (static_cast<int32_t>(now - next_tick) >= 0) {
            const uint32_t elapsed = now - hold_start;
            Serial.printf("[BOOT] waiting... %lu/%lu ms\n",
                          static_cast<unsigned long>(elapsed),
                          static_cast<unsigned long>(STARTUP_LOG_DELAY_MS));
            next_tick += 1000;
        }
        delay(10);
    }

    build_link_local_from_mac(g_local_mac, g_secc_ip);
    Serial.printf("[NET] SECC IPv6 LL fe80::%02x%02x:%02xff:fe%02x:%02x%02x\n",
                  static_cast<unsigned>(g_secc_ip[8]),
                  static_cast<unsigned>(g_secc_ip[9]),
                  static_cast<unsigned>(g_secc_ip[10]),
                  static_cast<unsigned>(g_secc_ip[13]),
                  static_cast<unsigned>(g_secc_ip[14]),
                  static_cast<unsigned>(g_secc_ip[15]));

    analogReadResolution(12);
    analogSetPinAttenuation(CP_1_READ_PIN, ADC_11db);
    ledcSetup(CP_1_PWM_CHANNEL, CP_1_PWM_FREQUENCY, CP_1_PWM_RESOLUTION);
    ledcAttachPin(CP_1_PWM_PIN, CP_1_PWM_CHANNEL);
    g_last_cp_duty_pct = 100;
    write_ledc_duty(cp_pct_to_duty(g_last_cp_duty_pct));

    g_module_allowlist = {runtime_local_module_id()};
    (void)init_module_manager();
    if (!g_runtime_cfg.standalone_mode) {
        g_module_allowlist.clear();
        Serial.println("[CTRL] controller mode active: starting with empty allowlist");
    }
    request_modules_off("Boot");
    controller_publish_local_identity_once();

    (void)try_init_qca_and_fsm(millis());
}

void loop() {
    uint32_t now_ms = millis();
    service_serial_commands();
    if (!g_fsm && static_cast<int32_t>(now_ms - g_next_qca_init_ms) >= 0) {
        (void)try_init_qca_and_fsm(now_ms);
    }
    if (g_transport && g_transport->is_ready()) {
        g_transport->service_ingress_once();
    }
    controller_poll_can_raw_once();
    controller_service_rx();
    // Re-sample time after controller RX so same-iteration heartbeats do not
    // look "from the future" to the watchdog/session gate logic.
    now_ms = millis();
    controller_service_watchdogs(now_ms);
    process_cp_and_fsm(now_ms);
    service_lwip_tx_queue_once();
    service_module_manager_once(now_ms);
    controller_service_periodic_tx(now_ms);
    const bool hlc_priority = g_session_started || g_hlc_active || cp_connected(g_cp_state);
    delay(hlc_priority ? 1 : 5);
}
