#include <Arduino.h>
#include <MFRC522.h>
#include <SPI.h>
#include <Wire.h>
#include <esp_system.h>
#include <Preferences.h>
#include <WebServer.h>
#include <freertos/FreeRTOS.h>
#include <freertos/queue.h>
#include <freertos/semphr.h>
#include <freertos/task.h>
#include <WiFi.h>
#include <esp_heap_caps.h>
#include <esp_task_wdt.h>
#include <esp_timer.h>
#include <soc/soc_memory_types.h>

#ifndef CBPLC_ENABLE_STATUS_LEDS
#define CBPLC_ENABLE_STATUS_LEDS 1
#endif

#ifndef FASTLED_INTERNAL
#define FASTLED_INTERNAL
#endif
#include <FastLED.h>

#include <array>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <map>
#include <memory>
#include <set>
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
// Keep the plc_firmware wrapper more tolerant than the shared cbslac core.
// The inner FSM can wait 40 s for CM_SLAC_PARAM.REQ, so the outer watchdog
// must not abort a healthy session first.
constexpr uint32_t SLAC_HOLD_MS = 10000;
constexpr uint32_t SLAC_PROGRESS_TIMEOUT_MS = 45000;
constexpr uint32_t SLAC_INIT_PROGRESS_TIMEOUT_MS = 130000;
constexpr uint32_t SLAC_SOFT_RETRY_HOLD_MS = 1200;
constexpr uint32_t SLAC_SOFT_RETRY_ELAPSED_MAX_MS = 3000;
constexpr uint8_t SLAC_SOFT_RETRY_LIMIT = 2;
constexpr uint32_t CP_EF_PULSE_MS = 180;
constexpr uint8_t EF_PULSE_AFTER_FAILURES = 3;
constexpr uint32_t STARTUP_LOG_DELAY_MS = 10000;
constexpr uint32_t QCA_INIT_RETRY_MS = 1000;
constexpr int HLC_FIRST_PACKET_TIMEOUT_MS = 20000;
constexpr int HLC_IDLE_TIMEOUT_MS = 60000;
constexpr uint32_t HLC_TASK_STACK_WORDS = 65536;
constexpr UBaseType_t HLC_CLIENT_QUEUE_DEPTH = 2;
constexpr UBaseType_t LWIP_TX_QUEUE_DEPTH = 32;
constexpr uint8_t LWIP_TX_MAX_FRAMES_PER_LOOP = 8u;
constexpr bool ENABLE_DECODED_LOGS = true;

constexpr uint16_t QCA7K_SPI_READ = (1u << 15);
constexpr uint16_t QCA7K_SPI_WRITE = (0u << 15);
constexpr uint16_t QCA7K_SPI_INTERNAL = (1u << 14);
constexpr uint16_t QCA7K_SPI_EXTERNAL = (0u << 14);

constexpr uint16_t SPI_REG_BFR_SIZE = 0x0100;
constexpr uint16_t SPI_REG_WRBUF_SPC_AVA = 0x0200;
constexpr uint16_t SPI_REG_RDBUF_BYTE_AVA = 0x0300;
constexpr uint16_t SPI_REG_SPI_CONFIG = 0x0400;
constexpr uint16_t SPI_REG_INTR_CAUSE = 0x0C00;
constexpr uint16_t SPI_REG_INTR_ENABLE = 0x0D00;
constexpr uint16_t SPI_REG_SIGNATURE = 0x1A00;

constexpr uint16_t QCASPI_SLAVE_RESET_BIT = (1u << 6);
constexpr uint16_t SPI_INT_CPU_ON = (1u << 6);
constexpr uint16_t SPI_INT_WRBUF_ERR = (1u << 2);
constexpr uint16_t SPI_INT_RDBUF_ERR = (1u << 1);
constexpr uint16_t SPI_INT_PKT_AVLBL = (1u << 0);
constexpr uint16_t QCA_INT_ENABLE_MASK =
    static_cast<uint16_t>(SPI_INT_CPU_ON | SPI_INT_WRBUF_ERR | SPI_INT_RDBUF_ERR | SPI_INT_PKT_AVLBL);
constexpr uint16_t QCA_INT_STARTUP_MASK =
    static_cast<uint16_t>(SPI_INT_CPU_ON | SPI_INT_WRBUF_ERR | SPI_INT_RDBUF_ERR | SPI_INT_PKT_AVLBL);
constexpr uint16_t QCASPI_GOOD_SIGNATURE = 0xAA55;

constexpr uint16_t QCA7K_BUFFER_SIZE = 3163;
constexpr uint16_t ETH_FRAME_MIN_LEN_NO_FCS = 60u;
constexpr size_t RX_CHUNK_CAPACITY = 4096;
constexpr size_t RX_QUEUE_CAPACITY = 48;
constexpr uint8_t SLAC_ACTIVE_MAX_FRAMES_PER_LOOP = 12u;
constexpr uint32_t QCA_POLL_FALLBACK_MS = 5u;
constexpr uint32_t QCA_RESET_TIMEOUT_MS = 1000u;
constexpr uint32_t QCA_STARTUP_FORCE_SYNC_MS = 100u;
constexpr uint32_t QCA_STARTUP_IO_WAIT_MS = 500u;
constexpr uint8_t QCA_MAX_BURSTS_PER_POLL = 8u;
constexpr uint16_t RX_PROCESS_BREATHER_STRIDE = 1024u;
constexpr uint8_t FSM_POLL_BREATHER_STRIDE = 1u;
constexpr uint16_t SERIAL_CMD_MAX_BYTES_PER_LOOP = 192u;
constexpr uint16_t SERIAL_CMD_BREATHER_STRIDE = 64u;

constexpr int32_t QCAFRM_ERR_BASE = -1000;
constexpr int32_t QCAFRM_GATHER = 0;
constexpr int32_t QCAFRM_NOHEAD = QCAFRM_ERR_BASE - 1;
constexpr int32_t QCAFRM_NOTAIL = QCAFRM_ERR_BASE - 2;
constexpr int32_t QCAFRM_INVLEN = QCAFRM_ERR_BASE - 3;

enum class QcaFrameDecodeState : uint8_t {
    HwLen0 = 0,
    HwLen1,
    HwLen2,
    HwLen3,
    Header0,
    Header1,
    Header2,
    Header3,
    Len0,
    Len1,
    Reserved0,
    Reserved1,
    Payload,
    Footer0,
    Footer1,
};

enum class CrashBreadcrumbStage : uint16_t {
    Unknown = 0,
    SetupStart,
    SetupAfterStartupHold,
    SetupAfterQcaInit,
    LoopEnter,
    LoopAfterSerial,
    LoopAfterQcaIngress,
    LoopAfterCtrlRx,
    LoopAfterCtrlTx,
    LoopAfterCpFsm,
    LoopAfterLwipTx,
    LoopSleep,
    QcaIngressEnter,
    QcaIngressBeforePoll,
    QcaPollEnter,
    QcaPollAfterMask,
    QcaPollBeforeDrain,
    QcaPollAfterBurstRead,
    QcaPollAfterBurstProcess,
    QcaPollAfterFinish,
    QcaReadBurstEnter,
    QcaReadBurstAfterAvail,
    QcaReadBurstBeforePayload,
    QcaReadBurstAfterPayload,
    QcaWriteEnter,
    QcaWriteWaitWrbuf,
    QcaWriteFrameStart,
    QcaWriteFrameDone,
    QcaProcessRxStart,
    QcaProcessRxDone,
    CtrlAuthHandle,
    CtrlFeedbackHandle,
    FsmPollActive,
};

struct CrashBreadcrumb {
    uint32_t magic;
    uint32_t boot_count;
    uint32_t loop_count;
    uint32_t last_ms;
    uint16_t stage;
    uint16_t detail;
};

constexpr uint32_t CRASH_BREADCRUMB_MAGIC = 0x4352504Du; // CRPM
RTC_NOINIT_ATTR CrashBreadcrumb g_crash_breadcrumb;

volatile bool g_qca_irq_pending = false;

void IRAM_ATTR qca_irq_handler() {
    g_qca_irq_pending = true;
}

void mark_lwip_tx_transport_flush(const char* reason);
void maybe_learn_qca_mac_from_homeplug_frame(const uint8_t* frame, uint16_t len);

const char* crash_breadcrumb_stage_name(CrashBreadcrumbStage stage) {
    switch (stage) {
    case CrashBreadcrumbStage::Unknown: return "Unknown";
    case CrashBreadcrumbStage::SetupStart: return "SetupStart";
    case CrashBreadcrumbStage::SetupAfterStartupHold: return "SetupAfterStartupHold";
    case CrashBreadcrumbStage::SetupAfterQcaInit: return "SetupAfterQcaInit";
    case CrashBreadcrumbStage::LoopEnter: return "LoopEnter";
    case CrashBreadcrumbStage::LoopAfterSerial: return "LoopAfterSerial";
    case CrashBreadcrumbStage::LoopAfterQcaIngress: return "LoopAfterQcaIngress";
    case CrashBreadcrumbStage::LoopAfterCtrlRx: return "LoopAfterCtrlRx";
    case CrashBreadcrumbStage::LoopAfterCtrlTx: return "LoopAfterCtrlTx";
    case CrashBreadcrumbStage::LoopAfterCpFsm: return "LoopAfterCpFsm";
    case CrashBreadcrumbStage::LoopAfterLwipTx: return "LoopAfterLwipTx";
    case CrashBreadcrumbStage::LoopSleep: return "LoopSleep";
    case CrashBreadcrumbStage::QcaIngressEnter: return "QcaIngressEnter";
    case CrashBreadcrumbStage::QcaIngressBeforePoll: return "QcaIngressBeforePoll";
    case CrashBreadcrumbStage::QcaPollEnter: return "QcaPollEnter";
    case CrashBreadcrumbStage::QcaPollAfterMask: return "QcaPollAfterMask";
    case CrashBreadcrumbStage::QcaPollBeforeDrain: return "QcaPollBeforeDrain";
    case CrashBreadcrumbStage::QcaPollAfterBurstRead: return "QcaPollAfterBurstRead";
    case CrashBreadcrumbStage::QcaPollAfterBurstProcess: return "QcaPollAfterBurstProcess";
    case CrashBreadcrumbStage::QcaPollAfterFinish: return "QcaPollAfterFinish";
    case CrashBreadcrumbStage::QcaReadBurstEnter: return "QcaReadBurstEnter";
    case CrashBreadcrumbStage::QcaReadBurstAfterAvail: return "QcaReadBurstAfterAvail";
    case CrashBreadcrumbStage::QcaReadBurstBeforePayload: return "QcaReadBurstBeforePayload";
    case CrashBreadcrumbStage::QcaReadBurstAfterPayload: return "QcaReadBurstAfterPayload";
    case CrashBreadcrumbStage::QcaWriteEnter: return "QcaWriteEnter";
    case CrashBreadcrumbStage::QcaWriteWaitWrbuf: return "QcaWriteWaitWrbuf";
    case CrashBreadcrumbStage::QcaWriteFrameStart: return "QcaWriteFrameStart";
    case CrashBreadcrumbStage::QcaWriteFrameDone: return "QcaWriteFrameDone";
    case CrashBreadcrumbStage::QcaProcessRxStart: return "QcaProcessRxStart";
    case CrashBreadcrumbStage::QcaProcessRxDone: return "QcaProcessRxDone";
    case CrashBreadcrumbStage::CtrlAuthHandle: return "CtrlAuthHandle";
    case CrashBreadcrumbStage::CtrlFeedbackHandle: return "CtrlFeedbackHandle";
    case CrashBreadcrumbStage::FsmPollActive: return "FsmPollActive";
    }
    return "Unknown";
}

inline void note_crash_breadcrumb(CrashBreadcrumbStage stage, uint16_t detail = 0u) {
    g_crash_breadcrumb.magic = CRASH_BREADCRUMB_MAGIC;
    g_crash_breadcrumb.stage = static_cast<uint16_t>(stage);
    g_crash_breadcrumb.detail = detail;
    g_crash_breadcrumb.last_ms = millis();
}

inline void feed_loop_watchdog() {
    if (xTaskGetSchedulerState() != taskSCHEDULER_NOT_STARTED) {
        (void)esp_task_wdt_reset();
    }
}

inline void cooperative_runtime_breather(CrashBreadcrumbStage stage, uint16_t detail = 0u) {
    note_crash_breadcrumb(stage, detail);
    if (xTaskGetSchedulerState() != taskSCHEDULER_NOT_STARTED) {
        feed_loop_watchdog();
        delay(1);
    }
}

const char* homeplug_mmtype_name(uint16_t mmtype) {
    switch (mmtype) {
    case (slac::defs::MMTYPE_CM_SET_KEY | slac::defs::MMTYPE_MODE_REQ): return "CM_SET_KEY.REQ";
    case (slac::defs::MMTYPE_CM_SET_KEY | slac::defs::MMTYPE_MODE_CNF): return "CM_SET_KEY.CNF";
    case (slac::defs::MMTYPE_CM_SLAC_PARAM | slac::defs::MMTYPE_MODE_REQ): return "CM_SLAC_PARAM.REQ";
    case (slac::defs::MMTYPE_CM_SLAC_PARAM | slac::defs::MMTYPE_MODE_CNF): return "CM_SLAC_PARAM.CNF";
    case (slac::defs::MMTYPE_CM_START_ATTEN_CHAR | slac::defs::MMTYPE_MODE_IND): return "CM_START_ATTEN_CHAR.IND";
    case (slac::defs::MMTYPE_CM_MNBC_SOUND | slac::defs::MMTYPE_MODE_IND): return "CM_MNBC_SOUND.IND";
    case (slac::defs::MMTYPE_CM_ATTEN_PROFILE | slac::defs::MMTYPE_MODE_IND): return "CM_ATTEN_PROFILE.IND";
    case (slac::defs::MMTYPE_CM_ATTEN_CHAR | slac::defs::MMTYPE_MODE_IND): return "CM_ATTEN_CHAR.IND";
    case (slac::defs::MMTYPE_CM_ATTEN_CHAR | slac::defs::MMTYPE_MODE_RSP): return "CM_ATTEN_CHAR.RSP";
    case (slac::defs::MMTYPE_CM_SLAC_MATCH | slac::defs::MMTYPE_MODE_REQ): return "CM_SLAC_MATCH.REQ";
    case (slac::defs::MMTYPE_CM_SLAC_MATCH | slac::defs::MMTYPE_MODE_CNF): return "CM_SLAC_MATCH.CNF";
    case (slac::defs::MMTYPE_CM_VALIDATE | slac::defs::MMTYPE_MODE_REQ): return "CM_VALIDATE.REQ";
    case (slac::defs::MMTYPE_CM_VALIDATE | slac::defs::MMTYPE_MODE_CNF): return "CM_VALIDATE.CNF";
    default: return "UNKNOWN";
    }
}

void format_hex_bytes_compact(const uint8_t* data, size_t len, char* out, size_t out_size) {
    if (!out || out_size == 0u) {
        return;
    }
    if (!data || len == 0u) {
        out[0] = '\0';
        return;
    }
    size_t pos = 0u;
    for (size_t i = 0; i < len && pos + 3u < out_size; ++i) {
        const int written = snprintf(out + pos, out_size - pos, "%02X", static_cast<unsigned>(data[i]));
        if (written <= 0) {
            break;
        }
        pos += static_cast<size_t>(written);
        if ((i + 1u) < len && pos + 2u < out_size) {
            out[pos++] = ' ';
        }
    }
    out[std::min(pos, out_size - 1u)] = '\0';
}

void log_homeplug_slac_details(const char* dir, uint16_t mmtype, uint8_t mmv, const uint8_t* frame, uint16_t len) {
    if (!ENABLE_DECODED_LOGS || !dir || !frame || len < 17u) {
        return;
    }

    const uint16_t payload_offset = (mmv == static_cast<uint8_t>(slac::defs::MMV::AV_1_0)) ? 17u : 19u;
    if (len <= payload_offset) {
        return;
    }

    const uint8_t* payload = frame + payload_offset;
    const size_t payload_len = static_cast<size_t>(len - payload_offset);
    char run_id_hex[3u * slac::defs::RUN_ID_LEN]{};
    char forwarding_hex[3u * ETH_ALEN]{};
    char msound_hex[3u * 6u]{};
    char raw_hex[3u * 32u]{};

    switch (mmtype) {
    case (slac::defs::MMTYPE_CM_SLAC_PARAM | slac::defs::MMTYPE_MODE_REQ):
        format_hex_bytes_compact(payload + 2u, std::min<size_t>(slac::defs::RUN_ID_LEN, payload_len > 2u ? payload_len - 2u : 0u),
                                 run_id_hex, sizeof(run_id_hex));
        format_hex_bytes_compact(payload, std::min<size_t>(16u, payload_len), raw_hex, sizeof(raw_hex));
        Serial.printf("[QCA] HP %s param.req app=%u sec=%u run_id=%s body=%s\n",
                      dir,
                      payload_len > 0u ? static_cast<unsigned>(payload[0]) : 0u,
                      payload_len > 1u ? static_cast<unsigned>(payload[1]) : 0u,
                      run_id_hex,
                      raw_hex);
        break;

    case (slac::defs::MMTYPE_CM_SLAC_PARAM | slac::defs::MMTYPE_MODE_CNF):
        format_hex_bytes_compact(payload, std::min<size_t>(6u, payload_len), msound_hex, sizeof(msound_hex));
        format_hex_bytes_compact(payload + 9u, std::min<size_t>(ETH_ALEN, payload_len > 9u ? payload_len - 9u : 0u),
                                 forwarding_hex, sizeof(forwarding_hex));
        format_hex_bytes_compact(payload + 17u, std::min<size_t>(slac::defs::RUN_ID_LEN, payload_len > 17u ? payload_len - 17u : 0u),
                                 run_id_hex, sizeof(run_id_hex));
        format_hex_bytes_compact(payload, std::min<size_t>(27u, payload_len), raw_hex, sizeof(raw_hex));
        Serial.printf("[QCA] HP %s param.cnf target=%s num=%u timeout=%u resp=%u fwd=%s app=%u sec=%u run_id=%s body=%s\n",
                      dir,
                      msound_hex,
                      payload_len > 6u ? static_cast<unsigned>(payload[6]) : 0u,
                      payload_len > 7u ? static_cast<unsigned>(payload[7]) : 0u,
                      payload_len > 8u ? static_cast<unsigned>(payload[8]) : 0u,
                      forwarding_hex,
                      payload_len > 15u ? static_cast<unsigned>(payload[15]) : 0u,
                      payload_len > 16u ? static_cast<unsigned>(payload[16]) : 0u,
                      run_id_hex,
                      raw_hex);
        break;

    case (slac::defs::MMTYPE_CM_START_ATTEN_CHAR | slac::defs::MMTYPE_MODE_IND):
        format_hex_bytes_compact(payload + 5u, std::min<size_t>(ETH_ALEN, payload_len > 5u ? payload_len - 5u : 0u),
                                 forwarding_hex, sizeof(forwarding_hex));
        format_hex_bytes_compact(payload + 11u, std::min<size_t>(slac::defs::RUN_ID_LEN, payload_len > 11u ? payload_len - 11u : 0u),
                                 run_id_hex, sizeof(run_id_hex));
        format_hex_bytes_compact(payload, std::min<size_t>(24u, payload_len), raw_hex, sizeof(raw_hex));
        Serial.printf("[QCA] HP %s start_atten app=%u sec=%u num=%u timeout=%u resp=%u fwd=%s run_id=%s body=%s\n",
                      dir,
                      payload_len > 0u ? static_cast<unsigned>(payload[0]) : 0u,
                      payload_len > 1u ? static_cast<unsigned>(payload[1]) : 0u,
                      payload_len > 2u ? static_cast<unsigned>(payload[2]) : 0u,
                      payload_len > 3u ? static_cast<unsigned>(payload[3]) : 0u,
                      payload_len > 4u ? static_cast<unsigned>(payload[4]) : 0u,
                      forwarding_hex,
                      run_id_hex,
                      raw_hex);
        break;

    default:
        break;
    }
}

void log_homeplug_frame(const char* dir, const uint8_t* frame, uint16_t len) {
    if (!ENABLE_DECODED_LOGS || !dir || !frame || len < 17u) {
        return;
    }
    const uint16_t eth_type = static_cast<uint16_t>((frame[12] << 8) | frame[13]);
    if (eth_type != slac::defs::ETH_P_HOMEPLUG_GREENPHY) {
        return;
    }

    const uint8_t mmv = frame[14];
    const uint16_t mmtype = static_cast<uint16_t>(frame[15] | (static_cast<uint16_t>(frame[16]) << 8u));
    Serial.printf("[QCA] HP %s mmtype=0x%04X(%s) mmv=0x%02X len=%u src=%02X:%02X:%02X:%02X:%02X:%02X dst=%02X:%02X:%02X:%02X:%02X:%02X\n",
                  dir,
                  static_cast<unsigned>(mmtype),
                  homeplug_mmtype_name(mmtype),
                  static_cast<unsigned>(mmv),
                  static_cast<unsigned>(len),
                  static_cast<unsigned>(frame[6]), static_cast<unsigned>(frame[7]),
                  static_cast<unsigned>(frame[8]), static_cast<unsigned>(frame[9]),
                  static_cast<unsigned>(frame[10]), static_cast<unsigned>(frame[11]),
                  static_cast<unsigned>(frame[0]), static_cast<unsigned>(frame[1]),
                  static_cast<unsigned>(frame[2]), static_cast<unsigned>(frame[3]),
                  static_cast<unsigned>(frame[4]), static_cast<unsigned>(frame[5]));
    log_homeplug_slac_details(dir, mmtype, mmv, frame, len);
}

constexpr uint16_t PLC_PEER_MAC_DEFAULT[3] = {0x00B0, 0x5200, 0x0001};
constexpr char ISO_EVSE_ID[] = "IN*JPE*E000100010001";
constexpr float PRECHARGE_CURRENT_LIMIT_A = 2.0f;
constexpr float PRECHARGE_VOLTAGE_TOLERANCE_V = 20.0f;
constexpr float PRECHARGE_START_ACCEPT_MIN_RATIO = 0.05f;
constexpr float PRECHARGE_START_ACCEPT_MIN_V = 5.0f;
constexpr float PRECHARGE_START_ACCEPT_MIN_CURRENT_A = 0.50f;
constexpr uint32_t PRECHARGE_RELAX_MIN_COUNT = 2u;
constexpr uint32_t CURRENT_DEMAND_FRESH_SAMPLE_TIMEOUT_MS = 120u;
constexpr uint32_t CURRENT_DEMAND_FRESH_SAMPLE_POLL_MS = 5u;
constexpr uint32_t HLC_UNEXPECTED_DISCONNECT_HOLD_MS = 10000u;
constexpr float MODULE_MAX_VOLTAGE_V = 1000.0f;
constexpr float MODULE_MAX_CURRENT_A = 100.0f;
constexpr float MODULE_MAX_POWER_KW = 30.0f;
constexpr uint32_t MODULE_ALLOWLIST_LEASE_MS = 3000;
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
constexpr int EMERGENCY_EXP_PIN = 8;
constexpr bool RELAY_ACTIVE_HIGH = true;

constexpr int LED_DATA_PIN = 3;
constexpr uint8_t DEFAULT_LED_COUNT = 10u;
constexpr uint8_t MAX_LED_COUNT = 32u;
constexpr uint8_t LED_BRIGHTNESS = 96u;

constexpr int CAN_CS_PIN = 41;
constexpr int CAN_INT_PIN = 40;
constexpr int CAN_SPI_SCK = 48;
constexpr int CAN_SPI_MISO = 21;
constexpr int CAN_SPI_MOSI = 47;
constexpr uint32_t CAN_SPI_HZ = 8000000UL;
constexpr uint32_t CAN_BITRATE_KBPS = 125;
constexpr uint32_t RFID_SPI_HZ = 1000000UL;
constexpr uint32_t RFID_POLL_MS = 30u;
constexpr uint32_t RFID_CARD_RELEASE_MS = 350u;
constexpr uint32_t RFID_REINIT_BACKOFF_MS = 5000u;
constexpr uint32_t RFID_KEEPALIVE_MS = 1000u;
constexpr uint32_t RFID_MISSING_LOG_MS = 30000u;
constexpr size_t RFID_MAX_UID_BYTES = 10u;
constexpr uint32_t EMERGENCY_POLL_MS = 25u;
constexpr uint32_t EMERGENCY_DEBOUNCE_MS = 50u;
constexpr uint32_t LED_REFRESH_MS = 40u;
constexpr uint32_t RUNTIME_STATS_PERIOD_MS = 15000u;
constexpr size_t PSRAM_BULK_ALLOC_THRESHOLD_BYTES = 4096u;
struct RfidPinMap {
    const char* name;
    int ss_pin;
    int rst_pin;
};

constexpr std::array<RfidPinMap, 2> RFID_PIN_MAPS{{
    {"spare_36_39", 36, 39},
    {"legacy_18_17", 18, 17},
}};

constexpr uint32_t CFG_MAGIC = 0x4342504Cu; // "CBPL"
constexpr uint16_t CFG_VERSION = 6u;

#ifndef CBPLC_DEFAULT_MODE
#ifdef CBPLC_DEFAULT_STANDALONE_MODE
#define CBPLC_DEFAULT_MODE ((CBPLC_DEFAULT_STANDALONE_MODE) ? 0 : 1)
#else
#define CBPLC_DEFAULT_MODE 1
#endif
#endif

#ifndef CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START
#define CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START 0
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

#ifndef CBPLC_DEFAULT_CAN_NODE_ID
#define CBPLC_DEFAULT_CAN_NODE_ID CBPLC_DEFAULT_PLC_ID
#endif

enum class OperatingMode : uint8_t {
    Standalone = 0u,
    ExternalController = 1u,
};

enum class LedPreset : uint8_t {
    Booting = 0u,
    Available = 1u,
    Preparing = 2u,
    Charging = 3u,
    Finishing = 4u,
    Faulted = 5u,
    Emergency = 6u,
};

constexpr uint8_t DEFAULT_MODE_RAW = static_cast<uint8_t>(CBPLC_DEFAULT_MODE);
constexpr OperatingMode DEFAULT_MODE =
    (DEFAULT_MODE_RAW == static_cast<uint8_t>(OperatingMode::Standalone))
        ? OperatingMode::Standalone
        : OperatingMode::ExternalController;
constexpr bool DEFAULT_SLAC_REQUIRES_CONTROLLER_START = (CBPLC_DEFAULT_SLAC_REQUIRES_CONTROLLER_START != 0);
constexpr uint8_t DEFAULT_PLC_ID = static_cast<uint8_t>(CBPLC_DEFAULT_PLC_ID);
constexpr uint8_t DEFAULT_CONNECTOR_ID = static_cast<uint8_t>(CBPLC_DEFAULT_CONNECTOR_ID);
constexpr uint8_t DEFAULT_CONTROLLER_ID = static_cast<uint8_t>(CBPLC_DEFAULT_CONTROLLER_ID);
constexpr uint8_t DEFAULT_LOCAL_MODULE_ADDRESS = static_cast<uint8_t>(CBPLC_DEFAULT_LOCAL_MODULE_ADDRESS);
constexpr uint8_t DEFAULT_CAN_NODE_ID = static_cast<uint8_t>(CBPLC_DEFAULT_CAN_NODE_ID);
constexpr uint32_t DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS = 1200u;
constexpr uint32_t DEFAULT_CONTROLLER_AUTH_TTL_MS = 2500u;
constexpr uint32_t DEFAULT_SLAC_ARM_TIMEOUT_MS = 10000u;
constexpr uint32_t CONTROLLER_HB_SOFT_STOP_GRACE_MS = 3000u;
constexpr uint32_t CONTROLLER_HB_HARD_STOP_GRACE_MS = 15000u;
constexpr uint32_t CONTROLLER_ENERGY_HOLD_GRACE_MS = 1200u;
constexpr uint32_t CONTROLLER_FEEDBACK_TIMEOUT_MS = 1200u;
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
constexpr uint32_t CAN_ID_CTRL_AUX_RELAY = 0x18FF5070u;
constexpr uint32_t CAN_ID_CTRL_SESSION = 0x18FF5080u;
constexpr uint32_t CAN_ID_CTRL_HLC_FEEDBACK = 0x18FF50A0u;

constexpr uint32_t PLC_TX_IDENTITY_RETRY_MS = 1000u;
constexpr uint32_t SERIAL_TX_CP_STATUS_MS = 500u;
constexpr uint32_t SERIAL_TX_SLAC_STATUS_MS = 500u;
constexpr uint32_t SERIAL_TX_HLC_STATUS_MS = 500u;
constexpr uint32_t SERIAL_TX_SESSION_STATUS_MS = 500u;
constexpr uint32_t SERIAL_TX_BMS_STATUS_MS = 250u;

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

SPIClass& g_can_spi = SPI;
PCA9555 g_relay_expander;

extern SemaphoreHandle_t g_can_mutex;
void tap_controller_rx_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8], uint32_t now_ms);
uint8_t crc8_07(const uint8_t* data, size_t len);
uint32_t can_with_plc_id(uint32_t base_id);
bool is_allowed_module_can_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8]);
void controller_publish_local_identity_once();
const char* bms_stage_name(uint8_t stage);
void serial_tx_identity_hex(const char* kind, const uint8_t* data, size_t len);
bool mode_uart_status_stream_active();
void serial_tx_local_identity();
void serial_tx_cp_status(uint32_t now_ms);
void serial_tx_slac_status();
void serial_tx_hlc_status(uint32_t now_ms);
void serial_tx_session_status(uint32_t now_ms);
void serial_tx_bms_status();
void serial_tx_emergency_event(bool active);
bool apply_new_allowlist(const std::vector<std::string>& next, float limit_kw, const char* reason);
bool lock_modules(uint32_t timeout_ms);
void unlock_modules();
void stop_hlc_stack();
void stop_session(uint32_t now_ms);
float snapshot_present_group_voltage_v();
void controller_force_safe_stop(const char* reason);
bool read_emergency_pressed();
void service_emergency_stop(uint32_t now_ms);
void init_status_leds();
void service_status_leds(uint32_t now_ms);
LedPreset effective_led_preset(uint32_t now_ms = 0u);
void enable_psram_malloc_policy();
void log_runtime_stats(const char* reason);
void service_runtime_stats(uint32_t now_ms);

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
        if (pop_buffered_module_frame(frame_out)) {
            return true;
        }
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

            if (!is_allowed_module_can_frame(in.id, in.extended, candidate.dlc, candidate.data)) {
                continue;
            }

            frame_out = candidate;
            return true;
        }
        return false;
    }

    void poll_controller_frames(uint8_t budget = 16u) {
        (void)budget;
    }

    uint32_t now_ms() const override {
        return millis();
    }

private:
    static constexpr size_t BUFFERED_MODULE_FRAME_CAPACITY = 32u;

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
        // Leave the MCP2515 in accept-all extended-frame mode because this bus
        // carries shared module telemetry and ownership frames in standalone
        // mode. Per-target acceptance is enforced in software by
        // is_allowed_module_can_frame().
        const uint32_t accept_all_mask = 0u;
        const uint32_t accept_all_filter = 0u;
        if (mcp2515_set_filters_ext(&dev_, accept_all_mask, &accept_all_filter, 1) != ENR_OK) {
            xSemaphoreGive(g_can_mutex);
            return false;
        }
        xSemaphoreGive(g_can_mutex);
        return true;
    }

    bool push_buffered_module_frame(const cbmodules::CanFrame& frame) {
        if (buffered_module_count_ >= BUFFERED_MODULE_FRAME_CAPACITY) {
            buffered_module_head_ = (buffered_module_head_ + 1u) % BUFFERED_MODULE_FRAME_CAPACITY;
            buffered_module_count_--;
            const uint32_t now_ms = millis();
            if (buffered_module_overflow_log_ms_ == 0u ||
                static_cast<int32_t>(now_ms - buffered_module_overflow_log_ms_) >= 0) {
                Serial.printf("[CAN] buffered module frame overflow cap=%u\n",
                              static_cast<unsigned>(BUFFERED_MODULE_FRAME_CAPACITY));
                buffered_module_overflow_log_ms_ = now_ms + 5000u;
            }
        }
        buffered_module_frames_[buffered_module_tail_] = frame;
        buffered_module_tail_ = (buffered_module_tail_ + 1u) % BUFFERED_MODULE_FRAME_CAPACITY;
        buffered_module_count_++;
        return true;
    }

    bool pop_buffered_module_frame(cbmodules::CanFrame& frame) {
        if (buffered_module_count_ == 0u) {
            return false;
        }
        frame = buffered_module_frames_[buffered_module_head_];
        buffered_module_head_ = (buffered_module_head_ + 1u) % BUFFERED_MODULE_FRAME_CAPACITY;
        buffered_module_count_--;
        return true;
    }

    mcp2515_t dev_{};
    mcp2515_hal_t hal_{};
    bool inited_{false};
    uint8_t active_osc_mhz_{0};
    std::array<cbmodules::CanFrame, BUFFERED_MODULE_FRAME_CAPACITY> buffered_module_frames_{};
    size_t buffered_module_head_{0};
    size_t buffered_module_tail_{0};
    size_t buffered_module_count_{0};
    uint32_t buffered_module_overflow_log_ms_{0};
};

void lwip_ingress_ethernet_frame(const uint8_t* frame, uint16_t len);

class Qca7000Transport final : public slac::ITransport {
public:
    Qca7000Transport() : spi(HSPI) {
        spi_mutex = xSemaphoreCreateMutex();
    }

    ~Qca7000Transport() override {
        if (interrupt_attached) {
            detachInterrupt(digitalPinToInterrupt(PIN_QCA700X_INT));
        }
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

        pulse_hardware_reset();

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

        uint16_t stale_intr = 0;
        (void)spi_read_register16(SPI_REG_INTR_CAUSE, &stale_intr);
        if (stale_intr != 0u) {
            (void)spi_write_register16(SPI_REG_INTR_CAUSE, stale_intr);
        }
        (void)spi_write_register16(SPI_REG_INTR_ENABLE, 0u);

        if (interrupt_attached) {
            detachInterrupt(digitalPinToInterrupt(PIN_QCA700X_INT));
        }
        attachInterrupt(digitalPinToInterrupt(PIN_QCA700X_INT), qca_irq_handler, RISING);
        interrupt_attached = true;
        startup_pending = false;
        cpu_on_seen_since_reset = false;
        reset_started_ms = 0u;
        interrupts_armed = false;
        g_qca_irq_pending = digitalRead(PIN_QCA700X_INT) == HIGH;
        ready = true;
        clear_rx_state();
        last_error.clear();
        return true;
    }

    void modem_reset() {
        (void)restart_startup_sync(false);
    }

    uint32_t rx_queue_drops() const {
        return rx_queue_drop_count;
    }

    bool restart_startup_sync(bool hard_reset) {
        if (!ready) {
            last_error = "transport not ready";
            return false;
        }
        clear_rx_state();
        mark_lwip_tx_transport_flush(hard_reset ? "qca hard reset" : "qca modem reset");
        startup_pending = true;
        interrupts_armed = false;
        cpu_on_seen_since_reset = false;
        last_startup_signature = 0u;
        last_startup_wrbuf = 0u;
        reset_started_ms = millis();
        g_qca_irq_pending = false;
        startup_wait_recovered_without_irq = false;

        if (hard_reset) {
            pulse_hardware_reset();
            uint16_t sig = 0u;
            if (!probe_signature(sig, 750u, 5u)) {
                last_startup_signature = sig;
                char buf[96];
                snprintf(buf, sizeof(buf), "QCA7000 hard-reset signature mismatch (last=0x%04X)", sig);
                last_error = buf;
                return false;
            }
        }

        (void)spi_write_register16(SPI_REG_INTR_ENABLE, 0u);
        clear_pending_interrupts();
        enable_interrupts(QCA_INT_STARTUP_MASK);
        uint16_t reg = 0u;
        if (!spi_read_register16(SPI_REG_SPI_CONFIG, &reg)) {
            if (last_error.empty()) {
                last_error = hard_reset ? "QCA7000 hard-reset read failed" : "QCA7000 reset read failed";
            }
            return false;
        }
        reg = static_cast<uint16_t>(reg | QCASPI_SLAVE_RESET_BIT);
        if (!spi_write_register16(SPI_REG_SPI_CONFIG, reg)) {
            if (last_error.empty()) {
                last_error = hard_reset ? "QCA7000 hard-reset write failed" : "QCA7000 reset write failed";
            }
            return false;
        }
        uint16_t startup_intr = 0u;
        if (spi_read_register16(SPI_REG_INTR_CAUSE, &startup_intr)) {
            if ((startup_intr & SPI_INT_CPU_ON) != 0u) {
                cpu_on_seen_since_reset = true;
            }
            if (startup_intr != 0u) {
                (void)spi_write_register16(SPI_REG_INTR_CAUSE, startup_intr);
            }
        }
        const bool line_high = digitalRead(PIN_QCA700X_INT) == HIGH;
        g_qca_irq_pending = line_high || startup_intr != 0u;
        last_error = hard_reset ? "QCA7000 hard reset in progress" : "QCA7000 reset in progress";
        return true;
    }

    bool is_ready() const {
        return ready;
    }

    IOResult write_nowait(const void* buffer, size_t size) {
        return write_internal(buffer, size, 0, true);
    }

    bool wait_for_sync(uint32_t timeout_ms, uint32_t poll_delay_ms = 5u) {
        return ensure_startup_ready(timeout_ms, poll_delay_ms);
    }

    void service_ingress_once() {
        if (!ready) {
            return;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaIngressEnter);
        const uint32_t now_ms = millis();
        const bool line_high = digitalRead(PIN_QCA700X_INT) == HIGH;
        const bool fallback_poll = (last_poll_ms == 0u) || static_cast<int32_t>(now_ms - last_poll_ms) >=
                                                           static_cast<int32_t>(QCA_POLL_FALLBACK_MS);
        if (!g_qca_irq_pending && !line_high && !fallback_poll) {
            return;
        }
        last_poll_ms = now_ms;
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaIngressBeforePoll,
                              static_cast<uint16_t>((g_qca_irq_pending ? 0x100u : 0u) |
                                                    (line_high ? 0x010u : 0u) |
                                                    (fallback_poll ? 0x001u : 0u)));
        poll_ingress(fallback_poll);
    }

    IOResult read(uint8_t* buffer, int timeout_ms) override {
        if (!ready) {
            last_error = "transport not ready";
            return IOResult::Failure;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaIngressEnter,
                              static_cast<uint16_t>(0x2000u | ((timeout_ms > 0) ? std::min(timeout_ms, 0x0FFF) : 0u)));
        const bool short_deadline = timeout_ms >= 0 && timeout_ms <= 5;
        if (startup_pending) {
            if (short_deadline) {
                last_error = "transport startup in progress";
                return IOResult::Timeout;
            }
            if (!ensure_startup_ready(500, 5)) {
                return IOResult::Failure;
            }
        }

        const uint32_t start_ms = millis();
        while (true) {
            uint16_t frame_len = 0u;
            if (pop_frame(buffer, &frame_len)) {
                note_crash_breadcrumb(CrashBreadcrumbStage::QcaProcessRxDone,
                                      static_cast<uint16_t>(0x2000u | std::min<uint16_t>(frame_len, 0x0FFFu)));
                return IOResult::Ok;
            }

            service_ingress_once();
            if (pop_frame(buffer, &frame_len)) {
                note_crash_breadcrumb(CrashBreadcrumbStage::QcaProcessRxDone,
                                      static_cast<uint16_t>(0x2400u | std::min<uint16_t>(frame_len, 0x0BFFu)));
                return IOResult::Ok;
            }

            if (timeout_ms >= 0 && (uint32_t)(millis() - start_ms) >= static_cast<uint32_t>(timeout_ms)) {
                return IOResult::Timeout;
            }
            cooperative_runtime_breather(CrashBreadcrumbStage::QcaIngressBeforePoll,
                                         static_cast<uint16_t>(std::min(timeout_ms < 0 ? 0 : timeout_ms, 0xFFFF)));
        }
    }

    IOResult write(const void* buffer, size_t size, int timeout_ms) override {
        return write_internal(buffer, size, timeout_ms, false);
    }

    const std::string& get_error() const override {
        return last_error;
    }

private:
    struct RxFrame {
        std::array<uint8_t, ETH_FRAME_LEN> data{};
        uint16_t len{0};
    };

    void pulse_hardware_reset() {
        digitalWrite(PIN_QCA700X_RESET, LOW);
        delay(10);
        digitalWrite(PIN_QCA700X_RESET, HIGH);
        delay(10);
    }

    void clear_pending_interrupts() {
        uint16_t stale_intr = 0u;
        if (!spi_read_register16(SPI_REG_INTR_CAUSE, &stale_intr)) {
            return;
        }
        if (stale_intr != 0u) {
            (void)spi_write_register16(SPI_REG_INTR_CAUSE, stale_intr);
        }
    }

    bool probe_signature(uint16_t& last_sig, uint32_t timeout_ms, uint32_t poll_delay_ms) {
        const uint32_t start_ms = millis();
        bool primed = false;
        last_sig = 0;

        while ((millis() - start_ms) < timeout_ms) {
            if (!primed) {
                (void)spi_read_register16(SPI_REG_SIGNATURE, &last_sig);
                primed = true;
                delay(poll_delay_ms);
                continue;
            }

            if (!spi_read_register16(SPI_REG_SIGNATURE, &last_sig)) {
                delay(poll_delay_ms);
                continue;
            }
            if (last_sig == QCASPI_GOOD_SIGNATURE) {
                return true;
            }

            primed = false;
            delay(poll_delay_ms);
        }

        return false;
    }

    bool ensure_startup_ready(uint32_t timeout_ms, uint32_t poll_delay_ms) {
        if (!ready) {
            last_error = "transport not ready";
            return false;
        }
        if (!startup_pending) {
            if (!interrupts_armed) {
                enable_interrupts(QCA_INT_ENABLE_MASK);
            }
            return true;
        }

        uint8_t startup_restart_attempts = 0u;
        uint32_t attempt_start_ms = millis();
        while (startup_pending) {
            poll_ingress(true);
            const uint32_t now_ms = millis();
            const bool allow_signature_only =
                reset_started_ms != 0u &&
                static_cast<int32_t>(now_ms - (reset_started_ms + QCA_STARTUP_FORCE_SYNC_MS)) >= 0;
            if (try_complete_startup_sync(allow_signature_only)) {
                break;
            }
            if (timeout_ms != 0u &&
                static_cast<uint32_t>(now_ms - attempt_start_ms) >= timeout_ms) {
                if (startup_restart_attempts < 2u) {
                    const bool hard_reset = startup_restart_attempts >= 1u;
                    Serial.printf("[QCA] startup sync timeout, retrying with %s reset\n",
                                  hard_reset ? "hardware" : "soft");
                    if (!restart_startup_sync(hard_reset)) {
                        break;
                    }
                    startup_restart_attempts++;
                    attempt_start_ms = millis();
                    continue;
                }
                break;
            }
            if (poll_delay_ms > 0u) {
                delay(poll_delay_ms);
            } else {
                taskYIELD();
            }
        }

        if (startup_pending) {
            char buf[96];
            snprintf(buf, sizeof(buf), "QCA7000 startup timeout (cpu_on=%d sig=0x%04X wrbuf=%u)",
                     cpu_on_seen_since_reset ? 1 : 0,
                     last_startup_signature,
                     static_cast<unsigned>(last_startup_wrbuf));
            last_error = buf;
            return false;
        }

        enable_interrupts(QCA_INT_ENABLE_MASK);
        g_qca_irq_pending = digitalRead(PIN_QCA700X_INT) == HIGH;
        last_error.clear();
        return true;
    }

    bool try_complete_startup_sync(bool allow_without_cpu_on) {
        if (!startup_pending) {
            return true;
        }
        if (!cpu_on_seen_since_reset && !allow_without_cpu_on) {
            return false;
        }

        uint16_t sig0 = 0u;
        uint16_t sig1 = 0u;
        uint16_t wrbuf_space = 0u;
        if (!spi_read_register16(SPI_REG_SIGNATURE, &sig0) ||
            !spi_read_register16(SPI_REG_SIGNATURE, &sig1)) {
            return false;
        }
        last_startup_signature = sig1;
        if (sig0 != QCASPI_GOOD_SIGNATURE || sig1 != QCASPI_GOOD_SIGNATURE) {
            return false;
        }
        if (!spi_read_register16(SPI_REG_WRBUF_SPC_AVA, &wrbuf_space)) {
            return false;
        }
        last_startup_wrbuf = wrbuf_space;
        if (wrbuf_space != QCA7K_BUFFER_SIZE) {
            return false;
        }

        startup_pending = false;
        const bool recovered_without_cpu_on = !cpu_on_seen_since_reset && allow_without_cpu_on;
        cpu_on_seen_since_reset = false;
        reset_started_ms = 0u;
        g_qca_irq_pending = digitalRead(PIN_QCA700X_INT) == HIGH;
        startup_wait_recovered_without_irq = recovered_without_cpu_on;
        if (recovered_without_cpu_on) {
            Serial.println("[QCA] startup sync recovered without CPU_ON irq");
        }
        Serial.printf("[QCA] sync ready sig=0x%04X wrbuf=%u\n",
                      sig1,
                      static_cast<unsigned>(wrbuf_space));
        last_error.clear();
        return true;
    }

    void enable_interrupts(uint16_t mask) {
        if (spi_write_register16(SPI_REG_INTR_ENABLE, mask)) {
            interrupts_armed = true;
        } else {
            interrupts_armed = false;
        }
    }

    bool spi_read_register16(uint16_t reg, uint16_t* out) {
        if (!out) {
            last_error = "spi read null destination";
            return false;
        }
        const uint16_t tx = static_cast<uint16_t>(QCA7K_SPI_READ | QCA7K_SPI_INTERNAL | reg);
        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return false;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(tx);
        *out = spi.transfer16(0x0000);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        return true;
    }

    bool spi_write_register16(uint16_t reg, uint16_t value) {
        const uint16_t tx = static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | reg);
        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return false;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(tx);
        spi.transfer16(value);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        return true;
    }

    bool read_and_mask_interrupt_cause(uint16_t* intr_cause) {
        if (!intr_cause) {
            last_error = "interrupt cause null destination";
            return false;
        }
        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return false;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_INTR_ENABLE));
        spi.transfer16(0x0000);
        digitalWrite(PIN_QCA700X_CS, HIGH);

        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_READ | QCA7K_SPI_INTERNAL | SPI_REG_INTR_CAUSE));
        *intr_cause = spi.transfer16(0x0000);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        interrupts_armed = false;
        return true;
    }

    void finish_interrupt_service(uint16_t intr_cause) {
        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        if (intr_cause != 0u) {
            digitalWrite(PIN_QCA700X_CS, LOW);
            spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_INTR_CAUSE));
            spi.transfer16(intr_cause);
            digitalWrite(PIN_QCA700X_CS, HIGH);
        }

        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_INTR_ENABLE));
        spi.transfer16(startup_pending ? QCA_INT_STARTUP_MASK : QCA_INT_ENABLE_MASK);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        interrupts_armed = true;
        g_qca_irq_pending = digitalRead(PIN_QCA700X_INT) == HIGH;
    }

    void clear_rx_state() {
        reset_decoder_state();
        q_head = 0;
        q_tail = 0;
        q_count = 0;
        invalid_frame_streak = 0;
        invalid_frame_since_ms = 0;
        last_poll_ms = 0;
    }

    bool looks_like_spi_glitch(uint16_t value) const {
        return value == 0x5555 || value == 0xAAAA || value == 0xFFFF;
    }

    bool valid_rx_len(uint16_t value) const {
        return value != 0 && value <= QCA7K_BUFFER_SIZE && value <= RX_CHUNK_CAPACITY;
    }

    void reset_decoder_state() {
        rx_decode_state = QcaFrameDecodeState::HwLen0;
        rx_expected_frame_len = 0u;
        rx_decode_offset = 0u;
    }

    int32_t decode_rx_byte(uint8_t recv_byte) {
        switch (rx_decode_state) {
        case QcaFrameDecodeState::HwLen0:
            rx_decode_state = (recv_byte == 0x00u) ? QcaFrameDecodeState::HwLen1 : QcaFrameDecodeState::HwLen0;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::HwLen1:
            rx_decode_state = (recv_byte == 0x00u) ? QcaFrameDecodeState::HwLen2 : QcaFrameDecodeState::HwLen0;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::HwLen2:
            rx_decode_state = QcaFrameDecodeState::HwLen3;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::HwLen3:
            rx_decode_state = QcaFrameDecodeState::Header0;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Header0:
            if (recv_byte != 0xAAu) {
                reset_decoder_state();
                return QCAFRM_NOHEAD;
            }
            rx_decode_state = QcaFrameDecodeState::Header1;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Header1:
            if (recv_byte != 0xAAu) {
                reset_decoder_state();
                return QCAFRM_NOHEAD;
            }
            rx_decode_state = QcaFrameDecodeState::Header2;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Header2:
            if (recv_byte != 0xAAu) {
                reset_decoder_state();
                return QCAFRM_NOHEAD;
            }
            rx_decode_state = QcaFrameDecodeState::Header3;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Header3:
            if (recv_byte != 0xAAu) {
                reset_decoder_state();
                return QCAFRM_NOHEAD;
            }
            rx_decode_state = QcaFrameDecodeState::Len0;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Len0:
            rx_expected_frame_len = recv_byte;
            rx_decode_state = QcaFrameDecodeState::Len1;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Len1:
            rx_expected_frame_len =
                static_cast<uint16_t>(rx_expected_frame_len | (static_cast<uint16_t>(recv_byte) << 8u));
            rx_decode_state = QcaFrameDecodeState::Reserved0;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Reserved0:
            rx_decode_state = QcaFrameDecodeState::Reserved1;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Reserved1:
            if (rx_expected_frame_len < ETH_FRAME_MIN_LEN_NO_FCS || rx_expected_frame_len > ETH_FRAME_LEN) {
                reset_decoder_state();
                return QCAFRM_INVLEN;
            }
            rx_decode_offset = 0u;
            rx_decode_state = QcaFrameDecodeState::Payload;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Payload:
            if (rx_decode_offset >= rx_expected_frame_len ||
                rx_decode_offset >= rx_frame_build.size()) {
                reset_decoder_state();
                return QCAFRM_INVLEN;
            }
            rx_frame_build[rx_decode_offset++] = recv_byte;
            if (rx_decode_offset >= rx_expected_frame_len) {
                rx_decode_state = QcaFrameDecodeState::Footer0;
            }
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Footer0:
            if (recv_byte != 0x55u) {
                reset_decoder_state();
                return QCAFRM_NOTAIL;
            }
            rx_decode_state = QcaFrameDecodeState::Footer1;
            return QCAFRM_GATHER;
        case QcaFrameDecodeState::Footer1: {
            if (recv_byte != 0x55u) {
                reset_decoder_state();
                return QCAFRM_NOTAIL;
            }
            const int32_t frame_len = static_cast<int32_t>(rx_expected_frame_len);
            reset_decoder_state();
            return frame_len;
        }
        }
        reset_decoder_state();
        return QCAFRM_INVLEN;
    }

    void note_invalid_decoder(int32_t rc, uint16_t pos, uint16_t total_avail) {
        const uint32_t now_ms = millis();
        if (invalid_frame_streak == 0u) {
            invalid_frame_since_ms = now_ms;
        }
        if (invalid_frame_streak < 0xFFu) {
            invalid_frame_streak++;
        }
        if (invalid_frame_streak == 1u || (invalid_frame_streak % 4u) == 0u) {
            Serial.printf("[QCA] invalid RX framing rc=%ld state=%ld pos=%u avail=%u streak=%u\n",
                          static_cast<long>(rc),
                          static_cast<long>(static_cast<uint8_t>(rx_decode_state)),
                          static_cast<unsigned>(pos),
                          static_cast<unsigned>(total_avail),
                          invalid_frame_streak);
        }
        if (invalid_frame_streak >= 10u && invalid_frame_since_ms != 0u &&
            static_cast<int32_t>(now_ms - invalid_frame_since_ms) >= 400) {
            Serial.printf("[QCA] invalid RX framing persisted, resetting modem\n");
            invalid_frame_streak = 0;
            invalid_frame_since_ms = 0;
            last_reset_ms = now_ms;
            modem_reset();
        }
    }

    uint16_t read_burst(uint8_t* dst) {
        if (!ready || startup_pending || !dst) {
            return 0;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaReadBurstEnter);

        uint16_t available = 0u;
        if (!spi_read_register16(SPI_REG_RDBUF_BYTE_AVA, &available)) {
            return 0;
        }
        if (!valid_rx_len(available)) {
            uint16_t retry1 = 0u;
            uint16_t retry2 = 0u;
            (void)spi_read_register16(SPI_REG_RDBUF_BYTE_AVA, &retry1);
            (void)spi_read_register16(SPI_REG_RDBUF_BYTE_AVA, &retry2);
            if (valid_rx_len(retry1)) {
                available = retry1;
            } else if (valid_rx_len(retry2)) {
                available = retry2;
            } else {
                const bool glitch_pair =
                    (looks_like_spi_glitch(available) && looks_like_spi_glitch(retry1)) ||
                    (looks_like_spi_glitch(available) && looks_like_spi_glitch(retry2)) ||
                    (looks_like_spi_glitch(retry1) && looks_like_spi_glitch(retry2));
                if (glitch_pair) {
                    if (glitch_streak < 0xFF) {
                        glitch_streak++;
                    }
                    const uint32_t now_ms = millis();
                    if (glitch_streak == 1u || (glitch_streak % 8u) == 0u ||
                        last_glitch_log_ms == 0u ||
                        static_cast<int32_t>(now_ms - last_glitch_log_ms) >= 500) {
                        Serial.printf("[QCA] rdbuf glitch v0=0x%04X v1=0x%04X v2=0x%04X streak=%u\n",
                                      available, retry1, retry2, glitch_streak);
                        last_glitch_log_ms = now_ms;
                    }
                    if (glitch_streak >= 64u &&
                        static_cast<int32_t>(now_ms - last_reset_ms) >= 3000) {
                        Serial.printf("[QCA] persistent rdbuf glitch, resetting modem\n");
                        last_reset_ms = now_ms;
                        glitch_streak = 0;
                        oversize_streak = 0;
                        modem_reset();
                    }
                } else if (available > RX_CHUNK_CAPACITY || retry1 > RX_CHUNK_CAPACITY || retry2 > RX_CHUNK_CAPACITY ||
                           available > QCA7K_BUFFER_SIZE || retry1 > QCA7K_BUFFER_SIZE || retry2 > QCA7K_BUFFER_SIZE) {
                    if (oversize_streak < 0xFF) {
                        oversize_streak++;
                    }
                    const uint32_t now_ms = millis();
                    if (oversize_streak == 1u || (oversize_streak % 8u) == 0u ||
                        last_oversize_log_ms == 0u ||
                        static_cast<int32_t>(now_ms - last_oversize_log_ms) >= 500) {
                        Serial.printf("[QCA] drop oversize burst v0=%u v1=%u v2=%u streak=%u\n",
                                      available, retry1, retry2, oversize_streak);
                        last_oversize_log_ms = now_ms;
                    }
                    if (oversize_streak >= 16u &&
                        static_cast<int32_t>(now_ms - last_reset_ms) >= 1000) {
                        Serial.printf("[QCA] persistent oversize burst, resetting modem\n");
                        last_reset_ms = now_ms;
                        glitch_streak = 0;
                        oversize_streak = 0;
                        modem_reset();
                    }
                }
                return 0;
            }
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaReadBurstAfterAvail, available);

        glitch_streak = 0;
        oversize_streak = 0;

        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return 0;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_BFR_SIZE));
        spi.transfer16(available);
        digitalWrite(PIN_QCA700X_CS, HIGH);

        note_crash_breadcrumb(CrashBreadcrumbStage::QcaReadBurstBeforePayload, available);
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_READ | QCA7K_SPI_EXTERNAL));
        // Use a read-only transfer so the inbound burst lands in dst without
        // aliasing tx/rx buffers inside the ESP32 SPI driver.
        spi.transferBytes(nullptr, dst, available);
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaReadBurstAfterPayload, available);

        return available;
    }

    bool push_frame(const uint8_t* frame, uint16_t len) {
        if (len > ETH_FRAME_LEN) {
            return false;
        }
        if (q_count >= RX_QUEUE_CAPACITY) {
            q_head = (q_head + 1u) % RX_QUEUE_CAPACITY;
            q_count--;
            rx_queue_drop_count++;
            const uint32_t now_ms = millis();
            if (last_rx_queue_drop_log_ms == 0u || static_cast<int32_t>(now_ms - last_rx_queue_drop_log_ms) >= 1000) {
                Serial.printf("[QCA] HomePlug RX queue overrun drops=%lu\n",
                              static_cast<unsigned long>(rx_queue_drop_count));
                last_rx_queue_drop_log_ms = now_ms;
            }
        }
        if (q_count >= RX_QUEUE_CAPACITY) {
            return false;
        }
        auto& slot = frame_queue[q_tail];
        memcpy(slot.data.data(), frame, len);
        slot.len = len;
        q_tail = (q_tail + 1u) % RX_QUEUE_CAPACITY;
        q_count++;
        return true;
    }

    bool pop_frame(uint8_t* dst, uint16_t* out_len) {
        if (q_count == 0) {
            return false;
        }
        auto& slot = frame_queue[q_head];
        if (slot.len == 0u || slot.len > ETH_FRAME_LEN) {
            Serial.printf("[QCA] RX queue corruption len=%u head=%u count=%u, resetting modem\n",
                          static_cast<unsigned>(slot.len),
                          static_cast<unsigned>(q_head),
                          static_cast<unsigned>(q_count));
            slot.len = 0u;
            clear_rx_state();
            modem_reset();
            return false;
        }
        if (out_len) {
            *out_len = slot.len;
        }
        if (dst) {
            memset(dst, 0, ETH_FRAME_LEN);
            memcpy(dst, slot.data.data(), slot.len);
        }
        slot.len = 0u;
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
            maybe_learn_qca_mac_from_homeplug_frame(frame, len);
            log_homeplug_frame("rx", frame, len);
            push_frame(frame, len);
            return;
        }
        lwip_ingress_ethernet_frame(frame, len);
    }

    void process_rx_stream(uint16_t chunk_len) {
        if (chunk_len == 0) {
            return;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaProcessRxStart, chunk_len);
        for (uint16_t i = 0u; i < chunk_len; ++i) {
            const int32_t rc = decode_rx_byte(rx_chunk[i]);
            if (rc == QCAFRM_GATHER || rc == QCAFRM_NOHEAD) {
                if (((i + 1u) % RX_PROCESS_BREATHER_STRIDE) == 0u) {
                    cooperative_runtime_breather(CrashBreadcrumbStage::QcaProcessRxStart, i + 1u);
                }
                continue;
            }
            if (rc == QCAFRM_INVLEN || rc == QCAFRM_NOTAIL) {
                note_invalid_decoder(rc, i, chunk_len);
                if (startup_pending) {
                    return;
                }
                continue;
            }
            if (rc > 0) {
                dispatch_eth_frame(rx_frame_build.data(), static_cast<uint16_t>(rc));
                invalid_frame_streak = 0;
                invalid_frame_since_ms = 0;
            }
            if (((i + 1u) % RX_PROCESS_BREATHER_STRIDE) == 0u) {
                cooperative_runtime_breather(CrashBreadcrumbStage::QcaProcessRxStart, i + 1u);
            }
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaProcessRxDone, chunk_len);
    }

    void poll_ingress(bool fallback_poll) {
        if (!ready) {
            return;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollEnter, fallback_poll ? 1u : 0u);

        const bool line_high = digitalRead(PIN_QCA700X_INT) == HIGH;
        const bool have_irq = g_qca_irq_pending || line_high;
        uint16_t intr_cause = 0u;
        bool intr_masked = false;

        if (have_irq) {
            if (!read_and_mask_interrupt_cause(&intr_cause)) {
                return;
            }
            intr_masked = true;
            note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollAfterMask, intr_cause);
            if ((intr_cause & SPI_INT_CPU_ON) != 0u) {
                cpu_on_seen_since_reset = true;
            }
            if ((intr_cause & (SPI_INT_RDBUF_ERR | SPI_INT_WRBUF_ERR)) != 0u) {
                Serial.printf("[QCA] interrupt error cause=0x%04X, resetting modem\n", intr_cause);
                finish_interrupt_service(intr_cause);
                modem_reset();
                return;
            }
        }

        const uint32_t now_ms = millis();
        if (startup_pending) {
            const bool allow_signature_only =
                reset_started_ms != 0u &&
                static_cast<int32_t>(now_ms - (reset_started_ms + QCA_STARTUP_FORCE_SYNC_MS)) >= 0;
            (void)try_complete_startup_sync(allow_signature_only);
            if (startup_pending && reset_started_ms != 0u &&
                static_cast<int32_t>(now_ms - (reset_started_ms + QCA_RESET_TIMEOUT_MS)) >= 0) {
                Serial.println("[QCA] reset wait timeout, escalating modem sync recovery");
                if (intr_masked) {
                    finish_interrupt_service(intr_cause);
                }
                (void)restart_startup_sync(true);
                return;
            }
        }

        const bool should_drain = !startup_pending &&
                                  (fallback_poll || line_high || (intr_cause & SPI_INT_PKT_AVLBL) != 0u);
        if (should_drain) {
            note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollBeforeDrain, intr_cause);
            for (uint8_t burst = 0; burst < QCA_MAX_BURSTS_PER_POLL; ++burst) {
                const uint16_t chunk_len = read_burst(rx_chunk.data());
                note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollAfterBurstRead,
                                      static_cast<uint16_t>((static_cast<uint16_t>(burst) << 12u) |
                                                            (chunk_len & 0x0FFFu)));
                if (chunk_len == 0u) {
                    break;
                }
                process_rx_stream(chunk_len);
                note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollAfterBurstProcess,
                                      static_cast<uint16_t>((static_cast<uint16_t>(burst) << 12u) |
                                                            (chunk_len & 0x0FFFu)));
                if (startup_pending) {
                    break;
                }
                if ((burst + 1u) < QCA_MAX_BURSTS_PER_POLL) {
                    cooperative_runtime_breather(CrashBreadcrumbStage::QcaPollAfterBurstProcess, burst + 1u);
                }
            }
        }

        if (intr_masked) {
            finish_interrupt_service(intr_cause);
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaPollAfterFinish, intr_cause);
    }

    IOResult write_internal(const void* buffer, size_t size, int timeout_ms, bool nonblocking) {
        if (!ready) {
            last_error = "transport not ready";
            return IOResult::Failure;
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaWriteEnter, static_cast<uint16_t>(std::min<size_t>(size, 0xFFFFu)));
        if (!buffer || size == 0u || size > ETH_FRAME_LEN) {
            last_error = "invalid frame size";
            return IOResult::Failure;
        }
        if (startup_pending) {
            if (nonblocking) {
                last_error = "transport startup in progress";
                return IOResult::Timeout;
            }
            if (!ensure_startup_ready(QCA_STARTUP_IO_WAIT_MS, 1u)) {
                return IOResult::Failure;
            }
        }

        const uint16_t payload_len = static_cast<uint16_t>(size);
        const uint16_t framed_eth_len = static_cast<uint16_t>(std::max<size_t>(size, ETH_FRAME_MIN_LEN_NO_FCS));
        const uint16_t total_len = static_cast<uint16_t>(framed_eth_len + 10u);
        const uint32_t start_ms = millis();
        while (true) {
            uint16_t wrbuf_space = 0u;
            if (!spi_read_register16(SPI_REG_WRBUF_SPC_AVA, &wrbuf_space)) {
                return IOResult::Failure;
            }
            note_crash_breadcrumb(CrashBreadcrumbStage::QcaWriteWaitWrbuf, wrbuf_space);
            if (wrbuf_space >= total_len) {
                break;
            }
            if (nonblocking || timeout_ms == 0 ||
                (timeout_ms > 0 &&
                 static_cast<uint32_t>(millis() - start_ms) >= static_cast<uint32_t>(timeout_ms))) {
                last_error = "QCA7000 write buffer unavailable";
                return IOResult::Timeout;
            }
            delay(1);
        }

        if (!write_framed_payload(reinterpret_cast<const uint8_t*>(buffer), payload_len, framed_eth_len, total_len)) {
            return IOResult::Failure;
        }

        last_error.clear();
        return IOResult::Ok;
    }

    bool write_framed_payload(const uint8_t* frame, uint16_t payload_len, uint16_t framed_eth_len, uint16_t total_len) {
        static constexpr std::array<uint8_t, ETH_FRAME_MIN_LEN_NO_FCS> kZeroPad = {};
        static constexpr uint8_t kFooter[2] = {0x55u, 0x55u};
        uint8_t hdr[8] = {
            0xAAu, 0xAAu, 0xAAu, 0xAAu,
            static_cast<uint8_t>(framed_eth_len & 0xFFu),
            static_cast<uint8_t>((framed_eth_len >> 8u) & 0xFFu),
            0x00u, 0x00u
        };
        const uint16_t pad_len = static_cast<uint16_t>(framed_eth_len - payload_len);
        log_homeplug_frame("tx", frame, payload_len);
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaWriteFrameStart, framed_eth_len);

        if (!lock_spi(50)) {
            last_error = "spi lock timeout";
            return false;
        }
        spi.beginTransaction(SPISettings(QCA_SPI_HZ, MSBFIRST, SPI_MODE3));
        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_INTERNAL | SPI_REG_BFR_SIZE));
        spi.transfer16(total_len);
        digitalWrite(PIN_QCA700X_CS, HIGH);

        digitalWrite(PIN_QCA700X_CS, LOW);
        spi.transfer16(static_cast<uint16_t>(QCA7K_SPI_WRITE | QCA7K_SPI_EXTERNAL));
        // Do not use in-place full-duplex transfers for outbound frames: the
        // ESP32 SPI driver aliases tx/rx for transfer(void*, size), which can
        // overwrite bytes that have not been clocked out yet.
        spi.writeBytes(hdr, sizeof(hdr));
        spi.writeBytes(frame, payload_len);
        if (pad_len > 0u) {
            spi.writeBytes(kZeroPad.data(), pad_len);
        }
        spi.writeBytes(kFooter, sizeof(kFooter));
        digitalWrite(PIN_QCA700X_CS, HIGH);
        spi.endTransaction();
        unlock_spi();
        note_crash_breadcrumb(CrashBreadcrumbStage::QcaWriteFrameDone, framed_eth_len);
        return true;
    }

    SPIClass spi;
    SemaphoreHandle_t spi_mutex{nullptr};
    bool ready{false};
    bool startup_pending{true};
    bool interrupts_armed{false};
    bool interrupt_attached{false};
    bool cpu_on_seen_since_reset{false};
    std::string last_error;
    std::array<uint8_t, RX_CHUNK_CAPACITY> rx_chunk{};
    std::array<uint8_t, ETH_FRAME_LEN> rx_frame_build{};
    QcaFrameDecodeState rx_decode_state{QcaFrameDecodeState::HwLen0};
    uint16_t rx_expected_frame_len{0u};
    uint16_t rx_decode_offset{0u};

    std::array<RxFrame, RX_QUEUE_CAPACITY> frame_queue{};
    size_t q_head{0};
    size_t q_tail{0};
    size_t q_count{0};
    uint32_t rx_queue_drop_count{0};
    uint32_t last_rx_queue_drop_log_ms{0};
    uint8_t oversize_streak{0};
    uint8_t glitch_streak{0};
    uint8_t invalid_frame_streak{0};
    uint32_t invalid_frame_since_ms{0};
    uint32_t last_glitch_log_ms{0};
    uint32_t last_oversize_log_ms{0};
    uint32_t last_reset_ms{0};
    uint32_t last_poll_ms{0};
    uint32_t reset_started_ms{0};
    uint16_t last_startup_signature{0u};
    uint16_t last_startup_wrbuf{0u};
    bool startup_wait_recovered_without_irq{false};

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
    case slac::evse::State::FinalizeSounding:
        return "FinalizeSounding";
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
std::array<uint8_t, slac::defs::NMK_LEN> g_runtime_slac_nmk{};
bool g_runtime_slac_nmk_valid = false;
uint8_t g_local_mac[ETH_ALEN]{};
uint8_t g_wifi_sta_mac[ETH_ALEN]{};
uint8_t g_last_seen_ev_mac[ETH_ALEN]{};
bool g_last_seen_ev_mac_valid = false;
int g_last_cp_mv = 0;

void ensure_runtime_slac_nmk() {
    if (g_runtime_slac_nmk_valid) {
        return;
    }
    esp_fill_random(g_runtime_slac_nmk.data(), g_runtime_slac_nmk.size());
    g_runtime_slac_nmk_valid = true;
    Serial.println("[SLAC] runtime NMK seeded");
}

bool is_zero_mac_addr(const uint8_t mac[ETH_ALEN]);
bool is_broadcast_mac_addr(const uint8_t mac[ETH_ALEN]);
bool is_valid_unicast_mac_addr(const uint8_t mac[ETH_ALEN]);
void log_secc_link_local();
void refresh_plc_link_local_identity();
void update_plc_interface_mac(const uint8_t mac[ETH_ALEN], const char* source);

struct RuntimeConfig {
    uint32_t magic{CFG_MAGIC};
    uint16_t version{CFG_VERSION};
    uint8_t mode{static_cast<uint8_t>(DEFAULT_MODE)};
    bool slac_requires_controller_start{DEFAULT_SLAC_REQUIRES_CONTROLLER_START};
    uint8_t plc_id{DEFAULT_PLC_ID};
    uint8_t connector_id{DEFAULT_CONNECTOR_ID};
    uint8_t controller_id{DEFAULT_CONTROLLER_ID};
    uint8_t local_module_address{DEFAULT_LOCAL_MODULE_ADDRESS};
    uint8_t can_node_id{DEFAULT_CAN_NODE_ID};
    uint32_t controller_heartbeat_timeout_ms{DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS};
    uint32_t controller_auth_ttl_ms{DEFAULT_CONTROLLER_AUTH_TTL_MS};
    uint32_t controller_slac_arm_timeout_ms{DEFAULT_SLAC_ARM_TIMEOUT_MS};
    uint8_t led_count{DEFAULT_LED_COUNT};
};

struct RuntimeConfigV5 {
    uint32_t magic{CFG_MAGIC};
    uint16_t version{5u};
    uint8_t mode{static_cast<uint8_t>(DEFAULT_MODE)};
    bool slac_requires_controller_start{DEFAULT_SLAC_REQUIRES_CONTROLLER_START};
    uint8_t plc_id{DEFAULT_PLC_ID};
    uint8_t connector_id{DEFAULT_CONNECTOR_ID};
    uint8_t controller_id{DEFAULT_CONTROLLER_ID};
    uint8_t local_module_address{DEFAULT_LOCAL_MODULE_ADDRESS};
    uint8_t can_node_id{DEFAULT_CAN_NODE_ID};
    uint32_t controller_heartbeat_timeout_ms{DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS};
    uint32_t controller_auth_ttl_ms{DEFAULT_CONTROLLER_AUTH_TTL_MS};
    uint32_t controller_slac_arm_timeout_ms{DEFAULT_SLAC_ARM_TIMEOUT_MS};
};

struct RuntimeConfigV4 {
    uint32_t magic{CFG_MAGIC};
    uint16_t version{4u};
    uint8_t mode{static_cast<uint8_t>(DEFAULT_MODE)};
    bool slac_requires_controller_start{DEFAULT_SLAC_REQUIRES_CONTROLLER_START};
    uint8_t plc_id{DEFAULT_PLC_ID};
    uint8_t connector_id{DEFAULT_CONNECTOR_ID};
    uint8_t controller_id{DEFAULT_CONTROLLER_ID};
    uint8_t local_module_address{DEFAULT_LOCAL_MODULE_ADDRESS};
    uint8_t can_node_id{DEFAULT_CAN_NODE_ID};
    uint32_t controller_heartbeat_timeout_ms{DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS};
    uint32_t controller_auth_ttl_ms{DEFAULT_CONTROLLER_AUTH_TTL_MS};
    uint32_t controller_alloc_ttl_ms{3000u};
    uint32_t controller_slac_arm_timeout_ms{DEFAULT_SLAC_ARM_TIMEOUT_MS};
};

struct RuntimeConfigV3 {
    uint32_t magic{CFG_MAGIC};
    uint16_t version{3u};
    bool standalone_mode{true};
    bool slac_requires_controller_start{DEFAULT_SLAC_REQUIRES_CONTROLLER_START};
    uint8_t plc_id{DEFAULT_PLC_ID};
    uint8_t connector_id{DEFAULT_CONNECTOR_ID};
    uint8_t controller_id{DEFAULT_CONTROLLER_ID};
    uint8_t local_module_address{DEFAULT_LOCAL_MODULE_ADDRESS};
    uint8_t can_node_id{DEFAULT_CAN_NODE_ID};
    uint32_t controller_heartbeat_timeout_ms{DEFAULT_CONTROLLER_HEARTBEAT_TIMEOUT_MS};
    uint32_t controller_auth_ttl_ms{DEFAULT_CONTROLLER_AUTH_TTL_MS};
    uint32_t controller_alloc_ttl_ms{3000u};
    uint32_t controller_slac_arm_timeout_ms{DEFAULT_SLAC_ARM_TIMEOUT_MS};
};

enum class ControllerAuthState : uint8_t {
    Unknown = 0xFFu,
    Denied = CTRL_AUTH_DENY,
    Pending = CTRL_AUTH_PENDING,
    Granted = CTRL_AUTH_GRANTED,
};

struct ControllerRuntimeState {
    uint32_t heartbeat_timeout_ms{0};
    uint32_t last_heartbeat_ms{0};
    uint32_t last_auth_update_ms{0};
    uint32_t auth_expiry_ms{0};
    uint32_t last_energy_allowed_ms{0};
    uint32_t slac_arm_expiry_ms{0};
    uint32_t hb_lost_since_ms{0};
    bool heartbeat_seen{false};
    uint32_t heartbeat_session_marker{0};
    bool slac_armed{false};
    bool slac_start_latched{false};
    bool slac_plc_owned{false};
    ControllerAuthState auth_state{ControllerAuthState::Unknown};
    uint8_t last_seq_heartbeat{0};
    uint8_t last_seq_auth{0};
    uint8_t last_seq_slac{0};
    uint8_t last_seq_aux{0};
    uint8_t last_seq_session{0};
    uint8_t last_seq_feedback{0};
    bool seq_seen_heartbeat{false};
    bool seq_seen_auth{false};
    bool seq_seen_slac{false};
    bool seq_seen_aux{false};
    bool seq_seen_session{false};
    bool seq_seen_feedback{false};
    bool stop_active{false};
    bool stop_hard{false};
    bool stop_force_issued{false};
    uint8_t stop_reason{0};
    uint32_t stop_deadline_ms{0};
};

struct ControllerManagedFeedbackState {
    uint32_t last_update_ms{0};
    bool seen{false};
    bool valid{false};
    bool ready{false};
    bool stop_charging{false};
    bool current_limit_achieved{false};
    bool voltage_limit_achieved{false};
    bool power_limit_achieved{false};
    float present_voltage_v{0.0f};
    float present_current_a{0.0f};
};

struct LedRuntimeState {
    bool initialized{false};
    LedPreset preset{LedPreset::Booting};
    LedPreset rendered{LedPreset::Booting};
    uint32_t last_update_ms{0};
};

bool controller_get_managed_feedback(uint32_t now_ms, ControllerManagedFeedbackState* out);

struct SerialStatusTxSchedule {
    uint32_t next_cp_status_ms{0};
    uint32_t next_slac_status_ms{0};
    uint32_t next_hlc_status_ms{0};
    uint32_t next_session_status_ms{0};
    uint32_t next_bms_status_ms{0};
    uint32_t next_identity_evt_ms{0};
    uint8_t ack_seq{0};
    bool local_identity_sent{false};
};

struct RfidRuntimeState {
    bool initialized{false};
    bool reader_present{false};
    bool card_latched{false};
    uint8_t event_id{0u};
    uint8_t map_index{0xFFu};
    size_t uid_len{0u};
    std::array<uint8_t, RFID_MAX_UID_BYTES> uid{};
    uint32_t next_poll_ms{0u};
    uint32_t last_present_ms{0u};
    uint32_t last_good_ms{0u};
    uint32_t next_keepalive_ms{0u};
    uint32_t next_reinit_ms{0u};
    uint32_t next_missing_log_ms{0u};
};

RuntimeConfig g_runtime_cfg{};
ControllerRuntimeState g_ctrl{};
ControllerManagedFeedbackState g_ctrl_feedback{};
LedRuntimeState g_led_state{};
SerialStatusTxSchedule g_serial_tx{};
MFRC522 g_rfid;
RfidRuntimeState g_rfid_state{};
Preferences g_cfg_prefs{};
std::string g_local_group_id = "PLC_GROUP_1";
std::string g_local_module_id = "PLC1_MXR_01";
uint8_t g_local_module_group = 1u;
std::map<uint8_t, std::string> g_known_module_ids{};
std::map<uint8_t, uint8_t> g_known_module_can_groups{};

char g_cp_state = 'A';
char g_last_cp_state = 'A';
char g_cp_state_candidate = 'A';
uint32_t g_cp_candidate_since_ms = 0;
uint32_t g_cp_connected_since_ms = 0;
uint32_t g_cp_last_seen_connected_ms = 0;
uint32_t g_cp_pwm_ready_since_ms = 0;
uint32_t g_cp_next_spike_log_ms = 0;
uint32_t g_hlc_disconnect_hold_until_ms = 0;

uint16_t g_last_cp_duty_pct = 100;
bool g_ef_pulse_active = false;
uint32_t g_ef_pulse_until_ms = 0;

bool g_session_started = false;
bool g_bcd_entered = false;
bool g_session_matched = false;
bool g_fsm_priming = false;
uint32_t g_slac_pwm_enable_at_ms = 0;
uint32_t g_session_started_ms = 0;
uint32_t g_session_rx_queue_drop_baseline = 0;
uint8_t g_slac_failures_this_cp = 0;
uint8_t g_slac_soft_retries_this_cp = 0;
uint32_t g_slac_hold_until_ms = 0;
slac::evse::State g_last_fsm_state = slac::evse::State::Reset;
uint32_t g_next_qca_init_ms = 0;
bool g_emergency_stop_active = false;

typedef struct {
    jpv2g_secc_t* secc;
    bool auth_seen;
    bool auth_ongoing_sent;
    bool precharge_seen;
    bool precharge_converged;
    bool power_delivery_enabled;
    bool stop_requested;
    uint32_t precharge_count;
    uint8_t sa_schedule_tuple_id;
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
TaskHandle_t g_loop_task = nullptr;
uint32_t g_next_runtime_stats_ms = 0;
uint64_t g_runtime_stats_window_start_us = 0u;
uint64_t g_loop_busy_us = 0u;
uint64_t g_hlc_busy_us = 0u;
CRGB g_leds[MAX_LED_COUNT];
struct netif g_plc_netif{};
bool g_plc_netif_ready = false;
char g_plc_ifname[JPV2G_IFACE_NAME_MAX] = {0};
uint32_t g_next_lwip_drop_log_ms = 0;
struct LwipTxSlot {
    uint16_t len;
    uint32_t epoch;
    uint8_t data[ETH_FRAME_LEN];
};
QueueHandle_t g_lwip_tx_queue = nullptr;
QueueHandle_t g_lwip_tx_free_queue = nullptr;
StaticQueue_t g_lwip_tx_ready_queue_struct{};
StaticQueue_t g_lwip_tx_free_queue_struct{};
std::array<uint8_t, LWIP_TX_QUEUE_DEPTH> g_lwip_tx_ready_queue_storage{};
std::array<uint8_t, LWIP_TX_QUEUE_DEPTH> g_lwip_tx_free_queue_storage{};
LwipTxSlot* g_lwip_tx_slots = nullptr;
volatile uint32_t g_lwip_tx_epoch = 1u;
uint32_t g_next_lwip_tx_drop_log_ms = 0;
Mcp2515Transport g_can;
cbmodules::ModuleManager g_module_mgr{};

uint8_t active_led_count() {
    return std::max<uint8_t>(1u, std::min<uint8_t>(g_runtime_cfg.led_count, MAX_LED_COUNT));
}

const char* led_preset_name(LedPreset preset) {
    switch (preset) {
        case LedPreset::Booting:
            return "booting";
        case LedPreset::Available:
            return "available";
        case LedPreset::Preparing:
            return "preparing";
        case LedPreset::Charging:
            return "charging";
        case LedPreset::Finishing:
            return "finishing";
        case LedPreset::Faulted:
            return "faulted";
        case LedPreset::Emergency:
            return "emergency";
        default:
            return "available";
    }
}

bool parse_led_preset_token(String token, LedPreset* out) {
    token.trim();
    token.toLowerCase();
    LedPreset preset = LedPreset::Available;
    if (token == "boot" || token == "booting" || token == "0") {
        preset = LedPreset::Booting;
    } else if (token == "available" || token == "ready" || token == "1") {
        preset = LedPreset::Available;
    } else if (token == "preparing" || token == "prepare" || token == "plugged" || token == "2") {
        preset = LedPreset::Preparing;
    } else if (token == "charging" || token == "charge" || token == "3") {
        preset = LedPreset::Charging;
    } else if (token == "finishing" || token == "finish" || token == "stopping" || token == "4") {
        preset = LedPreset::Finishing;
    } else if (token == "faulted" || token == "fault" || token == "error" || token == "5") {
        preset = LedPreset::Faulted;
    } else if (token == "emergency" || token == "estop" || token == "e-stop" || token == "6") {
        preset = LedPreset::Emergency;
    } else {
        return false;
    }
    if (out) {
        *out = preset;
    }
    return true;
}

void led_fill_active(const CRGB& color) {
    const uint8_t count = active_led_count();
    fill_solid(g_leds, MAX_LED_COUNT, CRGB::Black);
    fill_solid(g_leds, count, color);
}

void anim_solid(const CRGB& color) {
    led_fill_active(color);
}

void anim_blink(const CRGB& color, uint16_t on_ms, uint16_t off_ms, uint32_t now_ms) {
    const uint32_t cycle = static_cast<uint32_t>(on_ms) + static_cast<uint32_t>(off_ms);
    const bool on = cycle > 0u && (now_ms % cycle) < on_ms;
    if (on) {
        led_fill_active(color);
    } else {
        led_fill_active(CRGB::Black);
    }
}

void anim_double_flash(const CRGB& color, uint16_t flash_ms, uint16_t gap_ms, uint16_t pause_ms, uint32_t now_ms) {
    const uint32_t cycle = static_cast<uint32_t>(flash_ms) + gap_ms + flash_ms + pause_ms;
    const uint32_t t = cycle > 0u ? (now_ms % cycle) : 0u;
    const bool on = (t < flash_ms) || (t >= (flash_ms + gap_ms) && t < (flash_ms + gap_ms + flash_ms));
    if (on) {
        led_fill_active(color);
    } else {
        led_fill_active(CRGB::Black);
    }
}

void anim_chase(const CRGB& color, uint8_t width, uint16_t period_ms, uint8_t tail_dim, uint32_t now_ms) {
    const uint8_t count = active_led_count();
    const uint16_t phase = period_ms > 0u ? static_cast<uint16_t>(now_ms % period_ms) : 0u;
    const uint8_t head = period_ms > 0u ? static_cast<uint8_t>((static_cast<uint32_t>(phase) * count) / period_ms) : 0u;
    fill_solid(g_leds, MAX_LED_COUNT, CRGB::Black);
    for (uint8_t k = 0; k < width; ++k) {
        const uint8_t idx = static_cast<uint8_t>((head + k) % count);
        g_leds[idx] = color;
        if (tail_dim != 0u && k > 0u) {
            g_leds[idx].nscale8_video(255u - static_cast<uint8_t>(std::min<int>(254, k * tail_dim)));
        }
    }
}

void anim_theater_chase(const CRGB& color, uint8_t spacing, uint16_t step_ms, uint32_t now_ms) {
    const uint8_t count = active_led_count();
    if (spacing == 0u || step_ms == 0u) {
        anim_solid(color);
        return;
    }
    fill_solid(g_leds, MAX_LED_COUNT, CRGB::Black);
    const uint8_t phase = static_cast<uint8_t>((now_ms / step_ms) % spacing);
    for (uint8_t i = 0; i < count; ++i) {
        g_leds[i] = ((i % spacing) == phase) ? color : CRGB::Black;
    }
}

void anim_battery_fill(const CRGB& color, uint16_t fill_ms, uint16_t hold_full_ms, uint32_t now_ms) {
    const uint8_t count = active_led_count();
    const uint32_t cycle = static_cast<uint32_t>(fill_ms) + static_cast<uint32_t>(hold_full_ms);
    const uint32_t t = cycle > 0u ? (now_ms % cycle) : 0u;
    uint8_t lit = count;
    if (t < fill_ms && fill_ms > 0u) {
        lit = 1u + static_cast<uint8_t>((static_cast<uint32_t>(count) * t) / fill_ms);
        if (lit > count) {
            lit = count;
        }
    }
    fill_solid(g_leds, MAX_LED_COUNT, CRGB::Black);
    for (uint8_t i = 0; i < lit; ++i) {
        g_leds[i] = color;
        if (i + 1u < lit) {
            g_leds[i].nscale8_video(200);
        }
    }
}

void init_status_leds() {
#if !CBPLC_ENABLE_STATUS_LEDS
    g_led_state.initialized = false;
    g_led_state.preset = LedPreset::Booting;
    g_led_state.rendered = LedPreset::Booting;
    return;
#endif
    pinMode(LED_DATA_PIN, OUTPUT);
    FastLED.addLeds<WS2812B, LED_DATA_PIN, GRB>(g_leds, MAX_LED_COUNT);
    FastLED.setBrightness(LED_BRIGHTNESS);
    FastLED.setMaxPowerInVoltsAndMilliamps(5, 2000);
    fill_solid(g_leds, MAX_LED_COUNT, CRGB::Black);
    FastLED.show();
    g_led_state.initialized = true;
    g_led_state.preset = LedPreset::Booting;
    g_led_state.rendered = LedPreset::Booting;
    g_led_state.last_update_ms = millis();
}

void service_status_leds(uint32_t now_ms) {
#if !CBPLC_ENABLE_STATUS_LEDS
    (void)now_ms;
    return;
#endif
    if (!g_led_state.initialized) {
        return;
    }
    const LedPreset preset = effective_led_preset(now_ms);
    const bool animated = preset == LedPreset::Booting || preset == LedPreset::Charging ||
                          preset == LedPreset::Finishing || preset == LedPreset::Faulted ||
                          preset == LedPreset::Emergency;
    const bool preset_changed = preset != g_led_state.rendered;
    if (!preset_changed) {
        if (!animated) {
            return;
        }
        if (g_led_state.last_update_ms != 0u &&
            static_cast<int32_t>(now_ms - g_led_state.last_update_ms) < static_cast<int32_t>(LED_REFRESH_MS)) {
            return;
        }
    }
    switch (preset) {
        case LedPreset::Booting:
            anim_chase(CRGB::Cyan, 2u, 900u, 90u, now_ms);
            break;
        case LedPreset::Available:
            anim_solid(CRGB::Green);
            break;
        case LedPreset::Preparing:
            anim_solid(CRGB::Blue);
            break;
        case LedPreset::Charging:
            anim_battery_fill(CRGB::Blue, 1400u, 300u, now_ms);
            break;
        case LedPreset::Finishing:
            anim_theater_chase(CRGB::Gold, 3u, 120u, now_ms);
            break;
        case LedPreset::Faulted:
            anim_blink(CRGB::OrangeRed, 350u, 350u, now_ms);
            break;
        case LedPreset::Emergency:
            anim_double_flash(CRGB::Red, 120u, 150u, 700u, now_ms);
            break;
    }
    FastLED.show();
    g_led_state.rendered = preset;
    g_led_state.last_update_ms = now_ms;
}
SemaphoreHandle_t g_module_mutex = nullptr;
SemaphoreHandle_t g_relay_mutex = nullptr;
bool g_module_ready = false;
bool g_relay_ready = false;
bool g_relay1_closed = false;
bool g_relay2_closed = false;
bool g_relay3_closed = false;
uint32_t g_relay1_deadline_ms = 0;
uint32_t g_relay2_deadline_ms = 0;
uint32_t g_relay3_deadline_ms = 0;
float g_last_module_target_v = 380.0f;
float g_last_module_target_i = PRECHARGE_CURRENT_LIMIT_A;
float g_last_requested_target_v = 380.0f;
float g_last_requested_target_i = PRECHARGE_CURRENT_LIMIT_A;
float g_assignment_power_limit_kw = 0.0f;
bool g_module_output_enabled = false;
std::vector<std::string> g_module_allowlist{};
std::map<std::string, float> g_active_module_power_limits_kw{};
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

uint32_t effective_controller_heartbeat_timeout_ms() {
    return (g_ctrl.heartbeat_timeout_ms != 0u)
               ? g_ctrl.heartbeat_timeout_ms
               : g_runtime_cfg.controller_heartbeat_timeout_ms;
}

void reset_session_measurements() {
    g_meter_wh_real = 0.0;
    g_meter_last_sample_ms = 0u;
    g_last_measured_v = 0.0f;
    g_last_measured_i = 0.0f;
}
uint32_t g_last_module_runtime_log_ms = 0;

size_t task_stack_free_bytes(TaskHandle_t handle) {
    if (!handle) {
        return 0u;
    }
    return static_cast<size_t>(uxTaskGetStackHighWaterMark(handle));
}

struct HeapSnapshot {
    size_t free_bytes{0u};
    size_t min_free_bytes{0u};
    size_t largest_free_block{0u};
    size_t allocated_blocks{0u};
};

HeapSnapshot capture_heap_snapshot(uint32_t caps) {
    HeapSnapshot snap{};
    multi_heap_info_t info{};
    heap_caps_get_info(&info, caps);
    snap.free_bytes = info.total_free_bytes;
    snap.min_free_bytes = info.minimum_free_bytes;
    snap.largest_free_block = info.largest_free_block;
    snap.allocated_blocks = info.allocated_blocks;
    return snap;
}

unsigned heap_fragmentation_pct(const HeapSnapshot& snap) {
    if (snap.free_bytes == 0u || snap.largest_free_block >= snap.free_bytes) {
        return 0u;
    }
    const size_t fragmented = snap.free_bytes - snap.largest_free_block;
    return static_cast<unsigned>((fragmented * 100u) / snap.free_bytes);
}

unsigned runtime_pct_tenths(uint64_t busy_us, uint64_t window_us) {
    if (busy_us == 0u || window_us == 0u) {
        return 0u;
    }
    return static_cast<unsigned>((busy_us * 1000u) / window_us);
}

void enable_psram_malloc_policy() {
    if (!psramFound()) {
        Serial.println("[MEM] PSRAM not detected; large allocations stay internal");
        return;
    }
    heap_caps_malloc_extmem_enable(PSRAM_BULK_ALLOC_THRESHOLD_BYTES);
    Serial.printf("[MEM] PSRAM bulk threshold=%u bytes\n",
                  static_cast<unsigned>(PSRAM_BULK_ALLOC_THRESHOLD_BYTES));
}

void* alloc_bulk_storage(size_t size, const char* tag) {
    void* ptr = heap_caps_malloc_prefer(size,
                                        2,
                                        MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT,
                                        MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
    if (!ptr) {
        Serial.printf("[MEM] bulk alloc failed tag=%s size=%u\n",
                      tag ? tag : "-",
                      static_cast<unsigned>(size));
        return nullptr;
    }
    memset(ptr, 0, size);
    Serial.printf("[MEM] bulk alloc tag=%s size=%u loc=%s\n",
                  tag ? tag : "-",
                  static_cast<unsigned>(size),
                  esp_ptr_external_ram(ptr) ? "psram" : "internal");
    return ptr;
}

void log_runtime_stats(const char* reason) {
    const HeapSnapshot internal_heap = capture_heap_snapshot(MALLOC_CAP_INTERNAL | MALLOC_CAP_8BIT);
    const bool has_psram = psramFound();
    const HeapSnapshot psram_heap =
        has_psram ? capture_heap_snapshot(MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT) : HeapSnapshot{};

    const bool scheduler_started = xTaskGetSchedulerState() != taskSCHEDULER_NOT_STARTED;
    TaskHandle_t idle0_handle = scheduler_started ? xTaskGetIdleTaskHandleForCPU(0) : nullptr;
#ifndef CONFIG_FREERTOS_UNICORE
    TaskHandle_t idle1_handle = scheduler_started ? xTaskGetIdleTaskHandleForCPU(1) : nullptr;
#else
    TaskHandle_t idle1_handle = nullptr;
#endif
    const uint64_t now_us = esp_timer_get_time();
    const uint64_t window_us =
        (scheduler_started && g_runtime_stats_window_start_us > 0u && now_us > g_runtime_stats_window_start_us)
            ? (now_us - g_runtime_stats_window_start_us)
            : 0u;

    Serial.printf(
        "[MEM] {\"reason\":\"%s\",\"intFree\":%u,\"intMin\":%u,\"intLargest\":%u,\"intFragPct\":%u,"
        "\"intBlocks\":%u,\"psFree\":%u,\"psMin\":%u,\"psLargest\":%u,\"psFragPct\":%u,\"psBlocks\":%u,"
        "\"loopStackFree\":%u,\"hlcStackFree\":%u,\"idle0StackFree\":%u,\"idle1StackFree\":%u,"
        "\"windowMs\":%u,\"loopBusyPct10\":%u,\"hlcBusyPct10\":%u}\n",
        reason ? reason : "-",
        static_cast<unsigned>(internal_heap.free_bytes),
        static_cast<unsigned>(internal_heap.min_free_bytes),
        static_cast<unsigned>(internal_heap.largest_free_block),
        heap_fragmentation_pct(internal_heap),
        static_cast<unsigned>(internal_heap.allocated_blocks),
        static_cast<unsigned>(psram_heap.free_bytes),
        static_cast<unsigned>(psram_heap.min_free_bytes),
        static_cast<unsigned>(psram_heap.largest_free_block),
        heap_fragmentation_pct(psram_heap),
        static_cast<unsigned>(psram_heap.allocated_blocks),
        static_cast<unsigned>(scheduler_started ? task_stack_free_bytes(g_loop_task) : 0u),
        static_cast<unsigned>(scheduler_started ? task_stack_free_bytes(g_hlc_worker_task) : 0u),
        static_cast<unsigned>(scheduler_started ? task_stack_free_bytes(idle0_handle) : 0u),
        static_cast<unsigned>(scheduler_started ? task_stack_free_bytes(idle1_handle) : 0u),
        static_cast<unsigned>(window_us / 1000u),
        runtime_pct_tenths(g_loop_busy_us, window_us),
        runtime_pct_tenths(g_hlc_busy_us, window_us));
}

void service_runtime_stats(uint32_t now_ms) {
    if (g_next_runtime_stats_ms == 0u) {
        g_next_runtime_stats_ms = now_ms + RUNTIME_STATS_PERIOD_MS;
        g_runtime_stats_window_start_us = esp_timer_get_time();
        return;
    }
    if (static_cast<int32_t>(now_ms - g_next_runtime_stats_ms) < 0) {
        return;
    }
    log_runtime_stats("periodic");
    g_next_runtime_stats_ms = now_ms + RUNTIME_STATS_PERIOD_MS;
    g_runtime_stats_window_start_us = esp_timer_get_time();
    g_loop_busy_us = 0u;
    g_hlc_busy_us = 0u;
}

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
    uint8_t aux{1u};
    uint8_t session{1u};
    uint8_t feedback{1u};
};

SerialControllerSeqState g_serial_ctrl_seq{};
uint32_t can_with_plc_id(uint32_t base_id) {
    return (base_id & 0x1FFFFFF0u) | static_cast<uint32_t>(g_runtime_cfg.can_node_id & 0x0Fu);
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

uint8_t normalize_can_node_id(uint8_t can_node_id, uint8_t plc_id) {
    if (can_node_id == 0u) return plc_id;
    return static_cast<uint8_t>(std::max<uint8_t>(1u, std::min<uint8_t>(0x0Fu, can_node_id)));
}

uint8_t normalize_local_module_address(uint8_t module_address, uint8_t plc_id) {
    if (module_address == 0u) return plc_id;
    return module_address;
}

OperatingMode normalize_mode(uint8_t mode_raw) {
    switch (mode_raw) {
        case static_cast<uint8_t>(OperatingMode::Standalone):
            return OperatingMode::Standalone;
        case static_cast<uint8_t>(OperatingMode::ExternalController):
        case 2u:
        case 3u:
        default:
            return OperatingMode::ExternalController;
    }
}

OperatingMode runtime_mode() {
    return normalize_mode(g_runtime_cfg.mode);
}

bool mode_is_local_autonomous() {
    return runtime_mode() == OperatingMode::Standalone;
}

bool mode_requires_controller_contract() {
    return runtime_mode() == OperatingMode::ExternalController;
}

bool mode_controller_routes_power_externally_over_uart() {
    return mode_requires_controller_contract();
}

bool mode_controller_owns_power_path() {
    return mode_requires_controller_contract();
}

bool mode_controller_uses_external_feedback() {
    return mode_controller_owns_power_path();
}

bool mode_uses_plc_module_manager() {
    return !mode_controller_routes_power_externally_over_uart();
}

bool mode_uses_plc_can_stack() {
    return !mode_controller_routes_power_externally_over_uart();
}

bool mode_uart_status_stream_active() {
    return mode_controller_routes_power_externally_over_uart();
}

bool mode_controller_has_final_decision() {
    return mode_controller_owns_power_path();
}

const char* runtime_mode_name(OperatingMode mode) {
    switch (mode) {
        case OperatingMode::Standalone:
            return "standalone";
        case OperatingMode::ExternalController:
            return "external_controller";
        default:
            return "unknown";
    }
}

const char* runtime_mode_name() {
    return runtime_mode_name(runtime_mode());
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

std::string runtime_module_id_for_addr(uint8_t module_addr) {
    if (module_addr == g_runtime_cfg.local_module_address) {
        return runtime_local_module_id();
    }
    char module_buf[24];
    snprintf(module_buf, sizeof(module_buf), "LEASED_MXR_%02X", static_cast<unsigned>(module_addr));
    return std::string(module_buf);
}

uint8_t module_addr_from_id(const std::string& module_id) {
    for (const auto& [addr, known_id] : g_known_module_ids) {
        if (known_id == module_id) {
            return addr;
        }
    }
    return 0u;
}

cbmodules::ModuleSpec build_module_spec_for_addr(uint8_t module_addr) {
    cbmodules::ModuleSpec spec;
    spec.id = runtime_module_id_for_addr(module_addr);
    spec.type = cbmodules::ModuleType::MaxwellMxr;
    spec.slot_id = g_runtime_cfg.connector_id;
    spec.slot_index = g_runtime_cfg.connector_id;
    spec.address = module_addr;
    uint8_t module_can_group = DEFAULT_LOCAL_MODULE_CAN_GROUP;
    if (module_addr == g_runtime_cfg.local_module_address) {
        module_can_group = g_local_module_group;
    } else {
        const auto it = g_known_module_can_groups.find(module_addr);
        if (it != g_known_module_can_groups.end()) {
            module_can_group = static_cast<uint8_t>(std::max<uint8_t>(1u, std::min<uint8_t>(MAX_MODULE_GROUP_ID, it->second)));
        }
    }
    spec.group = module_can_group;
    spec.rated_current_a = MODULE_MAX_CURRENT_A;
    spec.rated_power_kw = MODULE_MAX_POWER_KW;
    spec.min_operating_voltage_v = 200.0f;
    spec.max_operating_voltage_v = MODULE_MAX_VOLTAGE_V;
    return spec;
}

float effective_module_limit_kw_for_id(const std::string& module_id) {
    const auto active_it = g_active_module_power_limits_kw.find(module_id);
    if (active_it != g_active_module_power_limits_kw.end() && active_it->second > 0.0f) {
        return active_it->second;
    }
    return MODULE_MAX_POWER_KW;
}


bool ensure_known_module_registered(uint8_t module_addr) {
    if (module_addr == 0u) {
        return false;
    }
    if (module_addr != g_runtime_cfg.local_module_address &&
        g_known_module_can_groups.find(module_addr) == g_known_module_can_groups.end()) {
        return false;
    }
    const auto spec = build_module_spec_for_addr(module_addr);
    if (!g_module_mgr.upsert_module(spec)) {
        return false;
    }
    if (module_addr != g_runtime_cfg.local_module_address) {
        const auto now_ms = millis();
        const auto states = g_module_mgr.module_states();
        for (const auto& state : states) {
            if (state.id != spec.id) {
                continue;
            }
            const bool telemetry_stale =
                state.telemetry.last_update_ms == 0 || now_ms < state.telemetry.last_update_ms ||
                (now_ms - state.telemetry.last_update_ms) > MODULE_TELEMETRY_FRESH_MS;
            const bool inactive_shadow = state.group_id.empty() && !state.lease_active;
            const bool poisoned_shadow = state.isolation_latched || state.telemetry.faulted;
            if (inactive_shadow && (poisoned_shadow || telemetry_stale)) {
                (void)g_module_mgr.reset_module_runtime_state(spec.id);
            }
            break;
        }
    }
    g_module_mgr.set_module_hard_limits(spec.id, MODULE_MAX_CURRENT_A, effective_module_limit_kw_for_id(spec.id));
    g_known_module_ids[module_addr] = spec.id;
    return true;
}

void prune_known_remote_modules(const std::vector<std::string>& active_allowlist) {
    if (!g_module_ready) {
        return;
    }
    for (const auto& [addr, known_id] : g_known_module_ids) {
        if (addr == g_runtime_cfg.local_module_address) {
            continue;
        }
        if (std::find(active_allowlist.begin(), active_allowlist.end(), known_id) != active_allowlist.end()) {
            continue;
        }
        // Keep remote module objects and addressing warm across lease changes.
        // The active allowlist is the ownership truth; removing a module from the
        // allowlist must stop using it, but it should not force a cold rediscovery
        // the next time the controller leases it back in.
        g_module_mgr.set_module_hard_limits(known_id, MODULE_MAX_CURRENT_A, effective_module_limit_kw_for_id(known_id));
    }
}

bool module_addr_allowed_for_rx(uint8_t module_addr) {
    if (module_addr == 0u) {
        return false;
    }
    if (module_addr == g_runtime_cfg.local_module_address) {
        return true;
    }
    for (const auto& id : g_module_allowlist) {
        if (module_addr_from_id(id) == module_addr) {
            return true;
        }
    }
    return false;
}

bool local_group_released_locked() {
    const auto status = g_module_mgr.group_status(runtime_local_group_id());
    return status.assigned_modules == 0u && status.active_modules == 0u;
}

bool local_group_released() {
    if (!g_module_ready) {
        return true;
    }
    if (!lock_modules(20)) {
        return false;
    }
    g_module_mgr.poll_rx(8);
    g_module_mgr.tick(millis());
    const bool released = local_group_released_locked();
    unlock_modules();
    return released;
}

bool is_allowed_module_can_frame(uint32_t id, bool extended, uint8_t dlc, const uint8_t data[8]) {
    if (!extended) return false;
    const uint32_t raw_id = id & 0x1FFFFFFFu;
    const uint16_t proto = static_cast<uint16_t>((raw_id >> 20) & 0x1FFu);
    if (proto == MAXWELL_CAN_PROTOCOL_ID) {
        const uint8_t src = static_cast<uint8_t>((raw_id >> 3) & 0xFFu);
        return module_addr_allowed_for_rx(src);
    }

    if (!data || dlc < 8u) return false;
    const uint32_t family = raw_id & MODULE_OWNERSHIP_CAN_ID_MASK;
    if (family == MODULE_OWNERSHIP_CAN_ID_BASE || family == MODULE_OWNERSHIP_EPOCH_CAN_ID_BASE) {
        const auto type = static_cast<cbmodules::ModuleType>(data[1]);
        return type == cbmodules::ModuleType::MaxwellMxr && module_addr_allowed_for_rx(data[2]);
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
    if (target_plc != static_cast<uint32_t>(g_runtime_cfg.can_node_id & 0x0Fu)) return;
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
    g_ctrl.last_seq_aux = 0;
    g_ctrl.last_seq_session = 0;
    g_ctrl.last_seq_feedback = 0;
    g_ctrl.seq_seen_auth = false;
    g_ctrl.seq_seen_slac = false;
    g_ctrl.seq_seen_aux = false;
    g_ctrl.seq_seen_session = false;
    g_ctrl.seq_seen_feedback = false;
}

void controller_clear_managed_power_cache() {
    // Bridge mode no longer accepts PLC-side power setpoints. Keep the reset hook
    // for call-site simplicity, but there is no managed-power cache to clear.
}

void controller_clear_managed_feedback_cache() {
    g_ctrl_feedback = ControllerManagedFeedbackState{};
}

bool controller_feedback_fresh(uint32_t now_ms) {
    if (!g_ctrl_feedback.seen || now_ms < g_ctrl_feedback.last_update_ms) {
        return false;
    }
    if (mode_controller_routes_power_externally_over_uart()) {
        const uint32_t timeout_ms =
            std::min<uint32_t>(5000u,
                               std::max<uint32_t>(CONTROLLER_FEEDBACK_TIMEOUT_MS,
                                                  effective_controller_heartbeat_timeout_ms() + 500u));
        return (now_ms - g_ctrl_feedback.last_update_ms) <= timeout_ms;
    }
    if (mode_controller_has_final_decision()) {
        return true;
    }
    const uint32_t timeout_ms =
        std::min<uint32_t>(5000u,
                           std::max<uint32_t>(CONTROLLER_FEEDBACK_TIMEOUT_MS,
                                              effective_controller_heartbeat_timeout_ms() + 500u));
    return (now_ms - g_ctrl_feedback.last_update_ms) <= timeout_ms;
}

bool controller_heartbeat_alive(uint32_t now_ms) {
    if (!mode_requires_controller_contract()) return true;
    if (!g_ctrl.heartbeat_seen) return false;
    if (now_ms < g_ctrl.last_heartbeat_ms) return false;
    const uint32_t age = now_ms - g_ctrl.last_heartbeat_ms;
    return age <= effective_controller_heartbeat_timeout_ms();
}

bool controller_auth_granted(uint32_t now_ms) {
    if (!mode_requires_controller_contract()) return true;
    if (g_ctrl.auth_state != ControllerAuthState::Granted) return false;
    if (g_ctrl.auth_expiry_ms == 0) return false;
    return static_cast<int32_t>(now_ms - g_ctrl.auth_expiry_ms) <= 0;
}

void controller_clear_slac_contract() {
    g_ctrl.slac_armed = false;
    g_ctrl.slac_start_latched = false;
    g_ctrl.slac_plc_owned = false;
    g_ctrl.slac_arm_expiry_ms = 0u;
}

bool controller_allows_slac_start(uint32_t now_ms) {
    if (g_emergency_stop_active) return false;
    if (!mode_requires_controller_contract()) return true;
    if (mode_controller_has_final_decision()) {
        if (!g_ctrl.slac_armed) return false;
        if (g_ctrl.slac_plc_owned && g_ctrl.slac_start_latched) {
            return true;
        }
        if (g_ctrl.slac_arm_expiry_ms == 0u) return false;
        if (static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) > 0) return false;
        if (!g_runtime_cfg.slac_requires_controller_start) return true;
        return g_ctrl.slac_start_latched;
    }
    if (!g_runtime_cfg.slac_requires_controller_start) return controller_heartbeat_alive(now_ms);
    if (!controller_heartbeat_alive(now_ms)) return false;
    if (!g_ctrl.slac_start_latched) return false;
    if (g_ctrl.slac_arm_expiry_ms == 0) return false;
    return static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) <= 0;
}

bool controller_requests_slac_pwm(uint32_t now_ms) {
    if (!mode_requires_controller_contract()) {
        return false;
    }
    if (g_ctrl.stop_active) {
        return false;
    }
    if (g_session_started || g_session_matched || g_hlc_ready || g_hlc_active) {
        return false;
    }
    return controller_allows_slac_start(now_ms);
}

bool controller_allows_energy(uint32_t now_ms) {
    if (g_emergency_stop_active) return false;
    if (!mode_requires_controller_contract()) return true;
    if (mode_controller_routes_power_externally_over_uart()) {
        const bool granted = controller_auth_granted(now_ms);
        if (granted) {
            g_ctrl.last_energy_allowed_ms = now_ms;
        }
        return granted;
    }
    if (mode_controller_has_final_decision()) {
        const bool granted = g_ctrl.auth_state == ControllerAuthState::Granted && !g_module_allowlist.empty();
        if (granted) {
            g_ctrl.last_energy_allowed_ms = now_ms;
        }
        return granted;
    }
    const bool direct_allow =
        controller_heartbeat_alive(now_ms) && controller_auth_granted(now_ms) && !g_module_allowlist.empty();
    if (direct_allow) {
        g_ctrl.last_energy_allowed_ms = now_ms;
        return true;
    }
    if (g_ctrl.auth_state == ControllerAuthState::Denied || g_module_allowlist.empty()) {
        return false;
    }
    const bool active_session = g_session_started || g_hlc_active || g_relay1_closed || g_module_output_enabled;
    if (!active_session || g_ctrl.last_energy_allowed_ms == 0u || now_ms < g_ctrl.last_energy_allowed_ms) {
        return false;
    }
    return (now_ms - g_ctrl.last_energy_allowed_ms) <= CONTROLLER_ENERGY_HOLD_GRACE_MS;
}

bool precharge_voltage_converged(float requested_v, float present_v) {
    const float target_v = std::isfinite(requested_v) ? std::max(0.0f, requested_v) : 0.0f;
    const float measured_v = std::isfinite(present_v) ? std::max(0.0f, present_v) : 0.0f;
    if (target_v <= 1.0f) {
        return measured_v <= PRECHARGE_VOLTAGE_TOLERANCE_V;
    }
    const float tolerance_v = std::max(PRECHARGE_VOLTAGE_TOLERANCE_V, target_v * 0.05f);
    return std::fabs(measured_v - target_v) <= tolerance_v || measured_v >= (target_v - tolerance_v);
}

bool snapshot_live_present_values(cbmodules::GroupStatus* out_st,
                                  float* out_present_v,
                                  float* out_present_i);

bool standalone_precharge_start_ready(HlcAppContext* ctx, float requested_v) {
    if (!ctx || mode_requires_controller_contract()) {
        return false;
    }
    if (!ctx->precharge_seen || !g_relay1_closed || !g_module_output_enabled) {
        return false;
    }
    cbmodules::GroupStatus st{};
    float present_v = 0.0f;
    float present_i = 0.0f;
    (void)snapshot_live_present_values(&st, &present_v, &present_i);
    if (precharge_voltage_converged(requested_v, present_v)) {
        return true;
    }
    const float target_v = std::isfinite(requested_v) ? std::max(0.0f, requested_v) : 0.0f;
    const float accept_floor_v = std::max(PRECHARGE_START_ACCEPT_MIN_V, target_v * PRECHARGE_START_ACCEPT_MIN_RATIO);
    const bool current_flow_ready =
        ctx->precharge_count >= PRECHARGE_RELAX_MIN_COUNT && present_i >= PRECHARGE_START_ACCEPT_MIN_CURRENT_A;
    const bool relaxed_ready = present_v >= accept_floor_v || current_flow_ready;
    if (relaxed_ready) {
        Serial.printf("[HLC] {\"msg\":\"PreChargeRelaxedStart\",\"targetV\":%.1f,\"presentV\":%.1f,"
                      "\"presentI\":%.2f,\"acceptFloorV\":%.1f,\"minCurrentA\":%.2f,\"prechargeCount\":%lu,"
                      "\"relay1\":%d,\"moduleOn\":%d}\n",
                      target_v,
                      present_v,
                      present_i,
                      accept_floor_v,
                      PRECHARGE_START_ACCEPT_MIN_CURRENT_A,
                      static_cast<unsigned long>(ctx->precharge_count),
                      g_relay1_closed ? 1 : 0,
                      g_module_output_enabled ? 1 : 0);
    }
    return relaxed_ready;
}

bool controller_precharge_start_ready(HlcAppContext* ctx,
                                      float requested_v,
                                      const ControllerManagedFeedbackState& fb) {
    if (!ctx || !mode_controller_uses_external_feedback()) {
        return false;
    }
    if (!fb.valid || !g_relay1_closed) {
        return false;
    }

    const float present_v =
        std::isfinite(fb.present_voltage_v) ? std::max(0.0f, fb.present_voltage_v) : 0.0f;
    const float present_i =
        std::isfinite(fb.present_current_a) ? std::max(0.0f, fb.present_current_a) : 0.0f;
    if (fb.ready || precharge_voltage_converged(requested_v, present_v)) {
        return true;
    }

    const float target_v = std::isfinite(requested_v) ? std::max(0.0f, requested_v) : 0.0f;
    const float accept_floor_v = std::max(PRECHARGE_START_ACCEPT_MIN_V, target_v * PRECHARGE_START_ACCEPT_MIN_RATIO);
    const bool current_flow_ready =
        ctx->precharge_count >= PRECHARGE_RELAX_MIN_COUNT && present_i >= PRECHARGE_START_ACCEPT_MIN_CURRENT_A;
    const bool relaxed_ready = present_v >= accept_floor_v || current_flow_ready;
    if (relaxed_ready) {
        Serial.printf("[HLC] {\"msg\":\"CtrlPreChargeRelaxedStart\",\"targetV\":%.1f,\"presentV\":%.1f,"
                      "\"presentI\":%.2f,\"acceptFloorV\":%.1f,\"minCurrentA\":%.2f,\"prechargeCount\":%lu,"
                      "\"relay1\":%d,\"fbReady\":%d}\n",
                      target_v,
                      present_v,
                      present_i,
                      accept_floor_v,
                      PRECHARGE_START_ACCEPT_MIN_CURRENT_A,
                      static_cast<unsigned long>(ctx->precharge_count),
                      g_relay1_closed ? 1 : 0,
                      fb.ready ? 1 : 0);
    }
    return relaxed_ready;
}

void send_ctrl_ack(uint8_t cmd_type, uint8_t seq, uint8_t status, uint8_t detail0, uint8_t detail1) {
    uint8_t p[8]{};
    p[0] = static_cast<uint8_t>(CTRL_MSG_VERSION);
    p[1] = g_serial_tx.ack_seq++;
    p[2] = cmd_type;
    p[3] = seq;
    p[4] = status;
    p[5] = detail0;
    p[6] = detail1;
    Serial.printf("[SERCTRL] ACK cmd=0x%02X seq=%u status=%u detail0=%u detail1=%u\n",
                  static_cast<unsigned>(cmd_type),
                  static_cast<unsigned>(seq),
                  static_cast<unsigned>(status),
                  static_cast<unsigned>(detail0),
                  static_cast<unsigned>(detail1));
}

uint32_t ctrl_unpack_u32_le(const uint8_t* data) {
    if (!data) return 0u;
    return static_cast<uint32_t>(data[0]) |
           (static_cast<uint32_t>(data[1]) << 8u) |
           (static_cast<uint32_t>(data[2]) << 16u) |
           (static_cast<uint32_t>(data[3]) << 24u);
}

uint16_t encode_voltage_d01(float voltage_v, uint16_t max_raw) {
    const float sane_voltage = std::isfinite(voltage_v) ? std::max(0.0f, voltage_v) : 0.0f;
    const float clamped = std::max(0.0f, std::min(static_cast<float>(max_raw) / 10.0f, sane_voltage));
    return static_cast<uint16_t>(std::lround(clamped * 10.0f));
}

uint16_t encode_current_d01(float current_a, uint16_t max_raw) {
    const float sane_current = std::isfinite(current_a) ? std::max(0.0f, current_a) : 0.0f;
    const float clamped = std::max(0.0f, std::min(static_cast<float>(max_raw) / 10.0f, sane_current));
    return static_cast<uint16_t>(std::lround(clamped * 10.0f));
}

uint32_t pack_ctrl_hlc_feedback(bool valid,
                                bool ready,
                                bool current_limit,
                                bool voltage_limit,
                                bool power_limit,
                                bool stop_charging,
                                float present_voltage_v,
                                float present_current_a) {
    const uint32_t v01 = encode_voltage_d01(present_voltage_v, 0x3FFFu);
    const uint32_t i01 = encode_current_d01(present_current_a, 0x0FFFu);
    return static_cast<uint32_t>(ready ? 1u : 0u) |
           (static_cast<uint32_t>(current_limit ? 1u : 0u) << 1u) |
           (static_cast<uint32_t>(voltage_limit ? 1u : 0u) << 2u) |
           (static_cast<uint32_t>(power_limit ? 1u : 0u) << 3u) |
           ((v01 & 0x3FFFu) << 4u) |
           ((i01 & 0x0FFFu) << 18u) |
           (static_cast<uint32_t>(valid ? 1u : 0u) << 30u) |
           (static_cast<uint32_t>(stop_charging ? 1u : 0u) << 31u);
}

void unpack_ctrl_hlc_feedback(uint32_t packed,
                              bool* valid,
                              bool* ready,
                              bool* current_limit,
                              bool* voltage_limit,
                              bool* power_limit,
                              bool* stop_charging,
                              float* present_voltage_v,
                              float* present_current_a) {
    if (ready) *ready = (packed & 0x01u) != 0u;
    if (current_limit) *current_limit = (packed & 0x02u) != 0u;
    if (voltage_limit) *voltage_limit = (packed & 0x04u) != 0u;
    if (power_limit) *power_limit = (packed & 0x08u) != 0u;
    if (present_voltage_v) *present_voltage_v = static_cast<float>((packed >> 4u) & 0x3FFFu) / 10.0f;
    if (present_current_a) *present_current_a = static_cast<float>((packed >> 18u) & 0x0FFFu) / 10.0f;
    if (valid) *valid = (packed & (1u << 30u)) != 0u;
    if (stop_charging) *stop_charging = (packed & (1u << 31u)) != 0u;
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

bool standalone_module_fault_active() {
    if (!mode_uses_plc_module_manager() || !g_module_ready) {
        return false;
    }
    const auto states = g_module_mgr.module_states();
    for (const auto& s : states) {
        if (s.lifecycle == cbmodules::ModuleLifecycle::Faulted ||
            s.lifecycle == cbmodules::ModuleLifecycle::Quarantined ||
            s.telemetry.faulted) {
            return true;
        }
    }
    return false;
}

bool standalone_energy_path_active() {
    return g_relay1_closed || g_module_output_enabled;
}

bool standalone_disconnect_hold_active(uint32_t now_ms) {
    if (g_hlc_disconnect_hold_until_ms == 0u) {
        return false;
    }
    return static_cast<int32_t>(now_ms - g_hlc_disconnect_hold_until_ms) < 0;
}

LedPreset standalone_led_preset(uint32_t now_ms) {
    if (standalone_module_fault_active()) {
        return LedPreset::Faulted;
    }

    const bool cp_is_connected = cp_connected(g_cp_state);
    const bool energy_path_active = standalone_energy_path_active();
    const bool charging_active =
        energy_path_active &&
        (g_hlc_ctx.power_delivery_enabled || sane_non_negative(g_last_measured_i) >= 1.0f ||
         (sane_non_negative(g_last_measured_v) >= 50.0f && sane_non_negative(g_last_measured_i) >= 0.2f));
    if (charging_active) {
        return LedPreset::Charging;
    }

    const bool finishing =
        g_hlc_ctx.stop_requested || standalone_disconnect_hold_active(now_ms) ||
        ((g_hlc_ctx.precharge_seen || energy_path_active || g_hlc_active) && !cp_is_connected);
    if (finishing) {
        return LedPreset::Finishing;
    }

    const bool preparing =
        cp_is_connected || g_session_started || g_session_matched || g_hlc_ready || g_hlc_active ||
        g_hlc_ctx.precharge_seen || g_hlc_ctx.power_delivery_enabled || energy_path_active;
    if (preparing) {
        return LedPreset::Preparing;
    }

    if (now_ms < 3000u) {
        return LedPreset::Booting;
    }
    return LedPreset::Available;
}

LedPreset effective_led_preset(uint32_t now_ms) {
    if (g_emergency_stop_active) {
        return LedPreset::Emergency;
    }
    if (now_ms == 0u) {
        now_ms = millis();
    }
    if (!mode_controller_has_final_decision()) {
        return standalone_led_preset(now_ms);
    }
    return g_led_state.preset;
}

float clamp_current_for_assignment_limit(float voltage_v, float current_a) {
    const float sane_current = sane_non_negative(current_a);
    if (g_assignment_power_limit_kw <= 0.0f) {
        return sane_current;
    }
    const float sane_voltage = std::max(50.0f, sane_non_negative(voltage_v));
    const float limit_current_a = (g_assignment_power_limit_kw * 1000.0f) / sane_voltage;
    return std::max(0.0f, std::min(sane_current, limit_current_a));
}

void clear_last_bms_request() {
    g_last_bms_requested_v = 0.0f;
    g_last_bms_requested_i = 0.0f;
    g_last_requested_target_v = 0.0f;
    g_last_requested_target_i = 0.0f;
    g_last_bms_hlc_stage = 0u;
    g_last_bms_valid = false;
    g_last_bms_delivery_ready = false;
    g_last_bms_update_ms = millis();
    g_last_bms_dirty = true;
}

void update_last_bms_request(float req_v, float req_i, uint8_t hlc_stage, bool delivery_ready, bool valid) {
    const float sane_v = sane_non_negative(req_v);
    const float sane_i = sane_non_negative(req_i);
    g_last_bms_requested_v = sane_v;
    g_last_bms_requested_i = sane_i;
    g_last_requested_target_v = sane_v;
    g_last_requested_target_i = sane_i;
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

void set_iso_dc_status_ready(iso2_DC_EVSEStatusType* st,
                             iso2_EVSENotificationType notification = iso2_EVSENotificationType_None) {
    if (!st) return;
    init_iso2_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = iso2_DC_EVSEStatusCodeType_EVSE_Ready;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_iso_dc_status_not_ready(iso2_DC_EVSEStatusType* st,
                                 iso2_EVSENotificationType notification = iso2_EVSENotificationType_None) {
    if (!st) return;
    init_iso2_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = iso2_DC_EVSEStatusCodeType_EVSE_NotReady;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_iso_dc_status_shutdown(iso2_DC_EVSEStatusType* st,
                                iso2_EVSENotificationType notification = iso2_EVSENotificationType_StopCharging) {
    if (!st) return;
    init_iso2_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = iso2_DC_EVSEStatusCodeType_EVSE_Shutdown;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_din_dc_status_ready(din_DC_EVSEStatusType* st,
                             din_EVSENotificationType notification = din_EVSENotificationType_None) {
    if (!st) return;
    init_din_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = din_DC_EVSEStatusCodeType_EVSE_Ready;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_din_dc_status_not_ready(din_DC_EVSEStatusType* st,
                                 din_EVSENotificationType notification = din_EVSENotificationType_None) {
    if (!st) return;
    init_din_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = din_DC_EVSEStatusCodeType_EVSE_NotReady;
    st->EVSEIsolationStatus_isUsed = 0;
}

void set_din_dc_status_shutdown(din_DC_EVSEStatusType* st,
                                din_EVSENotificationType notification = din_EVSENotificationType_StopCharging) {
    if (!st) return;
    init_din_DC_EVSEStatusType(st);
    st->NotificationMaxDelay = 1;
    st->EVSENotification = notification;
    st->EVSEStatusCode = din_DC_EVSEStatusCodeType_EVSE_Shutdown;
    st->EVSEIsolationStatus_isUsed = 0;
}

void update_real_meter_sample(float voltage_v, float current_a, uint32_t now_ms) {
    const float v = sane_non_negative(voltage_v);
    const float i = sane_non_negative(current_a);
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

void update_real_meter_with_status(const cbmodules::GroupStatus& st, uint32_t now_ms) {
    update_real_meter_sample(st.combined_voltage_v, st.combined_current_a, now_ms);
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
    const float target_v = sane_non_negative(st->applied_voltage_v);
    auto voltage_sample_ready = [target_v](const cbmodules::ModuleStateView& s) {
        if (!s.telemetry.online) {
            return false;
        }
        const float current_a = sane_non_negative(s.telemetry.current_a);
        const float voltage_v = sane_non_negative(s.telemetry.voltage_v);
        const bool live_output_sample =
            !s.telemetry.module_off || current_a >= PRECHARGE_START_ACCEPT_MIN_CURRENT_A ||
            voltage_v >= PRECHARGE_START_ACCEPT_MIN_V;
        if (!live_output_sample) {
            return false;
        }
        if (current_a >= PRECHARGE_START_ACCEPT_MIN_CURRENT_A) {
            return true;
        }
        if (target_v <= 50.0f) {
            return voltage_v >= PRECHARGE_START_ACCEPT_MIN_V;
        }
        const float required_voltage = std::max(50.0f, target_v * 0.85f);
        return voltage_v >= required_voltage;
    };
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
                if (v > 0.1f && voltage_sample_ready(s)) {
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

bool module_telemetry_fresh(const cbmodules::ModuleStateView& s, uint32_t now_ms) {
    if (s.telemetry.last_update_ms == 0 || now_ms < s.telemetry.last_update_ms) {
        return false;
    }
    return (now_ms - s.telemetry.last_update_ms) <= MODULE_TELEMETRY_FRESH_MS;
}

bool local_module_isolation_clearable(const cbmodules::ModuleStateView& s, uint32_t now_ms) {
    if (!s.isolation_latched) return false;
    if (s.id != runtime_local_module_id()) return false;
    if (s.lease_active || !s.group_id.empty()) return false;
    if (s.telemetry.faulted) return false;
    if (!s.telemetry.online || !module_telemetry_fresh(s, now_ms)) return false;
    if (!s.telemetry.module_off) return false;
    if (std::fabs(s.telemetry.current_a) > 0.20f) return false;
    if (g_module_output_enabled || std::fabs(g_last_module_target_i) > 0.05f) return false;
    if (g_relay1_closed) return false;
    return true;
}

bool try_clear_local_module_isolation_locked(uint32_t now_ms, const char* reason) {
    const auto states = g_module_mgr.module_states();
    for (const auto& s : states) {
        if (!local_module_isolation_clearable(s, now_ms)) {
            continue;
        }
        if (!g_module_mgr.clear_module_isolation(s.id)) {
            return false;
        }
        Serial.printf("[MOD] cleared local isolation (%s) id=%s\n",
                      reason ? reason : "Recovery",
                      s.id.c_str());
        return true;
    }
    return false;
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

float snapshot_present_group_voltage_v() {
    cbmodules::GroupStatus st{};
    (void)snapshot_group_status(&st, nullptr);
    float present_v = sane_non_negative(st.combined_voltage_v);
    if (present_v <= 0.1f) {
        present_v = sane_non_negative(g_last_measured_v);
    }
    return present_v;
}

bool refresh_precharge_convergence(HlcAppContext* ctx, float requested_v) {
    if (!ctx) {
        return false;
    }
    cbmodules::GroupStatus st{};
    (void)snapshot_group_status(&st, nullptr);
    float present_v = sane_non_negative(st.combined_voltage_v);
    if (present_v <= 0.1f) {
        present_v = sane_non_negative(g_last_measured_v);
    }
    const bool converged = controller_allows_energy(millis()) && !g_module_allowlist.empty() && st.assigned_modules > 0u &&
                           precharge_voltage_converged(requested_v, present_v);
    ctx->precharge_converged = converged;
    return converged;
}

bool snapshot_live_present_values(cbmodules::GroupStatus* out_st,
                                  float* out_present_v,
                                  float* out_present_i) {
    cbmodules::GroupStatus st{};
    const bool ok = snapshot_group_status(&st, nullptr);
    float present_v = sane_non_negative(st.combined_voltage_v);
    float present_i = sane_non_negative(st.combined_current_a);
    if (present_v <= 0.1f) {
        present_v = sane_non_negative(g_last_measured_v);
    }
    if (present_i <= 0.05f) {
        present_i = sane_non_negative(g_last_measured_i);
    }
    if (out_st) *out_st = st;
    if (out_present_v) *out_present_v = present_v;
    if (out_present_i) *out_present_i = present_i;
    return ok && st.valid;
}

float snapshot_welding_present_voltage_v(uint32_t now_ms) {
    if (!g_relay1_closed) {
        return 0.0f;
    }
    if (mode_controller_uses_external_feedback()) {
        ControllerManagedFeedbackState fb{};
        if (controller_get_managed_feedback(now_ms, &fb)) {
            return sane_non_negative(fb.present_voltage_v);
        }
        return sane_non_negative(g_ctrl_feedback.present_voltage_v);
    }
    float present_v = snapshot_present_group_voltage_v();
    if (present_v <= 0.1f) {
        present_v = sane_non_negative(g_last_measured_v);
    }
    return sane_non_negative(present_v);
}

uint32_t latest_local_group_telemetry_ms() {
    const auto states = g_module_mgr.module_states();
    uint32_t latest_ms = 0;
    for (const auto& s : states) {
        if (s.group_id != runtime_local_group_id()) continue;
        latest_ms = std::max(latest_ms, s.telemetry.last_update_ms);
        latest_ms = std::max(latest_ms, s.telemetry.last_voltage_update_ms);
        latest_ms = std::max(latest_ms, s.telemetry.last_current_update_ms);
    }
    return latest_ms;
}

bool snapshot_post_target_present_values(uint32_t baseline_telemetry_ms,
                                         cbmodules::GroupStatus* out_st,
                                         float* out_present_v,
                                         float* out_present_i,
                                         bool* out_fresh_after_target) {
    const uint32_t started_ms = millis();
    bool fresh_after_target = false;
    bool telemetry_valid = false;
    for (;;) {
        telemetry_valid = snapshot_live_present_values(out_st, out_present_v, out_present_i);
        const uint32_t latest_ms = latest_local_group_telemetry_ms();
        fresh_after_target = latest_ms != 0u && latest_ms > baseline_telemetry_ms;
        const uint32_t now_ms = millis();
        const bool expired = (now_ms - started_ms) >= CURRENT_DEMAND_FRESH_SAMPLE_TIMEOUT_MS;
        if (fresh_after_target || expired) {
            if (out_fresh_after_target) {
                *out_fresh_after_target = fresh_after_target;
            }
            return telemetry_valid;
        }
        delay(CURRENT_DEMAND_FRESH_SAMPLE_POLL_MS);
    }
}

uint16_t copy_iso_evse_id(char* out_chars) {
    if (!out_chars) {
        return 0u;
    }
    const size_t evse_len = strnlen(ISO_EVSE_ID, iso2_EVSEID_CHARACTER_SIZE - 1);
    memcpy(out_chars, ISO_EVSE_ID, evse_len);
    return static_cast<uint16_t>(evse_len);
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
            default:
                break;
        }
    }
}

const char* hlc_protocol_name(int protocol) {
    switch (protocol) {
        case JPV2G_PROTOCOL_ISO15118_2:
            return "iso2";
        case JPV2G_PROTOCOL_DIN70121:
            return "din";
        default:
            return "unknown";
    }
}

const char* hlc_message_type_name(jpv2g_message_type_t type) {
    switch (type) {
        case JPV2G_SESSION_SETUP_REQ:
            return "SessionSetupReq";
        case JPV2G_AUTHORIZATION_REQ:
            return "AuthorizationReq";
        case JPV2G_CHARGE_PARAMETER_DISCOVERY_REQ:
            return "ChargeParameterDiscoveryReq";
        case JPV2G_CABLE_CHECK_REQ:
            return "CableCheckReq";
        case JPV2G_PRE_CHARGE_REQ:
            return "PreChargeReq";
        case JPV2G_POWER_DELIVERY_REQ:
            return "PowerDeliveryReq";
        case JPV2G_CURRENT_DEMAND_REQ:
            return "CurrentDemandReq";
        case JPV2G_WELDING_DETECTION_REQ:
            return "WeldingDetectionReq";
        case JPV2G_SESSION_STOP_REQ:
            return "SessionStopReq";
        default:
            return "OtherReq";
    }
}

void log_hlc_request(jpv2g_message_type_t type, const jpv2g_secc_request_t* req) {
    if (!req) return;
    const char* protocol = hlc_protocol_name(req->protocol);
    if (!req->body) {
        Serial.printf("[HLC][REQ] {\"type\":\"%s\",\"protocol\":\"%s\",\"body\":0}\n",
                      hlc_message_type_name(type),
                      protocol);
        return;
    }

    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        switch (type) {
            case JPV2G_SESSION_SETUP_REQ: {
                const auto* rq = static_cast<const iso2_SessionSetupReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"SessionSetupReq\",\"protocol\":\"%s\",\"evccid_len\":%u}\n",
                              protocol,
                              static_cast<unsigned>(rq->EVCCID.bytesLen));
                return;
            }
            case JPV2G_AUTHORIZATION_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"AuthorizationReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            case JPV2G_CHARGE_PARAMETER_DISCOVERY_REQ: {
                const auto* rq = static_cast<const iso2_ChargeParameterDiscoveryReqType*>(req->body);
                const int soc = rq->DC_EVChargeParameter_isUsed
                                    ? static_cast<int>(rq->DC_EVChargeParameter.DC_EVStatus.EVRESSSOC)
                                    : -1;
                Serial.printf("[HLC][REQ] {\"type\":\"ChargeParameterDiscoveryReq\",\"protocol\":\"%s\",\"soc\":%d}\n",
                              protocol,
                              soc);
                return;
            }
            case JPV2G_CABLE_CHECK_REQ: {
                const auto* rq = static_cast<const iso2_CableCheckReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"CableCheckReq\",\"protocol\":\"%s\",\"soc\":%d}\n",
                              protocol,
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_PRE_CHARGE_REQ: {
                const auto* rq = static_cast<const iso2_PreChargeReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"PreChargeReq\",\"protocol\":\"%s\",\"targetV\":%.1f,"
                              "\"targetI\":%.2f,\"soc\":%d}\n",
                              protocol,
                              iso_pv_to_float(&rq->EVTargetVoltage),
                              iso_pv_to_float(&rq->EVTargetCurrent),
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_POWER_DELIVERY_REQ: {
                const auto* rq = static_cast<const iso2_PowerDeliveryReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"PowerDeliveryReq\",\"protocol\":\"%s\",\"chargeProgress\":%u,"
                              "\"scheduleId\":%u}\n",
                              protocol,
                              static_cast<unsigned>(rq->ChargeProgress),
                              static_cast<unsigned>(rq->SAScheduleTupleID));
                return;
            }
            case JPV2G_CURRENT_DEMAND_REQ: {
                const auto* rq = static_cast<const iso2_CurrentDemandReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"CurrentDemandReq\",\"protocol\":\"%s\",\"targetV\":%.1f,"
                              "\"targetI\":%.2f,\"soc\":%d}\n",
                              protocol,
                              iso_pv_to_float(&rq->EVTargetVoltage),
                              iso_pv_to_float(&rq->EVTargetCurrent),
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_WELDING_DETECTION_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"WeldingDetectionReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            case JPV2G_SESSION_STOP_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"SessionStopReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            default:
                break;
        }
    } else if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        switch (type) {
            case JPV2G_SESSION_SETUP_REQ: {
                const auto* rq = static_cast<const din_SessionSetupReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"SessionSetupReq\",\"protocol\":\"%s\",\"evccid_len\":%u}\n",
                              protocol,
                              static_cast<unsigned>(rq->EVCCID.bytesLen));
                return;
            }
            case JPV2G_AUTHORIZATION_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"AuthorizationReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            case JPV2G_CHARGE_PARAMETER_DISCOVERY_REQ: {
                const auto* rq = static_cast<const din_ChargeParameterDiscoveryReqType*>(req->body);
                const int soc = rq->DC_EVChargeParameter_isUsed
                                    ? static_cast<int>(rq->DC_EVChargeParameter.DC_EVStatus.EVRESSSOC)
                                    : -1;
                Serial.printf("[HLC][REQ] {\"type\":\"ChargeParameterDiscoveryReq\",\"protocol\":\"%s\",\"soc\":%d}\n",
                              protocol,
                              soc);
                return;
            }
            case JPV2G_CABLE_CHECK_REQ: {
                const auto* rq = static_cast<const din_CableCheckReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"CableCheckReq\",\"protocol\":\"%s\",\"soc\":%d}\n",
                              protocol,
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_PRE_CHARGE_REQ: {
                const auto* rq = static_cast<const din_PreChargeReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"PreChargeReq\",\"protocol\":\"%s\",\"targetV\":%.1f,"
                              "\"targetI\":%.2f,\"soc\":%d}\n",
                              protocol,
                              din_pv_to_float(&rq->EVTargetVoltage),
                              din_pv_to_float(&rq->EVTargetCurrent),
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_POWER_DELIVERY_REQ: {
                const auto* rq = static_cast<const din_PowerDeliveryReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"PowerDeliveryReq\",\"protocol\":\"%s\",\"readyToCharge\":%u}\n",
                              protocol,
                              static_cast<unsigned>(rq->ReadyToChargeState));
                return;
            }
            case JPV2G_CURRENT_DEMAND_REQ: {
                const auto* rq = static_cast<const din_CurrentDemandReqType*>(req->body);
                Serial.printf("[HLC][REQ] {\"type\":\"CurrentDemandReq\",\"protocol\":\"%s\",\"targetV\":%.1f,"
                              "\"targetI\":%.2f,\"soc\":%d}\n",
                              protocol,
                              din_pv_to_float(&rq->EVTargetVoltage),
                              din_pv_to_float(&rq->EVTargetCurrent),
                              static_cast<int>(rq->DC_EVStatus.EVRESSSOC));
                return;
            }
            case JPV2G_WELDING_DETECTION_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"WeldingDetectionReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            case JPV2G_SESSION_STOP_REQ:
                Serial.printf("[HLC][REQ] {\"type\":\"SessionStopReq\",\"protocol\":\"%s\"}\n", protocol);
                return;
            default:
                break;
        }
    }

    Serial.printf("[HLC][REQ] {\"type\":\"%s\",\"protocol\":\"%s\"}\n",
                  hlc_message_type_name(type),
                  protocol);
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
    const auto emergency = static_cast<PCA95x5::Port::Port>(EMERGENCY_EXP_PIN);

    const bool off_level = RELAY_ACTIVE_HIGH ? false : true;
    (void)g_relay_expander.direction(p1, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(p2, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(p3, PCA95x5::Direction::OUT);
    (void)g_relay_expander.direction(sw4, PCA95x5::Direction::IN);
    (void)g_relay_expander.direction(emergency, PCA95x5::Direction::IN);

    (void)g_relay_expander.write(p1, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    (void)g_relay_expander.write(p2, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    (void)g_relay_expander.write(p3, off_level ? PCA95x5::Level::H : PCA95x5::Level::L);
    g_relay1_closed = false;
    g_relay2_closed = false;
    g_relay3_closed = false;
    g_relay_ready = true;
    unlock_relay();
    Serial.printf("[RELAY] ready addr=0x%02X pin=P00/P01/P02, SW4=P06, EM=P10\n", static_cast<unsigned>(RELAY_I2C_ADDR));
    return true;
}

bool relay_set_impl(int pin, bool* state_cache, bool closed, const char* reason, const char* label) {
    if (!g_relay_ready && !relay1_init()) return false;
    if (!state_cache) return false;
    if (!lock_relay(30)) return false;
    const auto port = static_cast<PCA95x5::Port::Port>(pin);
    const bool level = RELAY_ACTIVE_HIGH ? closed : !closed;
    const bool ok = g_relay_expander.direction(port, PCA95x5::Direction::OUT) &&
                    g_relay_expander.write(port, level ? PCA95x5::Level::H : PCA95x5::Level::L);
    const auto readback = ok ? g_relay_expander.read(port) : PCA95x5::Level::L;
    const bool readback_ok = ok && (readback == (level ? PCA95x5::Level::H : PCA95x5::Level::L));
    if (readback_ok) {
        *state_cache = closed;
        Serial.printf("[RELAY] %s -> %s (%s)\n", label ? label : "Relay", closed ? "CLOSED" : "OPEN",
                      reason ? reason : "-");
    } else if (ok) {
        Serial.printf("[RELAY] %s write verify failed (%s)\n",
                      label ? label : "Relay",
                      reason ? reason : "-");
    }
    unlock_relay();
    return readback_ok;
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
    const auto lv = g_relay_expander.read(sw4);
    unlock_relay();
    return lv == PCA95x5::Level::L;
}

bool read_emergency_pressed() {
    if (!g_relay_ready && !relay1_init()) {
        Serial.println("[EMERGENCY] relay expander unavailable, fail-safe pressed");
        return true;
    }
    if (!lock_relay(20)) {
        Serial.println("[EMERGENCY] relay mutex unavailable, fail-safe pressed");
        return true;
    }
    const auto emergency = static_cast<PCA95x5::Port::Port>(EMERGENCY_EXP_PIN);
    const auto lv = g_relay_expander.read(emergency);
    unlock_relay();
    return lv == PCA95x5::Level::L;
}

void serial_tx_emergency_event(bool active) {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] EVT EMERGENCY plc_id=%u connector_id=%u active=%d\n",
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  active ? 1 : 0);
}

void service_emergency_stop(uint32_t now_ms) {
    static bool sampled_state = false;
    static bool debounced_state = false;
    static uint32_t last_change_ms = 0u;
    static uint32_t last_poll_ms = 0u;

    if (last_poll_ms != 0u && (now_ms - last_poll_ms) < EMERGENCY_POLL_MS) {
        return;
    }
    last_poll_ms = now_ms;
    const bool raw_state = read_emergency_pressed();
    if (raw_state != sampled_state) {
        sampled_state = raw_state;
        last_change_ms = now_ms;
    }
    if ((now_ms - last_change_ms) < EMERGENCY_DEBOUNCE_MS || debounced_state == sampled_state) {
        return;
    }

    debounced_state = sampled_state;
    g_emergency_stop_active = debounced_state;
    if (g_emergency_stop_active) {
        g_ctrl.auth_state = ControllerAuthState::Denied;
        g_ctrl.auth_expiry_ms = 0u;
        controller_clear_slac_contract();
        controller_force_safe_stop("EmergencyStop");
    }
    Serial.printf("[EMERGENCY] state=%s\n", g_emergency_stop_active ? "PRESSED" : "RELEASED");
    serial_tx_emergency_event(g_emergency_stop_active);
}

void normalize_runtime_config(RuntimeConfig* cfg) {
    if (!cfg) return;
    if (cfg->magic != CFG_MAGIC) {
        *cfg = RuntimeConfig{};
        return;
    }
    cfg->version = CFG_VERSION;
    cfg->mode = static_cast<uint8_t>(normalize_mode(cfg->mode));
    cfg->plc_id = normalize_plc_id(static_cast<uint8_t>(cfg->plc_id & 0x0Fu));
    cfg->connector_id = normalize_connector_id(cfg->connector_id, cfg->plc_id);
    cfg->controller_id = normalize_controller_id(static_cast<uint8_t>(cfg->controller_id & 0x0Fu));
    cfg->local_module_address = normalize_local_module_address(cfg->local_module_address, cfg->plc_id);
    cfg->can_node_id = normalize_can_node_id(static_cast<uint8_t>(cfg->can_node_id & 0x0Fu), cfg->plc_id);
    cfg->controller_heartbeat_timeout_ms =
        std::max<uint32_t>(500u, std::min<uint32_t>(cfg->controller_heartbeat_timeout_ms, 10000u));
    cfg->controller_auth_ttl_ms =
        std::max<uint32_t>(500u, std::min<uint32_t>(cfg->controller_auth_ttl_ms, 30000u));
    cfg->controller_slac_arm_timeout_ms =
        std::max<uint32_t>(1000u, std::min<uint32_t>(cfg->controller_slac_arm_timeout_ms, 60000u));
    cfg->led_count = std::max<uint8_t>(1u, std::min<uint8_t>(cfg->led_count, MAX_LED_COUNT));
}

bool migrate_runtime_config_v4(const RuntimeConfigV4& legacy, RuntimeConfig* out) {
    if (!out) return false;
    if (legacy.magic != CFG_MAGIC || legacy.version != 4u) {
        return false;
    }
    RuntimeConfig migrated{};
    migrated.mode = legacy.mode;
    migrated.slac_requires_controller_start = legacy.slac_requires_controller_start;
    migrated.plc_id = legacy.plc_id;
    migrated.connector_id = legacy.connector_id;
    migrated.controller_id = legacy.controller_id;
    migrated.local_module_address = legacy.local_module_address;
    migrated.can_node_id = legacy.can_node_id;
    migrated.controller_heartbeat_timeout_ms = legacy.controller_heartbeat_timeout_ms;
    migrated.controller_auth_ttl_ms = legacy.controller_auth_ttl_ms;
    migrated.controller_slac_arm_timeout_ms = legacy.controller_slac_arm_timeout_ms;
    normalize_runtime_config(&migrated);
    *out = migrated;
    return true;
}

bool migrate_runtime_config_v5(const RuntimeConfigV5& legacy, RuntimeConfig* out) {
    if (!out) return false;
    if (legacy.magic != CFG_MAGIC || legacy.version != 5u) {
        return false;
    }
    RuntimeConfig migrated{};
    migrated.mode = legacy.mode;
    migrated.slac_requires_controller_start = legacy.slac_requires_controller_start;
    migrated.plc_id = legacy.plc_id;
    migrated.connector_id = legacy.connector_id;
    migrated.controller_id = legacy.controller_id;
    migrated.local_module_address = legacy.local_module_address;
    migrated.can_node_id = legacy.can_node_id;
    migrated.controller_heartbeat_timeout_ms = legacy.controller_heartbeat_timeout_ms;
    migrated.controller_auth_ttl_ms = legacy.controller_auth_ttl_ms;
    migrated.controller_slac_arm_timeout_ms = legacy.controller_slac_arm_timeout_ms;
    migrated.led_count = DEFAULT_LED_COUNT;
    normalize_runtime_config(&migrated);
    *out = migrated;
    return true;
}

bool migrate_runtime_config_v3(const RuntimeConfigV3& legacy, RuntimeConfig* out) {
    if (!out) return false;
    if (legacy.magic != CFG_MAGIC || legacy.version != 3u) {
        return false;
    }
    RuntimeConfig migrated{};
    migrated.mode = static_cast<uint8_t>(legacy.standalone_mode
                                             ? OperatingMode::Standalone
                                             : OperatingMode::ExternalController);
    migrated.slac_requires_controller_start = legacy.slac_requires_controller_start;
    migrated.plc_id = legacy.plc_id;
    migrated.connector_id = legacy.connector_id;
    migrated.controller_id = legacy.controller_id;
    migrated.local_module_address = legacy.local_module_address;
    migrated.can_node_id = legacy.can_node_id;
    migrated.controller_heartbeat_timeout_ms = legacy.controller_heartbeat_timeout_ms;
    migrated.controller_auth_ttl_ms = legacy.controller_auth_ttl_ms;
    migrated.controller_slac_arm_timeout_ms = legacy.controller_slac_arm_timeout_ms;
    normalize_runtime_config(&migrated);
    *out = migrated;
    return true;
}

void print_runtime_config() {
    Serial.printf("[CFG] mode=%u(%s) slac_ctrl=%d plc_id=%u can_node_id=%u connector_id=%u controller_id=%u module_addr=0x%02X local_group=%u owner_id=0x%04X hb_to=%lu auth_ttl=%lu slac_arm_to=%lu led_count=%u\n",
                  static_cast<unsigned>(g_runtime_cfg.mode),
                  runtime_mode_name(),
                  g_runtime_cfg.slac_requires_controller_start ? 1 : 0,
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.can_node_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  static_cast<unsigned>(g_runtime_cfg.controller_id),
                  static_cast<unsigned>(g_runtime_cfg.local_module_address),
                  static_cast<unsigned>(g_local_module_group),
                  static_cast<unsigned>(runtime_module_manager_owner_id()),
                  static_cast<unsigned long>(g_runtime_cfg.controller_heartbeat_timeout_ms),
                  static_cast<unsigned long>(g_runtime_cfg.controller_auth_ttl_ms),
                  static_cast<unsigned long>(g_runtime_cfg.controller_slac_arm_timeout_ms),
                  static_cast<unsigned>(g_runtime_cfg.led_count));
}

bool load_runtime_config_from_nvs() {
    g_runtime_cfg = RuntimeConfig{};
    if (!g_cfg_prefs.begin("cbplc_cfg", false)) {
        Serial.println("[CFG] NVS open failed, using defaults");
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        return false;
    }
    const size_t stored = g_cfg_prefs.getBytesLength("runtime_cfg");
    if (stored == 0u) {
        Serial.println("[CFG] no persisted config, using volatile defaults");
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        g_cfg_prefs.end();
        return false;
    }
    RuntimeConfig nvs_cfg{};
    bool loaded = false;
    if (stored == sizeof(nvs_cfg)) {
        const size_t got = g_cfg_prefs.getBytes("runtime_cfg", &nvs_cfg, sizeof(nvs_cfg));
        loaded = (got == sizeof(nvs_cfg));
    } else if (stored == sizeof(RuntimeConfigV5)) {
        RuntimeConfigV5 legacy{};
        const size_t got = g_cfg_prefs.getBytes("runtime_cfg", &legacy, sizeof(legacy));
        loaded = (got == sizeof(legacy)) && migrate_runtime_config_v5(legacy, &nvs_cfg);
        if (loaded) {
            Serial.println("[CFG] migrated legacy v5 config to v6");
        }
    } else if (stored == sizeof(RuntimeConfigV4)) {
        RuntimeConfigV4 legacy{};
        const size_t got = g_cfg_prefs.getBytes("runtime_cfg", &legacy, sizeof(legacy));
        loaded = (got == sizeof(legacy)) && migrate_runtime_config_v4(legacy, &nvs_cfg);
        if (loaded) {
            Serial.println("[CFG] migrated legacy v4 config to v6");
        }
    } else if (stored == sizeof(RuntimeConfigV3)) {
        RuntimeConfigV3 legacy{};
        const size_t got = g_cfg_prefs.getBytes("runtime_cfg", &legacy, sizeof(legacy));
        loaded = (got == sizeof(legacy)) && migrate_runtime_config_v3(legacy, &nvs_cfg);
        if (loaded) {
            Serial.println("[CFG] migrated legacy v3 config to v6");
        }
    }
    g_cfg_prefs.end();
    if (!loaded) {
        Serial.println("[CFG] persisted config invalid, using defaults");
        normalize_runtime_config(&g_runtime_cfg);
        refresh_runtime_identity_cache();
        return false;
    }
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
    log_runtime_stats("wifi_ap_start");

    IPAddress ip = WiFi.softAPIP();
    Serial.printf("[CFG] setup portal AP=%s ip=%s (120s timeout)\n", ssid.c_str(), ip.toString().c_str());

    WebServer server(80);
    bool saved = false;

    server.on("/", HTTP_GET, [&]() {
        String html;
        html.reserve(1800);
        html += "<html><body><h2>CB PLC Setup</h2>";
        html += "<form method='POST' action='/save'>";
        html += "Mode (0=standalone,1=external_controller) <input name='mode' value='";
        html += String(g_runtime_cfg.mode);
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
        html += "SLAC arm timeout ms <input name='slac_arm_ms' value='" + String(g_runtime_cfg.controller_slac_arm_timeout_ms) + "'/><br/>";
        html += "LED count (1-32) <input name='led_count' value='" + String(g_runtime_cfg.led_count) + "'/><br/>";
        html += "<button type='submit'>Save & Reboot</button></form></body></html>";
        server.send(200, "text/html", html);
    });

    server.on("/save", HTTP_POST, [&]() {
        RuntimeConfig next = g_runtime_cfg;
        if (server.hasArg("mode")) next.mode = static_cast<uint8_t>(std::max<long>(0, server.arg("mode").toInt()));
        else if (server.hasArg("standalone")) {
            next.mode = static_cast<uint8_t>(parse_bool_from_arg(server.arg("standalone"))
                                                 ? OperatingMode::Standalone
                                                 : OperatingMode::ExternalController);
        }
        if (server.hasArg("slac_ctrl")) next.slac_requires_controller_start = parse_bool_from_arg(server.arg("slac_ctrl"));
        if (server.hasArg("plc_id")) next.plc_id = static_cast<uint8_t>(std::max<long>(0, server.arg("plc_id").toInt()));
        if (server.hasArg("connector_id")) next.connector_id = static_cast<uint8_t>(std::max<long>(0, server.arg("connector_id").toInt()));
        if (server.hasArg("controller_id")) next.controller_id = static_cast<uint8_t>(std::max<long>(0, server.arg("controller_id").toInt()));
        if (server.hasArg("module_addr")) next.local_module_address = static_cast<uint8_t>(std::max<long>(0, server.arg("module_addr").toInt()));
        if (server.hasArg("hb_ms")) next.controller_heartbeat_timeout_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("hb_ms").toInt()));
        if (server.hasArg("auth_ms")) next.controller_auth_ttl_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("auth_ms").toInt()));
        if (server.hasArg("slac_arm_ms")) next.controller_slac_arm_timeout_ms = static_cast<uint32_t>(std::max<long>(0, server.arg("slac_arm_ms").toInt()));
        if (server.hasArg("led_count")) next.led_count = static_cast<uint8_t>(std::max<long>(0, server.arg("led_count").toInt()));
        normalize_runtime_config(&next);
        const RuntimeConfig previous = g_runtime_cfg;
        g_runtime_cfg = next;
        const bool ok = save_runtime_config_to_nvs();
        if (!ok) {
            g_runtime_cfg = previous;
            refresh_runtime_identity_cache();
        }
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
    // Standalone HLC needs fast post-PreCharge telemetry so the first
    // CurrentDemand response reflects the real module output, not stale
    // precharge samples from the prior probe cycle.
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
    // In standalone mode the PLC owns the full charging path. After precharge,
    // move directly to the EV-requested target and let the real module
    // capability clamps enforce the hardware limits.
    cfg.safety.enable_ramp_sequencing = false;
    cfg.safety.enable_telemetry_stale_trip = false;
    cfg.safety.enable_can_rate_limit = false;
    cfg.safety.enable_multi_controller_guard = false;
    cfg.controller_id = runtime_module_manager_owner_id();

    g_module_mgr.~ModuleManager();
    new (&g_module_mgr) cbmodules::ModuleManager(cfg);
    g_module_mgr.set_transport(&g_can);

    g_known_module_ids.clear();
    g_known_module_can_groups.clear();
    const auto local_spec = build_module_spec_for_addr(g_runtime_cfg.local_module_address);
    g_module_mgr.set_inventory({local_spec});
    g_known_module_ids[g_runtime_cfg.local_module_address] = local_spec.id;
    g_known_module_can_groups[g_runtime_cfg.local_module_address] = local_spec.group;

    Serial.printf("[MOD] startup scan begin (connector=%u plc=%u module=%s addr=0x%02X logical_group=%u module_can_group=%u owner_id=0x%04X)\n",
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  runtime_local_module_id().c_str(),
                  static_cast<unsigned>(g_runtime_cfg.local_module_address),
                  static_cast<unsigned>(g_local_module_group),
                  static_cast<unsigned>(local_spec.group),
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
    group.min_modules = 0;
    group.max_modules = MAX_STAGED_ALLOC_MODULES;
    group.efficient_module_usage = mode_is_local_autonomous();
    group.hard_safety_clamp = true;
    group.max_group_current_a = MODULE_MAX_CURRENT_A * static_cast<float>(MAX_STAGED_ALLOC_MODULES);
    group.max_group_power_kw = MODULE_MAX_POWER_KW * static_cast<float>(MAX_STAGED_ALLOC_MODULES);

    const bool upsert_ok = g_module_mgr.upsert_group(group);
    const bool alloc_ok = true;
    g_module_mgr.set_module_hard_limits(runtime_local_module_id(), MODULE_MAX_CURRENT_A, MODULE_MAX_POWER_KW);
    g_module_mgr.set_group_target(runtime_local_group_id(), cbmodules::GroupTarget{false, 0.0f, 0.0f});
    g_module_mgr.tick(millis());
    g_module_ready = upsert_ok && alloc_ok;
    unlock_modules();

    Serial.printf("[MOD] setup upsert=%d allocate=%d ready=%d\n",
                  upsert_ok ? 1 : 0,
                  alloc_ok ? 1 : 0,
                  g_module_ready ? 1 : 0);
    if (g_module_ready) {
        log_runtime_stats("module_mgr_ready");
    }
    return g_module_ready;
}

bool modules_apply_target(bool enable, float target_v, float target_i, const char* reason) {
    if (!g_module_ready) return false;
    if (!lock_modules(40)) return false;

    g_module_mgr.poll_rx(24);
    const uint32_t pre_tick_ms = millis();
    g_module_mgr.tick(pre_tick_ms);

    cbmodules::GroupStatus live_st = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(&live_st, pre_tick_ms);

    cbmodules::GroupTarget tgt{};
    tgt.enable = enable;
    tgt.voltage_v = std::max(0.0f, std::min(MODULE_MAX_VOLTAGE_V, target_v));
    const float requested_current_a = std::max(0.0f, std::min(MODULE_MAX_CURRENT_A, target_i));
    const float limited_current_a = clamp_current_for_assignment_limit(tgt.voltage_v, target_i);
    // Do not reuse the previous group's live available-current snapshot to clamp
    // the next target. During hot allocation that value still reflects the old
    // module set and artificially drags the owner down exactly when new modules
    // have just been leased in. The module manager already performs the real
    // capability clamp on the active allowlist.
    const float safe_current_a = limited_current_a;
    tgt.current_a = std::max(0.0f, std::min(MODULE_MAX_CURRENT_A, safe_current_a));
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
        g_last_requested_target_v = tgt.voltage_v;
        g_last_requested_target_i = enable ? requested_current_a : 0.0f;
        g_module_output_enabled = enable;
    } else {
        Serial.printf("[MOD] apply failed (%s) en=%d v=%.1f i=%.1f\n",
                      reason ? reason : "-", enable ? 1 : 0, target_v, target_i);
    }
    return ok;
}

void request_modules_off(const char* reason);

bool ensure_power_delivery_started(HlcAppContext* ctx, float target_v, const char* reason) {
    if (!ctx) {
        return false;
    }
    const bool standalone_handoff_allowed =
        !mode_controller_has_final_decision() && ctx->precharge_seen && g_relay1_closed && g_module_output_enabled;
    if (!controller_allows_energy(millis()) || (!ctx->precharge_converged && !standalone_handoff_allowed) ||
        g_module_allowlist.empty()) {
        return false;
    }
    if (!modules_apply_target(true,
                              std::max(0.0f, std::min(MODULE_MAX_VOLTAGE_V, target_v)),
                              PRECHARGE_CURRENT_LIMIT_A,
                              reason ? reason : "PowerDeliveryStart")) {
        ctx->power_delivery_enabled = false;
        return false;
    }
    if (!g_relay1_closed) {
        if (!relay1_set(true, reason ? reason : "PowerDeliveryStart")) {
            request_modules_off("PowerDeliveryStartRelayFail");
            ctx->power_delivery_enabled = false;
            return false;
        }
    }
    ctx->power_delivery_enabled = g_relay1_closed;
    return ctx->power_delivery_enabled;
}

void request_modules_off(const char* reason) {
    if (g_module_allowlist.empty()) {
        g_module_output_enabled = false;
        g_last_module_target_i = 0.0f;
        g_last_requested_target_i = 0.0f;
        return;
    }
    const bool ok = modules_apply_target(false, 0.0f, 0.0f, reason);
    Serial.printf("[MOD] off request (%s) result=%d relay2=%d session=%d hlc=%d gc=%d allow=%u\n",
                  reason ? reason : "-",
                  ok ? 1 : 0,
                  g_relay2_closed ? 1 : 0,
                  g_session_started ? 1 : 0,
                  g_hlc_active ? 1 : 0,
                  g_relay1_closed ? 1 : 0,
                  static_cast<unsigned>(g_module_allowlist.size()));
    if (ok) {
        g_module_output_enabled = false;
        g_last_module_target_i = 0.0f;
        g_last_requested_target_i = 0.0f;
    } else {
        Serial.printf("[MOD] off request failed (%s)\n", reason ? reason : "-");
    }
}

void release_controller_allowlist_if_idle(const char* reason) {
    if (!mode_requires_controller_contract() || !g_module_ready) {
        return;
    }
    if (g_session_started || g_hlc_active || g_relay1_closed) {
        return;
    }
    if (g_module_allowlist.empty()) {
        return;
    }
    (void)apply_new_allowlist({}, 0.0f, reason ? reason : "ControllerIdleRelease");
}

void service_module_manager_once(uint32_t now_ms) {
    if (!g_module_ready) return;
    const bool module_control_active =
        mode_is_local_autonomous() || !g_module_allowlist.empty() || g_module_output_enabled;
    if (!module_control_active) {
        return;
    }
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
    if (try_clear_local_module_isolation_locked(now_ms, "IdleRecover")) {
        g_module_mgr.poll_rx(8);
        g_module_mgr.tick(millis());
    }
    const uint32_t sample_ms = millis();
    auto st = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(&st, sample_ms);
    update_real_meter_with_status(st, sample_ms);
    log_module_runtime_status(st, sample_ms);
    unlock_modules();
}

bool module_id_known(const std::string& module_id) {
    if (module_id.empty()) return false;
    for (const auto& [addr, known_id] : g_known_module_ids) {
        (void)addr;
        if (known_id == module_id) {
            return true;
        }
    }
    return false;
}

std::string module_id_from_addr(uint8_t addr) {
    if (addr == 0u) {
        return std::string();
    }
    if (!ensure_known_module_registered(addr)) {
        return std::string();
    }
    const auto it = g_known_module_ids.find(addr);
    return it == g_known_module_ids.end() ? std::string() : it->second;
}

void controller_force_safe_stop(const char* reason) {
    controller_clear_slac_contract();
    controller_clear_managed_power_cache();
    controller_clear_managed_feedback_cache();
    g_relay1_deadline_ms = 0;
    g_relay2_deadline_ms = 0;
    g_relay3_deadline_ms = 0;
    request_modules_off(reason ? reason : "ControllerSafeStop");
    if (!mode_controller_has_final_decision()) {
        (void)relay1_set(false, reason ? reason : "ControllerSafeStop");
    }
    stop_hlc_stack();
    stop_session(millis());
    release_controller_allowlist_if_idle(reason ? reason : "ControllerSafeStop");
}

void controller_reset_runtime_state() {
    controller_clear_slac_contract();
    g_ctrl.heartbeat_timeout_ms = 0u;
    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.last_auth_update_ms = 0u;
    g_ctrl.auth_expiry_ms = 0;
    g_ctrl.last_energy_allowed_ms = 0;
    g_ctrl.heartbeat_seen = false;
    g_ctrl.heartbeat_session_marker = 0u;
    g_ctrl.last_heartbeat_ms = 0;
    g_ctrl.hb_lost_since_ms = 0;
    controller_reset_command_seq_state(true);
    g_ctrl.stop_active = false;
    g_ctrl.stop_hard = false;
    g_ctrl.stop_force_issued = false;
    g_ctrl.stop_reason = 0;
    g_ctrl.stop_deadline_ms = 0;
    controller_clear_managed_power_cache();
    controller_clear_managed_feedback_cache();
    g_assignment_power_limit_kw = 0.0f;
    g_active_module_power_limits_kw.clear();
    g_last_requested_target_i = 0.0f;
    g_last_seen_ev_mac_valid = false;
    g_session_started_ms = 0;
    g_serial_tx = SerialStatusTxSchedule{};
    clear_last_bms_request();
    reset_session_measurements();
}

void restore_default_module_limits(const std::map<std::string, float>& previous_limits,
                                   const std::map<std::string, float>& next_limits) {
    for (const auto& [id, previous_limit_kw] : previous_limits) {
        (void)previous_limit_kw;
        if (next_limits.find(id) != next_limits.end()) {
            continue;
        }
        (void)g_module_mgr.set_module_hard_limits(id, MODULE_MAX_CURRENT_A, MODULE_MAX_POWER_KW);
        if (id != runtime_local_module_id()) {
            (void)g_module_mgr.reset_module_runtime_state(id);
        }
    }
}

void controller_apply_mode_transition(OperatingMode next_mode) {
    const OperatingMode prev_mode = runtime_mode();
    const bool mode_changed = (prev_mode != next_mode);
    g_runtime_cfg.mode = static_cast<uint8_t>(next_mode);
    if (!mode_changed) {
        return;
    }

    const char* reason = (next_mode == OperatingMode::Standalone)
                             ? "ModeStandalone"
                             : "ModeExternalController";
    const bool next_uses_plc_module_manager = (next_mode != OperatingMode::ExternalController);
    const bool prev_used_plc_module_manager = (prev_mode != OperatingMode::ExternalController);
    controller_force_safe_stop(reason);
    if (!next_uses_plc_module_manager) {
        g_module_allowlist.clear();
        g_assignment_power_limit_kw = 0.0f;
        g_active_module_power_limits_kw.clear();
        g_module_output_enabled = false;
        g_module_ready = false;
        Serial.printf("[MOD] mode=%s runtime transition: PLC module CAN/control stack disabled\n", runtime_mode_name());
    } else {
        if (!prev_used_plc_module_manager) {
            (void)init_module_manager();
        }
        if (next_mode != OperatingMode::Standalone && !g_module_allowlist.empty()) {
            (void)apply_new_allowlist({}, 0.0f, reason);
        } else if (next_mode == OperatingMode::Standalone && g_module_ready) {
            (void)apply_new_allowlist({runtime_local_module_id()}, MODULE_MAX_POWER_KW, reason);
        }
    }
    controller_reset_runtime_state();
}

bool controller_stop_is_complete() {
    if (!mode_uses_plc_module_manager()) {
        return !g_module_output_enabled && !g_session_started && !g_hlc_active;
    }
    if (g_relay1_closed || g_module_output_enabled || g_session_started || g_hlc_active ||
        !g_module_allowlist.empty()) {
        return false;
    }
    return local_group_released();
}

void controller_begin_stop(uint32_t now_ms, bool hard, uint32_t timeout_ms, uint8_t reason_code) {
    g_ctrl.stop_active = true;
    g_ctrl.stop_hard = hard;
    g_ctrl.stop_force_issued = false;
    g_ctrl.stop_reason = reason_code;
    g_ctrl.stop_deadline_ms = now_ms + timeout_ms;

    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.auth_expiry_ms = now_ms + 200u;
    controller_clear_slac_contract();

    clear_last_bms_request();
    controller_clear_managed_power_cache();
    controller_clear_managed_feedback_cache();
    g_relay1_deadline_ms = 0;
    request_modules_off(hard ? "CtrlStopHard" : "CtrlStopSoft");
    // Release module ownership after we have driven the currently leased set to zero.
    if (mode_uses_plc_module_manager()) {
        (void)apply_new_allowlist({}, 0.0f, hard ? "CtrlStopHard" : "CtrlStopSoft");
    }
    if (!mode_controller_has_final_decision()) {
        (void)relay1_set(false, hard ? "CtrlStopHard" : "CtrlStopSoft");
    }
}

void controller_apply_stop_policy(uint32_t now_ms) {
    if (!g_ctrl.stop_active) return;

    g_ctrl.auth_state = ControllerAuthState::Denied;
    g_ctrl.auth_expiry_ms = now_ms + 200u;
    controller_clear_slac_contract();

    clear_last_bms_request();
    controller_clear_managed_power_cache();
    controller_clear_managed_feedback_cache();
    g_relay1_deadline_ms = 0;
    request_modules_off(g_ctrl.stop_hard ? "CtrlStopHard" : "CtrlStopSoft");
    if (mode_uses_plc_module_manager() && !g_module_allowlist.empty()) {
        (void)apply_new_allowlist({}, 0.0f, g_ctrl.stop_hard ? "CtrlStopHardRelease" : "CtrlStopSoftRelease");
    }
    if (!mode_controller_has_final_decision()) {
        (void)relay1_set(false, g_ctrl.stop_hard ? "CtrlStopHard" : "CtrlStopSoft");
    }

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

void controller_publish_local_identity_once() {
    g_serial_tx.local_identity_sent = false;
}

void controller_publish_ev_mac_if_changed(const uint8_t ev_mac[ETH_ALEN]) {
    if (!ev_mac) return;
    if (g_last_seen_ev_mac_valid && memcmp(g_last_seen_ev_mac, ev_mac, ETH_ALEN) == 0) {
        return;
    }
    memcpy(g_last_seen_ev_mac, ev_mac, ETH_ALEN);
    g_last_seen_ev_mac_valid = true;
    serial_tx_identity_hex("EVMAC", ev_mac, ETH_ALEN);
    g_serial_tx.next_identity_evt_ms = millis() + PLC_TX_IDENTITY_RETRY_MS;
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
    serial_print_identity("EVCCID", data, len);
    serial_tx_identity_hex("EVCCID", data, len);
}

void controller_publish_emaid(const uint8_t* data, size_t len) {
    if (!data || len == 0) return;
    serial_print_identity("EMAID", data, len);
    serial_tx_identity_hex("EMAID", data, len);
}

std::string uppercase_hex_token(const uint8_t* data, size_t len) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string token;
    token.reserve(len * 2u);
    for (size_t i = 0; i < len; ++i) {
        const uint8_t byte = data[i];
        token.push_back(kHex[(byte >> 4u) & 0x0Fu]);
        token.push_back(kHex[byte & 0x0Fu]);
    }
    return token;
}

class ScopedCanBusLock {
public:
    explicit ScopedCanBusLock(uint32_t timeout_ms) {
        if (!g_can_mutex) {
            taken_ = true;
            return;
        }
        taken_ = xSemaphoreTake(g_can_mutex, pdMS_TO_TICKS(timeout_ms)) == pdTRUE;
    }

    ~ScopedCanBusLock() {
        if (taken_ && g_can_mutex) {
            xSemaphoreGive(g_can_mutex);
        }
    }

    bool locked() const {
        return taken_;
    }

private:
    bool taken_{false};
};

const RfidPinMap* active_rfid_pin_map() {
    if (g_rfid_state.map_index >= RFID_PIN_MAPS.size()) {
        return nullptr;
    }
    return &RFID_PIN_MAPS[g_rfid_state.map_index];
}

void rfid_deselect_all_chip_selects() {
    for (const auto& map : RFID_PIN_MAPS) {
        pinMode(map.ss_pin, OUTPUT);
        digitalWrite(map.ss_pin, HIGH);
    }
}

void rfid_log_missing(const char* reason, uint32_t now_ms, const RfidPinMap& map) {
    if (static_cast<int32_t>(now_ms - g_rfid_state.next_missing_log_ms) < 0) {
        return;
    }
    g_rfid_state.next_missing_log_ms = now_ms + RFID_MISSING_LOG_MS;
    Serial.printf("[RFID] reader unavailable (%s) map=%s ss=%d rst=%d sck=%d miso=%d mosi=%d\n",
                  reason ? reason : "unknown",
                  map.name,
                  map.ss_pin,
                  map.rst_pin,
                  CAN_SPI_SCK,
                  CAN_SPI_MISO,
                  CAN_SPI_MOSI);
}

uint8_t rfid_next_event_id() {
    g_rfid_state.event_id = static_cast<uint8_t>(g_rfid_state.event_id + 1u);
    if (g_rfid_state.event_id == 0u) {
        g_rfid_state.event_id = 1u;
    }
    return g_rfid_state.event_id;
}

bool rfid_probe_locked(uint32_t now_ms, const char* reason, size_t map_index) {
    const uint8_t version = g_rfid.PCD_ReadRegister(MFRC522::VersionReg);
    const auto& map = RFID_PIN_MAPS[map_index];
    if (version == 0x00u || version == 0xFFu) {
        g_rfid_state.reader_present = false;
        rfid_log_missing(reason, now_ms, map);
        return false;
    }
    g_rfid.PCD_SetAntennaGain(MFRC522::RxGain_max);
    g_rfid.PCD_AntennaOn();
    g_rfid_state.reader_present = true;
    g_rfid_state.last_good_ms = now_ms;
    g_rfid_state.map_index = static_cast<uint8_t>(map_index);
    return true;
}

void rfid_publish_tap(const uint8_t* uid, size_t uid_len, uint32_t now_ms) {
    if (!uid || uid_len == 0u || uid_len > RFID_MAX_UID_BYTES) {
        return;
    }
    const uint8_t event_id = rfid_next_event_id();
    const std::string token = uppercase_hex_token(uid, uid_len);
    if (mode_uart_status_stream_active()) {
        Serial.printf("[SERCTRL] EVT RFID plc_id=%u connector_id=%u token=%s event=%u\n",
                      static_cast<unsigned>(g_runtime_cfg.plc_id),
                      static_cast<unsigned>(g_runtime_cfg.connector_id),
                      token.c_str(),
                      static_cast<unsigned>(event_id));
        return;
    }
    Serial.printf("[RFID] tap plc=%u connector=%u token=%s event=%u\n",
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  token.c_str(),
                  static_cast<unsigned>(event_id));
}

void rfid_mark_card_released() {
    g_rfid_state.card_latched = false;
    g_rfid_state.uid_len = 0u;
    g_rfid_state.uid.fill(0u);
}

void rfid_init_device(uint32_t now_ms, const char* reason) {
    if (!mode_requires_controller_contract()) {
        return;
    }
    ScopedCanBusLock guard(20u);
    if (!guard.locked()) {
        return;
    }

    pinMode(CAN_CS_PIN, OUTPUT);
    digitalWrite(CAN_CS_PIN, HIGH);
    g_rfid_state.initialized = true;
    g_rfid_state.reader_present = false;
    g_rfid_state.map_index = 0xFFu;
    g_rfid_state.next_reinit_ms = now_ms + RFID_REINIT_BACKOFF_MS;
    g_rfid_state.next_keepalive_ms = now_ms + RFID_KEEPALIVE_MS;

    for (size_t map_index = 0; map_index < RFID_PIN_MAPS.size(); ++map_index) {
        const auto& map = RFID_PIN_MAPS[map_index];
        rfid_deselect_all_chip_selects();
        pinMode(map.rst_pin, OUTPUT);
        digitalWrite(map.rst_pin, LOW);
        delay(2);
        digitalWrite(map.rst_pin, HIGH);

        SPI.begin(CAN_SPI_SCK, CAN_SPI_MISO, CAN_SPI_MOSI, map.ss_pin);
        SPI.setFrequency(RFID_SPI_HZ);
        SPI.setDataMode(SPI_MODE0);

        g_rfid.PCD_Init(map.ss_pin, map.rst_pin);
        delay(10);
        if (rfid_probe_locked(now_ms, reason, map_index)) {
            Serial.printf("[RFID] ready map=%s ss=%d rst=%d event_mode=%s\n",
                          map.name,
                          map.ss_pin,
                          map.rst_pin,
                          mode_uart_status_stream_active() ? "uart" : "can");
            return;
        }
    }
}

void rfid_keepalive(uint32_t now_ms) {
    if (!g_rfid_state.initialized || !g_rfid_state.reader_present ||
        static_cast<int32_t>(now_ms - g_rfid_state.next_keepalive_ms) < 0) {
        return;
    }

    g_rfid_state.next_keepalive_ms = now_ms + RFID_KEEPALIVE_MS;
    ScopedCanBusLock guard(5u);
    if (!guard.locked()) {
        return;
    }
    const uint8_t version = g_rfid.PCD_ReadRegister(MFRC522::VersionReg);
    if (version == 0x00u || version == 0xFFu) {
        g_rfid_state.reader_present = false;
        g_rfid_state.map_index = 0xFFu;
        g_rfid_state.next_reinit_ms = now_ms + RFID_REINIT_BACKOFF_MS;
        if (const auto* map = active_rfid_pin_map()) {
            rfid_log_missing("comm_lost", now_ms, *map);
        }
        rfid_mark_card_released();
        return;
    }
    g_rfid_state.last_good_ms = now_ms;
}

void rfid_poll(uint32_t now_ms) {
    if (!mode_requires_controller_contract()) {
        return;
    }
    if (static_cast<int32_t>(now_ms - g_rfid_state.next_poll_ms) < 0) {
        return;
    }
    g_rfid_state.next_poll_ms = now_ms + RFID_POLL_MS;

    if (!g_rfid_state.initialized ||
        (!g_rfid_state.reader_present && static_cast<int32_t>(now_ms - g_rfid_state.next_reinit_ms) >= 0)) {
        rfid_init_device(now_ms, g_rfid_state.initialized ? "retry" : "boot");
    }

    rfid_keepalive(now_ms);
    if (!g_rfid_state.reader_present) {
        return;
    }

    ScopedCanBusLock guard(5u);
    if (!guard.locked()) {
        return;
    }

    bool present = g_rfid.PICC_IsNewCardPresent();
    if (!present) {
        byte atqa[2] = {0u, 0u};
        byte atqa_len = sizeof(atqa);
        present = g_rfid.PICC_WakeupA(atqa, &atqa_len) == MFRC522::STATUS_OK;
    }

    if (!present) {
        if (g_rfid_state.card_latched &&
            static_cast<int32_t>(now_ms - (g_rfid_state.last_present_ms + RFID_CARD_RELEASE_MS)) >= 0) {
            rfid_mark_card_released();
        }
        return;
    }

    g_rfid_state.last_present_ms = now_ms;
    if (!g_rfid.PICC_ReadCardSerial()) {
        return;
    }

    size_t uid_len = std::min<size_t>(g_rfid.uid.size, RFID_MAX_UID_BYTES);
    std::array<uint8_t, RFID_MAX_UID_BYTES> uid{};
    memcpy(uid.data(), g_rfid.uid.uidByte, uid_len);
    g_rfid.PICC_HaltA();
    g_rfid.PCD_StopCrypto1();

    const bool same_uid = g_rfid_state.card_latched && uid_len == g_rfid_state.uid_len &&
                          memcmp(uid.data(), g_rfid_state.uid.data(), uid_len) == 0;
    if (same_uid) {
        return;
    }

    g_rfid_state.card_latched = true;
    g_rfid_state.uid_len = uid_len;
    g_rfid_state.uid = uid;
    rfid_publish_tap(uid.data(), uid_len, now_ms);
}

bool apply_new_allowlist(const std::vector<std::string>& next, float limit_kw, const char* reason) {
    for (const auto& id : next) {
        if (!module_id_known(id)) {
            return false;
        }
    }
    const float sanitized_limit_kw = std::max(0.0f, limit_kw);
    const bool limit_changed = std::fabs(sanitized_limit_kw - g_assignment_power_limit_kw) > 0.05f;
    const bool empty_release_pending = next.empty() && g_module_ready && !local_group_released();
    if (next == g_module_allowlist && !limit_changed && !empty_release_pending && next.empty()) {
        return true;
    }
    if (!g_module_ready) {
        g_module_allowlist = next;
        g_assignment_power_limit_kw = sanitized_limit_kw;
        if (next.empty()) {
            g_active_module_power_limits_kw.clear();
            g_module_output_enabled = false;
            g_last_module_target_i = 0.0f;
        }
        return true;
    }

    if (next == g_module_allowlist && !limit_changed && !empty_release_pending && !next.empty()) {
        if (lock_modules(40)) {
            g_module_mgr.poll_rx(12);
            const bool refreshed =
                g_module_mgr.refresh_group_allowlist_lease(runtime_local_group_id(), next, MODULE_ALLOWLIST_LEASE_MS);
            g_module_mgr.tick(millis());
            unlock_modules();
            if (refreshed) {
                return true;
            }
        }
    }

    if (!lock_modules(80)) {
        return false;
    }

    const float previous_limit_kw = g_assignment_power_limit_kw;
    const bool enable = g_module_output_enabled && (g_last_requested_target_i > 0.05f);
    const float target_v =
        enable ? std::max(0.0f, std::min(MODULE_MAX_VOLTAGE_V, g_last_requested_target_v)) : 0.0f;
    const float target_i = enable ? clamp_current_for_assignment_limit(target_v, g_last_requested_target_i) : 0.0f;
    cbmodules::GroupTarget tgt{};
    tgt.enable = enable;
    tgt.voltage_v = target_v;
    tgt.current_a = enable ? std::max(0.0f, std::min(MODULE_MAX_CURRENT_A, target_i)) : 0.0f;
    if (next.empty()) {
        tgt.enable = false;
        tgt.voltage_v = 0.0f;
        tgt.current_a = 0.0f;
    }

    g_module_mgr.poll_rx(24);
    const uint32_t now_ms = millis();
    g_module_mgr.tick(now_ms);
    g_assignment_power_limit_kw = sanitized_limit_kw;
    bool ok =
        g_module_mgr.apply_group_setpoint_allowlist(runtime_local_group_id(), tgt, next, MODULE_ALLOWLIST_LEASE_MS);
    g_module_mgr.poll_rx(8);
    const uint32_t sample_ms = millis();
    g_module_mgr.tick(sample_ms);
    if (!ok && try_clear_local_module_isolation_locked(sample_ms, "AllowlistRecover")) {
        ok = g_module_mgr.apply_group_setpoint_allowlist(runtime_local_group_id(),
                                                         tgt,
                                                         next,
                                                         MODULE_ALLOWLIST_LEASE_MS);
        g_module_mgr.poll_rx(8);
        g_module_mgr.tick(millis());
    }
    auto st = g_module_mgr.group_status(runtime_local_group_id());
    apply_fresh_measurements_from_states(&st, sample_ms);
    update_real_meter_with_status(st, sample_ms);
    unlock_modules();

    if (!ok) {
        g_assignment_power_limit_kw = previous_limit_kw;
        Serial.printf("[CTRL] allowlist apply failed (%s)\n", reason ? reason : "AllowlistChange");
        return false;
    }

    g_module_allowlist = next;
    if (next.empty()) {
        restore_default_module_limits(g_active_module_power_limits_kw, {});
        g_assignment_power_limit_kw = 0.0f;
        g_module_output_enabled = false;
        g_last_module_target_i = 0.0f;
        g_last_requested_target_i = 0.0f;
        g_active_module_power_limits_kw.clear();
        prune_known_remote_modules(next);
        Serial.printf("[CTRL] allowlist released (%s)\n", reason ? reason : "AllowlistClear");
    } else {
        prune_known_remote_modules(next);
    }
    return true;
}

void controller_handle_heartbeat(const CtrlRxFrame& f) {
    const uint8_t seq = f.data[1];
    const uint32_t session_marker =
        static_cast<uint32_t>(f.data[4]) |
        (static_cast<uint32_t>(f.data[5]) << 8u) |
        (static_cast<uint32_t>(f.data[6]) << 16u);
    const bool first_heartbeat = !g_ctrl.heartbeat_seen;
    const bool marker_changed =
        session_marker != 0u && g_ctrl.heartbeat_session_marker != 0u && session_marker != g_ctrl.heartbeat_session_marker;
    const bool resync_needed = !controller_heartbeat_alive(f.rx_ms) || marker_changed;
    if (resync_needed) {
        controller_reset_command_seq_state(true);
    }
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_heartbeat, &g_ctrl.seq_seen_heartbeat)) {
        send_ctrl_ack(0x10u, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    const uint8_t hb_100ms = f.data[3];
    if (hb_100ms > 0u) {
        g_ctrl.heartbeat_timeout_ms =
            std::max<uint32_t>(500u, std::min<uint32_t>(10000u, static_cast<uint32_t>(hb_100ms) * 100u));
    }
    g_ctrl.last_heartbeat_ms = f.rx_ms;
    g_ctrl.heartbeat_seen = true;
    if (session_marker != 0u) {
        g_ctrl.heartbeat_session_marker = session_marker;
    }
    if (first_heartbeat || marker_changed) {
        controller_publish_local_identity_once();
    }
    send_ctrl_ack(0x10u, seq, ACK_OK, 0, 0);
}

void controller_handle_auth(const CtrlRxFrame& f) {
    note_crash_breadcrumb(CrashBreadcrumbStage::CtrlAuthHandle, f.data[1]);
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
    if (g_ctrl.auth_state == ControllerAuthState::Denied) {
        controller_clear_managed_power_cache();
        controller_clear_managed_feedback_cache();
        if (!mode_controller_has_final_decision()) {
            request_modules_off("CtrlAuthDenied");
            (void)relay1_set(false, "CtrlAuthDenied");
        }
    }
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
        const bool active_session = g_session_started || g_session_matched || g_hlc_ready || g_hlc_active;
        controller_clear_slac_contract();
        if (active_session || !mode_controller_has_final_decision()) {
            controller_force_safe_stop("CtrlDisarm");
        }
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
        g_ctrl.slac_plc_owned = false;
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
        g_ctrl.slac_plc_owned = true;
        g_ctrl.slac_arm_expiry_ms = 0u;
        const uint32_t now_dbg = millis();
        Serial.printf("[CTRLRX] SLAC start seq=%u plc_owned=%d expiry_ms=%lu\n",
                      static_cast<unsigned>(seq),
                      g_ctrl.slac_plc_owned ? 1 : 0,
                      static_cast<unsigned long>(g_ctrl.slac_arm_expiry_ms));
        Serial.printf("[CTRLRX] SLAC start timing rx=%lu now=%lu delta=%ld\n",
                      static_cast<unsigned long>(f.rx_ms),
                      static_cast<unsigned long>(now_dbg),
                      static_cast<long>(now_dbg - f.rx_ms));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    if (cmd == CTRL_SLAC_ABORT) {
        const bool active_session = g_session_started || g_session_matched || g_hlc_ready || g_hlc_active;
        controller_clear_slac_contract();
        if (active_session || !mode_controller_has_final_decision()) {
            controller_force_safe_stop("CtrlAbort");
        }
        Serial.printf("[CTRLRX] SLAC abort seq=%u\n", static_cast<unsigned>(seq));
        send_ctrl_ack(0x12u, seq, ACK_OK, cmd, 0);
        return;
    }
    send_ctrl_ack(0x12u, seq, ACK_BAD_VALUE, cmd, 0);
}

void controller_handle_hlc_feedback(const CtrlRxFrame& f) {
    note_crash_breadcrumb(CrashBreadcrumbStage::CtrlFeedbackHandle, f.data[1]);
    const uint8_t seq = f.data[1];
    if (!ctrl_seq_accept(seq, &g_ctrl.last_seq_feedback, &g_ctrl.seq_seen_feedback)) {
        send_ctrl_ack(0x1Au, seq, ACK_BAD_SEQ, 0, 0);
        return;
    }
    if (!mode_controller_uses_external_feedback()) {
        send_ctrl_ack(0x1Au, seq, ACK_BAD_STATE, 0, 0);
        return;
    }

    bool valid = false;
    bool ready = false;
    bool current_limit = false;
    bool voltage_limit = false;
    bool power_limit = false;
    bool stop_charging = false;
    float present_v = 0.0f;
    float present_i = 0.0f;
    unpack_ctrl_hlc_feedback(ctrl_unpack_u32_le(&f.data[3]),
                             &valid,
                             &ready,
                             &current_limit,
                             &voltage_limit,
                             &power_limit,
                             &stop_charging,
                             &present_v,
                             &present_i);
    g_ctrl_feedback.seen = true;
    g_ctrl_feedback.last_update_ms = f.rx_ms;
    g_ctrl_feedback.valid = valid;
    g_ctrl_feedback.ready = ready;
    g_ctrl_feedback.stop_charging = stop_charging;
    g_ctrl_feedback.current_limit_achieved = current_limit;
    g_ctrl_feedback.voltage_limit_achieved = voltage_limit;
    g_ctrl_feedback.power_limit_achieved = power_limit;
    g_ctrl_feedback.present_voltage_v = std::max(0.0f, present_v);
    g_ctrl_feedback.present_current_a = std::max(0.0f, present_i);
    if (valid) {
        update_real_meter_sample(g_ctrl_feedback.present_voltage_v, g_ctrl_feedback.present_current_a, f.rx_ms);
    }
    send_ctrl_ack(0x1Au, seq, ACK_OK, valid ? 1u : 0u, ready ? 1u : 0u);
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
    if (g_emergency_stop_active && ((en_mask & st_mask) != 0u)) {
        send_ctrl_ack(0x17u, seq, ACK_BAD_STATE, en_mask, st_mask);
        return;
    }

    // Match the Basic relay contract and natural bit order:
    // bit0 -> Relay1, bit1 -> Relay2, bit2 -> Relay3.
    if (en_mask & 0x01u) {
        if (!mode_controller_owns_power_path()) {
            ok = false;
        } else {
            const bool relay1_close = (st_mask & 0x01u) != 0u;
            if (relay1_close && !mode_controller_has_final_decision() &&
                (!controller_allows_energy(f.rx_ms) || !cp_connected(g_cp_state))) {
                ok = false;
            }
            const bool relay1_ok = ok ? relay1_set(relay1_close, "CtrlRelay1") : false;
            ok = ok && relay1_ok;
            if (relay1_ok) {
                g_relay1_deadline_ms = (hold_ms > 0u) ? (f.rx_ms + hold_ms) : 0u;
            }
        }
    }
    if (en_mask & 0x02u) {
        const bool relay2_ok = relay2_set((st_mask & 0x02u) != 0u, "CtrlRelay2");
        ok = ok && relay2_ok;
        if (relay2_ok) {
            g_relay2_deadline_ms = (hold_ms > 0u) ? (f.rx_ms + hold_ms) : 0u;
        }
    }
    if (en_mask & 0x04u) {
        const bool relay3_ok = relay3_set((st_mask & 0x04u) != 0u, "CtrlRelay3");
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
        controller_clear_managed_power_cache();
        controller_clear_managed_feedback_cache();
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
    } else if (base == (CAN_ID_CTRL_AUX_RELAY & 0x1FFFFFF0u)) {
        controller_handle_aux_relay(f);
    } else if (base == (CAN_ID_CTRL_SESSION & 0x1FFFFFF0u)) {
        controller_handle_session(f);
    } else if (base == (CAN_ID_CTRL_HLC_FEEDBACK & 0x1FFFFFF0u)) {
        controller_handle_hlc_feedback(f);
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

    if (g_relay1_deadline_ms != 0 && static_cast<int32_t>(now_ms - g_relay1_deadline_ms) > 0) {
        g_relay1_deadline_ms = 0;
        (void)relay1_set(false, "Relay1Timeout");
    }
    if (g_relay2_deadline_ms != 0 && static_cast<int32_t>(now_ms - g_relay2_deadline_ms) > 0) {
        g_relay2_deadline_ms = 0;
        (void)relay2_set(false, "Relay2Timeout");
    }
    if (g_relay3_deadline_ms != 0 && static_cast<int32_t>(now_ms - g_relay3_deadline_ms) > 0) {
        g_relay3_deadline_ms = 0;
        (void)relay3_set(false, "Relay3Timeout");
    }
    if (mode_requires_controller_contract()) {
        if (mode_controller_has_final_decision()) {
            const bool hb_alive = controller_heartbeat_alive(now_ms);
            if (!hb_alive && g_ctrl.heartbeat_seen) {
                if (g_ctrl.hb_lost_since_ms == 0u) {
                    g_ctrl.hb_lost_since_ms = now_ms;
                }
            } else {
                g_ctrl.hb_lost_since_ms = 0u;
            }
            if (g_ctrl.hb_lost_since_ms != 0u) {
                const uint32_t lost_ms =
                    (now_ms >= g_ctrl.hb_lost_since_ms) ? (now_ms - g_ctrl.hb_lost_since_ms) : 0u;
                const bool energy_path_active = g_module_output_enabled || g_relay1_closed;
                if ((energy_path_active && lost_ms >= CONTROLLER_HB_SOFT_STOP_GRACE_MS) ||
                    ((g_session_started || g_hlc_active) && lost_ms >= CONTROLLER_HB_HARD_STOP_GRACE_MS)) {
                    controller_force_safe_stop("CtrlHeartbeatTimeout");
                    g_ctrl.auth_state = ControllerAuthState::Unknown;
                } else if ((!g_session_started && !g_hlc_active) &&
                           lost_ms >= CONTROLLER_HB_SOFT_STOP_GRACE_MS &&
                           (g_ctrl.slac_armed || g_ctrl.slac_start_latched || g_ctrl.slac_plc_owned ||
                            g_ctrl.slac_arm_expiry_ms != 0u)) {
                    Serial.printf("[CTRL] clearing stale SLAC contract after heartbeat loss %lu ms\n",
                                  static_cast<unsigned long>(lost_ms));
                    controller_clear_slac_contract();
                    stop_session(now_ms);
                }
            }
            if (!g_ctrl.slac_plc_owned && g_ctrl.slac_arm_expiry_ms != 0u &&
                static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) > 0) {
                if (g_ctrl.slac_armed || g_ctrl.slac_start_latched) {
                    Serial.println("[CTRL] clearing expired SLAC contract in external_controller");
                }
                controller_clear_slac_contract();
                if (!g_session_started && !g_hlc_active) {
                    stop_session(now_ms);
                }
            }
            if (g_ctrl.auth_expiry_ms != 0u && static_cast<int32_t>(now_ms - g_ctrl.auth_expiry_ms) > 0) {
                if (g_ctrl.auth_state == ControllerAuthState::Granted) {
                    Serial.println("[CTRL] controller auth expired in external_controller");
                    g_ctrl.auth_state = ControllerAuthState::Denied;
                    controller_clear_managed_feedback_cache();
                }
                g_ctrl.auth_expiry_ms = 0u;
            }
            if (g_ctrl_feedback.seen && !controller_feedback_fresh(now_ms)) {
                controller_clear_managed_feedback_cache();
            }
            return;
        }
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
                const bool active_session =
                    g_session_started || g_hlc_active || g_relay1_closed || g_module_output_enabled;
                const uint32_t expired_ms =
                    (now_ms >= g_ctrl.auth_expiry_ms) ? (now_ms - g_ctrl.auth_expiry_ms) : 0u;
                if (!active_session || expired_ms > CONTROLLER_ENERGY_HOLD_GRACE_MS) {
                    g_ctrl.auth_state = ControllerAuthState::Denied;
                    controller_clear_managed_power_cache();
                    controller_clear_managed_feedback_cache();
                    request_modules_off("CtrlAuthExpired");
                    (void)relay1_set(false, "CtrlAuthExpired");
                }
            }
        }
        if (g_ctrl.slac_arm_expiry_ms != 0 && static_cast<int32_t>(now_ms - g_ctrl.slac_arm_expiry_ms) > 0) {
            g_ctrl.slac_armed = false;
            g_ctrl.slac_start_latched = false;
            g_ctrl.slac_arm_expiry_ms = 0;
        }
        if (mode_controller_owns_power_path() && g_ctrl_feedback.seen && !controller_feedback_fresh(now_ms)) {
            controller_clear_managed_feedback_cache();
        }
    }
}

const char* bms_stage_name(uint8_t stage) {
    switch (stage) {
        case 1u:
            return "PreCharge";
        case 2u:
            return "CurrentDemand";
        case 3u:
            return "PowerDelivery";
        default:
            return "None";
    }
}

void serial_tx_identity_hex(const char* kind, const uint8_t* data, size_t len) {
    if (!mode_uart_status_stream_active() || !kind || !data || len == 0u) return;
    Serial.printf("[SERCTRL] IDENTITY kind=%s plc_id=%u connector_id=%u value=",
                  kind,
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id));
    for (size_t i = 0; i < len; ++i) {
        Serial.printf("%02X", data[i]);
    }
    Serial.println();
}

void serial_tx_local_identity() {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] LOCAL mode=%u(%s) transport=uart modules=external_can plc_id=%u connector_id=%u controller_id=%u module_addr=0x%02X local_group=%u module_id=%s can_stack=%d module_mgr=%d\n",
                  static_cast<unsigned>(g_runtime_cfg.mode),
                  runtime_mode_name(),
                  static_cast<unsigned>(g_runtime_cfg.plc_id),
                  static_cast<unsigned>(g_runtime_cfg.connector_id),
                  static_cast<unsigned>(g_runtime_cfg.controller_id),
                  static_cast<unsigned>(g_runtime_cfg.local_module_address),
                  static_cast<unsigned>(g_local_module_group),
                  runtime_local_module_id().c_str(),
                  mode_uses_plc_can_stack() ? 1 : 0,
                  mode_uses_plc_module_manager() ? 1 : 0);
    g_serial_tx.local_identity_sent = true;
}

void serial_tx_cp_status(uint32_t now_ms) {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] EVT CP cp=%s duty=%u connected=%d hb=%d auth=%u allow_slac=%d allow_energy=%d armed=%d start=%d\n",
                  cp_phase_label(g_cp_state, g_last_cp_duty_pct),
                  static_cast<unsigned>(g_last_cp_duty_pct),
                  cp_connected(g_cp_state) ? 1 : 0,
                  controller_heartbeat_alive(now_ms) ? 1 : 0,
                  static_cast<unsigned>(g_ctrl.auth_state),
                  controller_allows_slac_start(now_ms) ? 1 : 0,
                  controller_allows_energy(now_ms) ? 1 : 0,
                  g_ctrl.slac_armed ? 1 : 0,
                  g_ctrl.slac_start_latched ? 1 : 0);
}

void serial_tx_slac_status() {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] EVT SLAC session=%d matched=%d fsm=%u failures=%u\n",
                  g_session_started ? 1 : 0,
                  g_session_matched ? 1 : 0,
                  g_fsm ? static_cast<unsigned>(g_fsm->get_state()) : 0xFFu,
                  static_cast<unsigned>(g_slac_failures_this_cp));
}

void serial_tx_hlc_status(uint32_t now_ms) {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] EVT HLC ready=%d active=%d auth=%u hb=%d precharge_seen=%d precharge_ready=%d fb_valid=%d fb_ready=%d fb_v=%.1f fb_i=%.1f\n",
                  g_hlc_ready ? 1 : 0,
                  g_hlc_active ? 1 : 0,
                  static_cast<unsigned>(g_ctrl.auth_state),
                  controller_heartbeat_alive(now_ms) ? 1 : 0,
                  g_hlc_ctx.precharge_seen ? 1 : 0,
                  g_hlc_ctx.precharge_converged ? 1 : 0,
                  g_ctrl_feedback.valid ? 1 : 0,
                  g_ctrl_feedback.ready ? 1 : 0,
                  static_cast<double>(g_ctrl_feedback.present_voltage_v),
                  static_cast<double>(g_ctrl_feedback.present_current_a));
}

void serial_tx_session_status(uint32_t now_ms) {
    if (!mode_uart_status_stream_active()) return;
    const uint32_t wh = static_cast<uint32_t>(std::max<int64_t>(0, std::min<int64_t>(real_meter_wh_i64(), 65535)));
    Serial.printf("[SERCTRL] EVT SESSION session=%d matched=%d relay1=%d relay2=%d relay3=%d stop_active=%d stop_hard=%d stop_done=%d meter_wh=%lu soc=%u\n",
                  g_session_started ? 1 : 0,
                  g_session_matched ? 1 : 0,
                  g_relay1_closed ? 1 : 0,
                  g_relay2_closed ? 1 : 0,
                  g_relay3_closed ? 1 : 0,
                  g_ctrl.stop_active ? 1 : 0,
                  g_ctrl.stop_hard ? 1 : 0,
                  controller_stop_is_complete() ? 1 : 0,
                  static_cast<unsigned long>(wh),
                  static_cast<unsigned>(encode_last_ev_soc_pct()));
    (void)now_ms;
}

void serial_tx_bms_status() {
    if (!mode_uart_status_stream_active()) return;
    Serial.printf("[SERCTRL] EVT BMS stage=%u(%s) valid=%d delivery_ready=%d target_v=%.1f target_i=%.1f fb_valid=%d fb_ready=%d present_v=%.1f present_i=%.1f curr_lim=%d volt_lim=%d pwr_lim=%d\n",
                  static_cast<unsigned>(g_last_bms_hlc_stage),
                  bms_stage_name(g_last_bms_hlc_stage),
                  g_last_bms_valid ? 1 : 0,
                  g_last_bms_delivery_ready ? 1 : 0,
                  static_cast<double>(g_last_bms_requested_v),
                  static_cast<double>(g_last_bms_requested_i),
                  g_ctrl_feedback.valid ? 1 : 0,
                  g_ctrl_feedback.ready ? 1 : 0,
                  static_cast<double>(g_ctrl_feedback.present_voltage_v),
                  static_cast<double>(g_ctrl_feedback.present_current_a),
                  g_ctrl_feedback.current_limit_achieved ? 1 : 0,
                  g_ctrl_feedback.voltage_limit_achieved ? 1 : 0,
                  g_ctrl_feedback.power_limit_achieved ? 1 : 0);
    g_last_bms_dirty = false;
}

void controller_service_uart_router_tx(uint32_t now_ms) {
    if (!mode_uart_status_stream_active()) {
        return;
    }
    if (!g_serial_tx.local_identity_sent) {
        serial_tx_local_identity();
    }
    if (static_cast<int32_t>(now_ms - g_serial_tx.next_cp_status_ms) >= 0) {
        serial_tx_cp_status(now_ms);
        g_serial_tx.next_cp_status_ms = now_ms + SERIAL_TX_CP_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_serial_tx.next_slac_status_ms) >= 0) {
        serial_tx_slac_status();
        g_serial_tx.next_slac_status_ms = now_ms + SERIAL_TX_SLAC_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_serial_tx.next_hlc_status_ms) >= 0) {
        serial_tx_hlc_status(now_ms);
        g_serial_tx.next_hlc_status_ms = now_ms + SERIAL_TX_HLC_STATUS_MS;
    }
    if (static_cast<int32_t>(now_ms - g_serial_tx.next_session_status_ms) >= 0) {
        serial_tx_session_status(now_ms);
        g_serial_tx.next_session_status_ms = now_ms + SERIAL_TX_SESSION_STATUS_MS;
    }
    if (g_last_bms_dirty || static_cast<int32_t>(now_ms - g_serial_tx.next_bms_status_ms) >= 0) {
        serial_tx_bms_status();
        g_serial_tx.next_bms_status_ms = now_ms + SERIAL_TX_BMS_STATUS_MS;
    }
    if (g_last_seen_ev_mac_valid &&
        static_cast<int32_t>(now_ms - g_serial_tx.next_identity_evt_ms) >= 0 &&
        (g_session_matched || g_hlc_ready || g_hlc_active)) {
        serial_tx_identity_hex("EVMAC", g_last_seen_ev_mac, sizeof(g_last_seen_ev_mac));
        g_serial_tx.next_identity_evt_ms = now_ms + PLC_TX_IDENTITY_RETRY_MS;
    }
}

void controller_service_periodic_tx(uint32_t now_ms) {
    if (!mode_uart_status_stream_active()) {
        return;
    }
    controller_service_uart_router_tx(now_ms);
}

uint32_t parse_u32_token(const String& token, uint32_t fallback) {
    if (token.isEmpty()) return fallback;
    char* end = nullptr;
    const unsigned long v = strtoul(token.c_str(), &end, 0);
    if (end == token.c_str()) return fallback;
    return static_cast<uint32_t>(v);
}

bool parse_mode_token(String token, OperatingMode* out) {
    token.trim();
    token.toLowerCase();
    if (token == "0" || token == "standalone" || token == "local") {
        if (out) *out = OperatingMode::Standalone;
        return true;
    }
    if (token == "1" || token == "external" || token == "external_controller" ||
        token == "uart" || token == "router" || token == "controller_uart_router" ||
        token == "uart_router" || token == "uart_only" ||
        token == "2" || token == "controller" || token == "controller_supported" || token == "supported" ||
        token == "3" || token == "managed" || token == "controller_managed" || token == "full") {
        if (out) *out = OperatingMode::ExternalController;
        return true;
    }
    return false;
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
        case (CAN_ID_CTRL_AUX_RELAY & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.aux;
            break;
        case (CAN_ID_CTRL_HLC_FEEDBACK & 0x1FFFFFF0u):
            seq = &g_serial_ctrl_seq.feedback;
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

void serial_ctrl_send_relay(uint8_t enable_mask, uint8_t state_mask, uint32_t hold_ms) {
    uint8_t p[8]{};
    p[3] = enable_mask;
    p[4] = state_mask;
    p[5] = static_cast<uint8_t>(std::min<uint32_t>(255u, hold_ms / 100u));
    serial_inject_controller_frame(CAN_ID_CTRL_AUX_RELAY, p);
}

void serial_ctrl_send_hlc_feedback(bool valid,
                                   bool ready,
                                   float present_v,
                                   float present_i,
                                   bool current_limit,
                                   bool voltage_limit,
                                   bool power_limit,
                                   bool stop_charging) {
    uint8_t p[8]{};
    const uint32_t packed = pack_ctrl_hlc_feedback(
        valid, ready, current_limit, voltage_limit, power_limit, stop_charging, present_v, present_i);
    p[3] = static_cast<uint8_t>(packed & 0xFFu);
    p[4] = static_cast<uint8_t>((packed >> 8u) & 0xFFu);
    p[5] = static_cast<uint8_t>((packed >> 16u) & 0xFFu);
    p[6] = static_cast<uint8_t>((packed >> 24u) & 0xFFu);
    serial_inject_controller_frame(CAN_ID_CTRL_HLC_FEEDBACK, p);
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
    Serial.println("  CTRL RELAY <enable_mask> <state_mask> [hold_ms]");
    Serial.println("  CTRL FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1] [stop_notify0|1]");
    Serial.println("  CTRL STOP <soft|hard|clear> [timeout_ms]");
    Serial.println("  CTRL LED <booting|available|preparing|charging|finishing|faulted|emergency>");
    Serial.println("  CTRL MODE / CTRL OWNERSHIP / CTRL SAVE are disabled at runtime");
    Serial.println("    Use the SW4 setup portal to change persisted PLC settings, then reboot");
    Serial.println("  CTRL STATUS");
    Serial.println("  mode1/external_controller: PLC bridges CP/SLAC/HLC, controller owns relay decisions + module CAN");
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
        Serial.println("[SERCTRL] RESET done (runtime/session state only; persisted settings unchanged)");
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
    if (op == "FEEDBACK") {
        if (t.size() < 6) {
            Serial.println("[SERCTRL] FEEDBACK <valid0|1> <ready0|1> <present_v> <present_i> [curr_lim0|1] [volt_lim0|1] [pwr_lim0|1]");
            return;
        }
        const bool valid = parse_u32_token(t[2], 0u) != 0u;
        const bool ready = parse_u32_token(t[3], 0u) != 0u;
        const float present_v = t[4].toFloat();
        const float present_i = t[5].toFloat();
        const bool current_limit = (t.size() >= 7) ? (parse_u32_token(t[6], 0u) != 0u) : false;
        const bool voltage_limit = (t.size() >= 8) ? (parse_u32_token(t[7], 0u) != 0u) : false;
        const bool power_limit = (t.size() >= 9) ? (parse_u32_token(t[8], 0u) != 0u) : false;
        const bool stop_charging = (t.size() >= 10) ? (parse_u32_token(t[9], 0u) != 0u) : false;
        serial_ctrl_send_hlc_feedback(valid, ready, present_v, present_i, current_limit, voltage_limit, power_limit, stop_charging);
        Serial.printf("[SERCTRL] FEEDBACK valid=%d ready=%d present_v=%.1f present_i=%.1f curr_lim=%d volt_lim=%d pwr_lim=%d stop_notify=%d\n",
                      valid ? 1 : 0,
                      ready ? 1 : 0,
                      static_cast<double>(present_v),
                      static_cast<double>(present_i),
                      current_limit ? 1 : 0,
                      voltage_limit ? 1 : 0,
                      power_limit ? 1 : 0,
                      stop_charging ? 1 : 0);
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
    if (op == "LED") {
        if (t.size() < 3) {
            Serial.println("[SERCTRL] LED <booting|available|preparing|charging|finishing|faulted|emergency>");
            return;
        }
        LedPreset preset = g_led_state.preset;
        if (!parse_led_preset_token(t[2], &preset)) {
            Serial.println("[SERCTRL] LED invalid preset");
            return;
        }
        g_led_state.preset = preset;
        g_led_state.last_update_ms = millis();
        Serial.printf("[SERCTRL] LED preset=%s count=%u\n",
                      led_preset_name(g_led_state.preset),
                      static_cast<unsigned>(active_led_count()));
        return;
    }
    if (op == "MODE") {
        Serial.println("[SERCTRL] MODE disabled at runtime; use the SW4 setup portal and reboot");
        return;
    }
    if (op == "OWNERSHIP") {
        Serial.println("[SERCTRL] OWNERSHIP disabled at runtime; use the SW4 setup portal and reboot");
        return;
    }
    if (op == "SAVE") {
        Serial.println("[SERCTRL] SAVE disabled at runtime; use the SW4 setup portal to persist settings");
        return;
    }
    if (op == "STATUS") {
        Serial.printf("[SERCTRL] STATUS mode=%u(%s) plc_id=%u connector_id=%u controller_id=%u module_addr=0x%02X local_group=%u module_id=%s can_stack=%d module_mgr=%d cp=%s duty=%u hb=%d auth=%u allow_slac=%d allow_energy=%d armed=%d start=%d relay1=%d relay2=%d relay3=%d alloc_sz=%u ctrl_fb=%d stop_active=%d stop_hard=%d stop_done=%d emergency=%d led=%s led_count=%u\n",
                      static_cast<unsigned>(g_runtime_cfg.mode),
                      runtime_mode_name(),
                      static_cast<unsigned>(g_runtime_cfg.plc_id),
                      static_cast<unsigned>(g_runtime_cfg.connector_id),
                      static_cast<unsigned>(g_runtime_cfg.controller_id),
                      static_cast<unsigned>(g_runtime_cfg.local_module_address),
                      static_cast<unsigned>(g_local_module_group),
                      runtime_local_module_id().c_str(),
                      mode_uses_plc_can_stack() ? 1 : 0,
                      mode_uses_plc_module_manager() ? 1 : 0,
                      cp_phase_label(g_cp_state, g_last_cp_duty_pct),
                      static_cast<unsigned>(g_last_cp_duty_pct),
                      controller_heartbeat_alive(millis()) ? 1 : 0,
                      static_cast<unsigned>(g_ctrl.auth_state),
                      controller_allows_slac_start(millis()) ? 1 : 0,
                      controller_allows_energy(millis()) ? 1 : 0,
                      g_ctrl.slac_armed ? 1 : 0,
                      g_ctrl.slac_start_latched ? 1 : 0,
                      g_relay1_closed ? 1 : 0,
                      g_relay2_closed ? 1 : 0,
                      g_relay3_closed ? 1 : 0,
                      static_cast<unsigned>(g_module_allowlist.size()),
                      controller_feedback_fresh(millis()) ? 1 : 0,
                      g_ctrl.stop_active ? 1 : 0,
                      g_ctrl.stop_hard ? 1 : 0,
                      controller_stop_is_complete() ? 1 : 0,
                      g_emergency_stop_active ? 1 : 0,
                      led_preset_name(effective_led_preset()),
                      static_cast<unsigned>(active_led_count()));
        return;
    }

    Serial.printf("[SERCTRL] unknown command: %s\n", line.c_str());
}

void service_serial_commands() {
    uint16_t processed = 0u;
    while (Serial.available() > 0) {
        const int ch = Serial.read();
        if (ch < 0) break;
        processed++;
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
        if ((processed % SERIAL_CMD_BREATHER_STRIDE) == 0u && Serial.available() > 0) {
            cooperative_runtime_breather(CrashBreadcrumbStage::LoopAfterSerial, processed);
        }
        if (processed >= SERIAL_CMD_MAX_BYTES_PER_LOOP) {
            break;
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
    log_runtime_stats("lwip_socket_stack_ready");
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

bool is_zero_mac_addr(const uint8_t mac[ETH_ALEN]) {
    static constexpr uint8_t kZeroMac[ETH_ALEN] = {0};
    return mac && memcmp(mac, kZeroMac, ETH_ALEN) == 0;
}

bool is_broadcast_mac_addr(const uint8_t mac[ETH_ALEN]) {
    static constexpr uint8_t kBroadcastMac[ETH_ALEN] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    return mac && memcmp(mac, kBroadcastMac, ETH_ALEN) == 0;
}

bool is_valid_unicast_mac_addr(const uint8_t mac[ETH_ALEN]) {
    return mac && !is_zero_mac_addr(mac) && !is_broadcast_mac_addr(mac) && ((mac[0] & 0x01u) == 0u);
}

void log_secc_link_local() {
    Serial.printf("[NET] SECC IPv6 LL fe80::%02x%02xff:fe%02x:%02x%02x\n",
                  static_cast<unsigned>(g_secc_ip[8]),
                  static_cast<unsigned>(g_secc_ip[9]),
                  static_cast<unsigned>(g_secc_ip[10]),
                  static_cast<unsigned>(g_secc_ip[13]),
                  static_cast<unsigned>(g_secc_ip[14]),
                  static_cast<unsigned>(g_secc_ip[15]));
}

void refresh_plc_link_local_identity() {
    build_link_local_from_mac(g_local_mac, g_secc_ip);
    log_secc_link_local();
}

void update_plc_interface_mac(const uint8_t mac[ETH_ALEN], const char* source) {
    if (!is_valid_unicast_mac_addr(mac)) {
        return;
    }

    const bool had_prior_mac = is_valid_unicast_mac_addr(g_local_mac);
    const bool changed = !had_prior_mac || memcmp(g_local_mac, mac, ETH_ALEN) != 0;
    if (!changed) {
        return;
    }

    uint8_t previous_mac[ETH_ALEN]{};
    memcpy(previous_mac, g_local_mac, sizeof(previous_mac));
    memcpy(g_local_mac, mac, ETH_ALEN);
    g_channel.set_local_mac(g_local_mac);
    mark_lwip_tx_transport_flush("plc mac update");
    refresh_plc_link_local_identity();

    if (g_plc_netif_ready) {
        LOCK_TCPIP_CORE();
        memcpy(g_plc_netif.hwaddr, g_local_mac, ETH_ALEN);
#if LWIP_IPV6
        netif_create_ip6_linklocal_address(&g_plc_netif, 1);
#endif
        UNLOCK_TCPIP_CORE();
    }

    if (had_prior_mac) {
        Serial.printf("[NET] PLC/QCA MAC updated from %s: %02X:%02X:%02X:%02X:%02X:%02X -> %02X:%02X:%02X:%02X:%02X:%02X\n",
                      source ? source : "runtime",
                      previous_mac[0], previous_mac[1], previous_mac[2],
                      previous_mac[3], previous_mac[4], previous_mac[5],
                      g_local_mac[0], g_local_mac[1], g_local_mac[2],
                      g_local_mac[3], g_local_mac[4], g_local_mac[5]);
    } else {
        Serial.printf("[NET] PLC/QCA MAC learned from %s: %02X:%02X:%02X:%02X:%02X:%02X\n",
                      source ? source : "runtime",
                      g_local_mac[0], g_local_mac[1], g_local_mac[2],
                      g_local_mac[3], g_local_mac[4], g_local_mac[5]);
    }
}

void maybe_learn_qca_mac_from_homeplug_frame(const uint8_t* frame, uint16_t len) {
    if (!frame || len < 17u) {
        return;
    }
    const uint16_t eth_type = static_cast<uint16_t>((frame[12] << 8) | frame[13]);
    if (eth_type != slac::defs::ETH_P_HOMEPLUG_GREENPHY) {
        return;
    }
    const uint16_t mmtype = static_cast<uint16_t>(frame[15] | (static_cast<uint16_t>(frame[16]) << 8u));
    if (mmtype != (slac::defs::MMTYPE_CM_SET_KEY | slac::defs::MMTYPE_MODE_CNF)) {
        return;
    }
    update_plc_interface_mac(frame + 6, "CM_SET_KEY.CNF");
}

bool ensure_lwip_tx_queue_ready() {
    if (g_lwip_tx_queue && g_lwip_tx_free_queue && g_lwip_tx_slots) {
        return true;
    }

    if (!g_lwip_tx_slots) {
        g_lwip_tx_slots = static_cast<LwipTxSlot*>(alloc_bulk_storage(sizeof(LwipTxSlot) * LWIP_TX_QUEUE_DEPTH,
                                                                      "lwip_tx_slots"));
        if (!g_lwip_tx_slots) {
            return false;
        }
    }

    if (!g_lwip_tx_queue) {
        g_lwip_tx_queue = xQueueCreateStatic(LWIP_TX_QUEUE_DEPTH,
                                             sizeof(uint8_t),
                                             g_lwip_tx_ready_queue_storage.data(),
                                             &g_lwip_tx_ready_queue_struct);
    }
    if (!g_lwip_tx_free_queue) {
        g_lwip_tx_free_queue = xQueueCreateStatic(LWIP_TX_QUEUE_DEPTH,
                                                  sizeof(uint8_t),
                                                  g_lwip_tx_free_queue_storage.data(),
                                                  &g_lwip_tx_free_queue_struct);
    }
    if (!g_lwip_tx_queue || !g_lwip_tx_free_queue) {
        Serial.println("[NET] lwIP TX queue create failed");
        return false;
    }

    xQueueReset(g_lwip_tx_queue);
    xQueueReset(g_lwip_tx_free_queue);
    for (uint8_t slot = 0u; slot < LWIP_TX_QUEUE_DEPTH; ++slot) {
        (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
    }
    log_runtime_stats("lwip_tx_pool_ready");
    return true;
}

void mark_lwip_tx_transport_flush(const char* reason) {
    uint32_t next_epoch = g_lwip_tx_epoch + 1u;
    if (next_epoch == 0u) {
        next_epoch = 1u;
    }
    g_lwip_tx_epoch = next_epoch;
    const uint32_t now = millis();
    if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
        Serial.printf("[NET] lwIP TX transport flush epoch=%lu reason=%s\n",
                      static_cast<unsigned long>(next_epoch),
                      reason ? reason : "-");
        g_next_lwip_tx_drop_log_ms = now + 2000;
    }
}

err_t plc_netif_linkoutput(struct netif* netif, struct pbuf* p) {
    (void)netif;
    if (!p || !g_lwip_tx_queue || !g_lwip_tx_free_queue || !g_lwip_tx_slots) {
        return ERR_IF;
    }

    const uint16_t total = static_cast<uint16_t>(p->tot_len);
    if (total == 0 || total > ETH_FRAME_LEN) {
        return ERR_IF;
    }

    uint8_t slot = 0u;
    if (xQueueReceive(g_lwip_tx_free_queue, &slot, 0) != pdTRUE) {
        const uint32_t now = millis();
        if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
            Serial.println("[NET] lwIP TX pool exhausted, dropping frame");
            g_next_lwip_tx_drop_log_ms = now + 2000;
        }
        return ERR_MEM;
    }
    if (slot >= LWIP_TX_QUEUE_DEPTH) {
        return ERR_BUF;
    }
    LwipTxSlot& frame = g_lwip_tx_slots[slot];
    frame.len = total;
    frame.epoch = g_lwip_tx_epoch;
    if (pbuf_copy_partial(p, frame.data, total, 0) != total) {
        (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
        return ERR_BUF;
    }
    if (xQueueSend(g_lwip_tx_queue, &slot, 0) != pdTRUE) {
        (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
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
    refresh_plc_link_local_identity();
    Serial.printf("[NET] PLC lwIP netif ready: %s\n", g_plc_ifname);
    log_runtime_stats("lwip_netif_ready");
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
    if (!g_lwip_tx_queue || !g_lwip_tx_free_queue || !g_lwip_tx_slots || !g_transport || !g_transport->is_ready()) {
        return;
    }
    uint8_t slot = 0u;
    uint8_t drained = 0u;
    const uint32_t current_epoch = g_lwip_tx_epoch;
    while (drained < LWIP_TX_MAX_FRAMES_PER_LOOP &&
           xQueueReceive(g_lwip_tx_queue, &slot, 0) == pdTRUE) {
        if (slot >= LWIP_TX_QUEUE_DEPTH) {
            drained++;
            continue;
        }
        LwipTxSlot& frame = g_lwip_tx_slots[slot];
        if (frame.epoch != current_epoch || frame.len == 0 || frame.len > ETH_FRAME_LEN) {
            frame.len = 0u;
            (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
            drained++;
            continue;
        }
        const auto rc = g_transport->write_nowait(frame.data, frame.len);
        if (rc != slac::ITransport::IOResult::Ok) {
            if (xQueueSendToFront(g_lwip_tx_queue, &slot, 0) != pdTRUE) {
                frame.len = 0u;
                (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
            }
            const uint32_t now = millis();
            if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
                Serial.printf("[NET] lwIP TX backpressure rc=%d err=%s\n",
                              static_cast<int>(rc),
                              g_transport->get_error().c_str());
                g_next_lwip_tx_drop_log_ms = now + 2000;
            }
            break;
        }
        frame.len = 0u;
        (void)xQueueSend(g_lwip_tx_free_queue, &slot, 0);
        drained++;
    }
    if (uxQueueMessagesWaiting(g_lwip_tx_queue) > 0u) {
        const uint32_t now = millis();
        if (g_next_lwip_tx_drop_log_ms == 0 || static_cast<int32_t>(now - g_next_lwip_tx_drop_log_ms) >= 0) {
            Serial.printf("[NET] lwIP TX backlog pending=%lu drained=%u perLoop=%u\n",
                          static_cast<unsigned long>(uxQueueMessagesWaiting(g_lwip_tx_queue)),
                          static_cast<unsigned>(drained),
                          static_cast<unsigned>(LWIP_TX_MAX_FRAMES_PER_LOOP));
            g_next_lwip_tx_drop_log_ms = now + 2000;
        }
    }
}

void hlc_reset_session_state(HlcAppContext* ctx) {
    if (!ctx) return;
    ctx->auth_seen = false;
    ctx->auth_ongoing_sent = false;
    ctx->precharge_seen = false;
    ctx->precharge_converged = false;
    ctx->power_delivery_enabled = false;
    ctx->stop_requested = false;
    ctx->precharge_count = 0;
    ctx->sa_schedule_tuple_id = 1u;
    g_hlc_disconnect_hold_until_ms = 0;
    clear_last_bms_request();
}

bool controller_get_managed_feedback(uint32_t now_ms, ControllerManagedFeedbackState* out) {
    if (!out) return false;
    if (!mode_controller_uses_external_feedback()) return false;
    if (!controller_feedback_fresh(now_ms)) return false;
    if (!g_ctrl_feedback.valid) return false;
    *out = g_ctrl_feedback;
    return true;
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
        if (mode_requires_controller_contract()) {
            const uint32_t now_ms = millis();
            const bool hb_ok = mode_controller_has_final_decision() ? true : controller_heartbeat_alive(now_ms);
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
        Serial.printf("[HLC][AUTH][ISO] rc=%d proc=%d mode=%u ctrl_auth=%d hb=%d\n",
                      static_cast<int>(rc),
                      static_cast<int>(proc),
                      static_cast<int>(g_runtime_cfg.mode),
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
        if (mode_requires_controller_contract()) {
            const uint32_t now_ms = millis();
            const bool hb_ok = mode_controller_has_final_decision() ? true : controller_heartbeat_alive(now_ms);
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
        Serial.printf("[HLC][AUTH][DIN] rc=%d proc=%d mode=%u ctrl_auth=%d hb=%d\n",
                      static_cast<int>(rc),
                      static_cast<int>(proc),
                      static_cast<int>(g_runtime_cfg.mode),
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

    if (mode_controller_uses_external_feedback()) {
        const uint32_t now_ms = millis();
        ControllerManagedFeedbackState fb{};
        const bool fb_ok = controller_get_managed_feedback(now_ms, &fb);
        const bool stop_charging = fb_ok && fb.stop_charging;
        if (!allow_energy && !mode_controller_has_final_decision()) {
            request_modules_off("PreChargeBlocked");
            (void)relay1_set(false, "PreChargeBlocked");
            ctx->precharge_converged = false;
            ctx->power_delivery_enabled = false;
        }
        const bool precharge_ready = allow_energy && fb_ok && fb.ready;
        ctx->precharge_converged = precharge_ready;
        ctx->power_delivery_enabled = false;
        const float present_v = fb_ok ? sane_non_negative(fb.present_voltage_v) : snapshot_present_group_voltage_v();

        const uint8_t* sid = ctx->secc->session_id;
        if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
            iso2_PreChargeResType res;
            init_iso2_PreChargeResType(&res);
            if (precharge_ready) {
                set_iso_dc_status_ready(&res.DC_EVSEStatus);
            } else {
                set_iso_dc_status_not_ready(
                    &res.DC_EVSEStatus,
                    stop_charging ? iso2_EVSENotificationType_StopCharging : iso2_EVSENotificationType_None);
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
            if (precharge_ready) {
                set_din_dc_status_ready(&res.DC_EVSEStatus);
            } else {
                set_din_dc_status_not_ready(
                    &res.DC_EVSEStatus,
                    stop_charging ? din_EVSENotificationType_StopCharging : din_EVSENotificationType_None);
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

    if (allow_energy) {
        (void)modules_apply_target(true, req_v, req_i, "PreCharge");
    } else {
        request_modules_off("PreChargeBlocked");
        req_i = 0.0f;
        ctx->precharge_converged = false;
        ctx->power_delivery_enabled = false;
    }

    if (allow_energy && !g_relay1_closed) {
        (void)relay1_set(true, "PreCharge");
    }
    bool precharge_ready = refresh_precharge_convergence(ctx, req_v);
    if (!precharge_ready && standalone_precharge_start_ready(ctx, req_v)) {
        ctx->precharge_converged = true;
        precharge_ready = true;
    }
    cbmodules::GroupStatus present_st{};
    float present_v = 0.0f;
    float present_i = 0.0f;
    (void)snapshot_live_present_values(&present_st, &present_v, &present_i);
    Serial.printf("[HLC] {\"msg\":\"PreChargeRuntime\",\"reqV\":%.1f,\"reqI\":%.1f,\"allow\":%d,\"ready\":%d,"
                  "\"presentV\":%.1f,\"presentI\":%.2f,\"relay1\":%d,\"prechargeDone\":%d}\n",
                  req_v,
                  req_i,
                  allow_energy ? 1 : 0,
                  precharge_ready ? 1 : 0,
                  present_v,
                  present_i,
                  g_relay1_closed ? 1 : 0,
                  ctx->precharge_converged ? 1 : 0);

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_PreChargeResType res;
        init_iso2_PreChargeResType(&res);
        if (precharge_ready) {
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
        if (precharge_ready) {
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

    float req_v = g_last_requested_target_v;
    float req_i = g_last_requested_target_i;
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
    const float requested_v = req_v;
    const float requested_i = req_i;
    const uint32_t now_ms = millis();
    const bool allow_energy = controller_allows_energy(now_ms);
    if (mode_controller_uses_external_feedback()) {
        ControllerManagedFeedbackState fb{};
        const bool fb_ok = controller_get_managed_feedback(now_ms, &fb);
        const bool stop_charging = fb_ok && fb.stop_charging;
        const bool response_ready = allow_energy && fb_ok && fb.ready;
        if (!allow_energy && !mode_controller_has_final_decision()) {
            request_modules_off("CurrentDemandBlocked");
            (void)relay1_set(false, "CurrentDemandBlocked");
        }
        ctx->precharge_converged = response_ready || ctx->precharge_converged;
        ctx->power_delivery_enabled = response_ready && g_relay1_closed;
        update_last_bms_request(requested_v, requested_i, 2u, response_ready, true);

        float present_v = fb_ok ? sane_non_negative(fb.present_voltage_v) : sane_non_negative(g_last_measured_v);
        const float present_i = fb_ok ? sane_non_negative(fb.present_current_a) : 0.0f;
        if (!fb_ok && present_v <= 0.1f) {
            present_v = snapshot_present_group_voltage_v();
        }

        const bool current_limited = fb_ok ? fb.current_limit_achieved : false;
        const bool power_limited = fb_ok ? fb.power_limit_achieved : false;
        const bool voltage_limited = fb_ok ? fb.voltage_limit_achieved : false;
        const uint8_t* sid = ctx->secc->session_id;
        if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
            iso2_CurrentDemandResType res;
            init_iso2_CurrentDemandResType(&res);
            if (response_ready) {
                if (stop_charging) {
                    set_iso_dc_status_shutdown(&res.DC_EVSEStatus, iso2_EVSENotificationType_StopCharging);
                } else {
                    set_iso_dc_status_ready(&res.DC_EVSEStatus);
                }
            } else {
                set_iso_dc_status_not_ready(
                    &res.DC_EVSEStatus,
                    stop_charging ? iso2_EVSENotificationType_StopCharging : iso2_EVSENotificationType_None);
            }
            set_iso_physical(&res.EVSEPresentVoltage, iso2_unitSymbolType_V, present_v);
            set_iso_physical(&res.EVSEPresentCurrent, iso2_unitSymbolType_A, present_i);
            res.EVSECurrentLimitAchieved = current_limited ? 1 : 0;
            res.EVSEVoltageLimitAchieved = voltage_limited ? 1 : 0;
            res.EVSEPowerLimitAchieved = power_limited ? 1 : 0;
            fill_iso_meter_info_real(&res.MeterInfo);
            res.MeterInfo_isUsed = 1;
            res.EVSEID.charactersLen = copy_iso_evse_id(res.EVSEID.characters);
            res.SAScheduleTupleID = ctx->sa_schedule_tuple_id;
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
            if (response_ready) {
                if (stop_charging) {
                    set_din_dc_status_shutdown(&res.DC_EVSEStatus, din_EVSENotificationType_StopCharging);
                } else {
                    set_din_dc_status_ready(&res.DC_EVSEStatus);
                }
            } else {
                set_din_dc_status_not_ready(
                    &res.DC_EVSEStatus,
                    stop_charging ? din_EVSENotificationType_StopCharging : din_EVSENotificationType_None);
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
    if (allow_energy && ctx->precharge_seen && !ctx->precharge_converged) {
        (void)refresh_precharge_convergence(ctx, requested_v);
        if (!ctx->precharge_converged && standalone_precharge_start_ready(ctx, requested_v)) {
            ctx->precharge_converged = true;
        }
    }
    if (allow_energy && ctx->precharge_converged && req_i > 0.05f && !ctx->power_delivery_enabled) {
        (void)ensure_power_delivery_started(ctx, requested_v, "CurrentDemandStart");
    }
    const bool delivery_active = allow_energy && ctx->power_delivery_enabled && g_relay1_closed;
    const bool current_requested = (req_i > 0.05f);
    const uint32_t baseline_telemetry_ms = latest_local_group_telemetry_ms();
    bool target_applied = false;
    if (delivery_active) {
        target_applied = modules_apply_target(true,
                                              req_v,
                                              current_requested ? req_i : 0.0f,
                                              current_requested ? "CurrentDemand" : "CurrentDemandHoldZero");
    } else if (allow_energy && ctx->precharge_seen) {
        target_applied = modules_apply_target(true,
                                              requested_v,
                                              std::min(PRECHARGE_CURRENT_LIMIT_A, requested_i),
                                              "CurrentDemandHold");
    } else {
        request_modules_off("CurrentDemandBlocked");
    }
    const float effective_req_i = delivery_active ? sane_non_negative(g_last_module_target_i) : 0.0f;
    const float effective_req_power_kw = (requested_v * effective_req_i) / 1000.0f;

    cbmodules::GroupStatus st{};
    float present_v = 0.0f;
    float present_i = 0.0f;
    bool fresh_after_target = false;
    const bool telemetry_valid = snapshot_post_target_present_values(
        baseline_telemetry_ms, &st, &present_v, &present_i, &fresh_after_target);
    if (!current_requested) {
        present_i = 0.0f;
    }
    const bool response_ready = delivery_active && target_applied && g_relay1_closed &&
                                (telemetry_valid || present_v > 0.1f || present_i > 0.05f);
    update_last_bms_request(requested_v, requested_i, 2u, response_ready, true);

    const float req_power_kw = (requested_v * requested_i) / 1000.0f;
    const float avail_i = sane_non_negative(st.available_current_a);
    const float avail_p = sane_non_negative(st.available_power_kw);
    const bool current_limited =
        (!delivery_active && requested_i > 0.05f) ||
        st.saturated || (requested_i > (effective_req_i + 0.1f)) || (avail_i > 0.0f && requested_i > (avail_i + 0.1f));
    const bool power_limited =
        (!delivery_active && req_power_kw > 0.1f) ||
        st.saturated || (req_power_kw > (effective_req_power_kw + 0.1f)) || (avail_p > 0.0f && req_power_kw > (avail_p + 0.1f));
    const bool voltage_limited = requested_v > (MODULE_MAX_VOLTAGE_V + 0.5f);
    Serial.printf("[HLC] {\"msg\":\"CurrentDemandRuntime\",\"reqV\":%.1f,\"reqI\":%.1f,\"appliedV\":%.1f,"
                  "\"appliedI\":%.2f,\"allow\":%d,\"delivery\":%d,\"targetApplied\":%d,\"ready\":%d,\"hold\":%d,"
                  "\"presentV\":%.1f,\"presentI\":%.2f,\"relay1\":%d,\"prechargeDone\":%d,\"telemetryValid\":%d,"
                  "\"sampleFresh\":%d,\"saturated\":%d}\n",
                  requested_v,
                  requested_i,
                  g_last_module_target_v,
                  g_last_module_target_i,
                  allow_energy ? 1 : 0,
                  delivery_active ? 1 : 0,
                  target_applied ? 1 : 0,
                  response_ready ? 1 : 0,
                  current_requested ? 0 : 1,
                  present_v,
                  present_i,
                  g_relay1_closed ? 1 : 0,
                  ctx->precharge_converged ? 1 : 0,
                  telemetry_valid ? 1 : 0,
                  fresh_after_target ? 1 : 0,
                  st.saturated ? 1 : 0);

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_CurrentDemandResType res;
        init_iso2_CurrentDemandResType(&res);
        if (response_ready) {
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
        res.EVSEID.charactersLen = copy_iso_evse_id(res.EVSEID.characters);
        res.SAScheduleTupleID = ctx->sa_schedule_tuple_id;
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
        if (response_ready) {
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

int hlc_handle_power_delivery(HlcAppContext* ctx,
                              const jpv2g_secc_request_t* req,
                              uint8_t* out,
                              size_t out_len,
                              size_t* written) {
    if (!ctx || !ctx->secc || !req) return -EINVAL;

    const bool allow_energy = controller_allows_energy(millis());
    bool start = false;
    bool stop = false;
    bool renegotiate = false;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2 && req->body) {
        const auto* rq = static_cast<const iso2_PowerDeliveryReqType*>(req->body);
        start = (rq->ChargeProgress == iso2_chargeProgressType_Start);
        stop = (rq->ChargeProgress == iso2_chargeProgressType_Stop);
        renegotiate = (rq->ChargeProgress == iso2_chargeProgressType_Renegotiate);
        ctx->sa_schedule_tuple_id = (rq->SAScheduleTupleID == 0u) ? 1u : rq->SAScheduleTupleID;
    } else if (req->protocol == JPV2G_PROTOCOL_DIN70121 && req->body) {
        const auto* rq = static_cast<const din_PowerDeliveryReqType*>(req->body);
        start = (rq->ReadyToChargeState != 0);
        stop = !start;
        renegotiate = false;
    }

    bool applied = false;
    bool ready = false;
    bool stop_charging = false;
    if (stop) {
        ctx->stop_requested = true;
        ctx->power_delivery_enabled = false;
        update_last_bms_request(g_last_requested_target_v, 0.0f, 3u, false, true);
        if (!mode_controller_has_final_decision()) {
            controller_clear_managed_power_cache();
            (void)relay1_set(false, "PowerDeliveryStop");
            request_modules_off("PowerDeliveryStop");
        }
        ctx->precharge_converged = false;
        clear_last_bms_request();
        applied = true;
        ready = false;
        stop_charging = true;
    } else if (renegotiate) {
        ctx->stop_requested = false;
        ctx->power_delivery_enabled = false;
        update_last_bms_request(g_last_requested_target_v, 0.0f, 3u, false, true);
        if (!mode_controller_has_final_decision()) {
            (void)relay1_set(false, "PowerDeliveryRenegotiate");
            request_modules_off("PowerDeliveryRenegotiate");
        }
        ctx->precharge_converged = false;
        applied = true;
        ready = false;
    } else if (start) {
        ctx->stop_requested = false;
        if (!mode_controller_has_final_decision() && !allow_energy) {
            applied = false;
        } else if (mode_controller_uses_external_feedback()) {
            ControllerManagedFeedbackState fb{};
            const bool fb_ok = controller_get_managed_feedback(millis(), &fb);
            const float requested_v = sane_non_negative(g_last_requested_target_v);
            const bool relaxed_start_ready =
                fb_ok && controller_precharge_start_ready(ctx, requested_v, fb);
            ready = allow_energy && fb_ok && g_relay1_closed && relaxed_start_ready;
            ctx->precharge_converged = ctx->precharge_converged || ready;
            ctx->power_delivery_enabled = ready;
            applied = ready;
        } else {
            const bool standalone_path_armed =
                !mode_controller_has_final_decision() && ctx->precharge_seen && g_relay1_closed && g_module_output_enabled;
            if (!ctx->precharge_converged) {
                (void)refresh_precharge_convergence(ctx, g_last_requested_target_v);
                if (!ctx->precharge_converged && standalone_precharge_start_ready(ctx, g_last_requested_target_v)) {
                    ctx->precharge_converged = true;
                }
            }
            if (ctx->precharge_converged || standalone_path_armed) {
                if (standalone_path_armed && !ctx->precharge_converged) {
                    Serial.printf("[HLC] {\"msg\":\"PowerDeliveryArmedStart\",\"targetV\":%.1f,"
                                  "\"relay1\":%d,\"moduleOn\":%d}\n",
                                  g_last_requested_target_v,
                                  g_relay1_closed ? 1 : 0,
                                  g_module_output_enabled ? 1 : 0);
                    ctx->precharge_converged = true;
                }
                applied = ensure_power_delivery_started(ctx, g_last_requested_target_v, "PowerDeliveryStart");
                ready = applied && g_relay1_closed;
            }
        }
    }

    const float logged_applied_v =
        mode_controller_uses_external_feedback() ? sane_non_negative(g_last_requested_target_v) : g_last_module_target_v;
    const float logged_applied_i =
        mode_controller_uses_external_feedback() ? sane_non_negative(g_last_requested_target_i) : g_last_module_target_i;
    Serial.printf("[HLC] {\"msg\":\"PowerDeliveryRuntime\",\"start\":%d,\"stop\":%d,\"renegotiate\":%d,"
                  "\"allow\":%d,\"applied\":%d,\"ready\":%d,\"relay1\":%d,\"prechargeDone\":%d,"
                  "\"targetV\":%.1f,\"appliedV\":%.1f,\"appliedI\":%.2f}\n",
                  start ? 1 : 0,
                  stop ? 1 : 0,
                  renegotiate ? 1 : 0,
                  allow_energy ? 1 : 0,
                  applied ? 1 : 0,
                  ready ? 1 : 0,
                  g_relay1_closed ? 1 : 0,
                  ctx->precharge_converged ? 1 : 0,
                  g_last_requested_target_v,
                  logged_applied_v,
                  logged_applied_i);

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_PowerDeliveryResType res;
        init_iso2_PowerDeliveryResType(&res);
        res.DC_EVSEStatus_isUsed = 1;
        if (stop_charging) {
            set_iso_dc_status_shutdown(&res.DC_EVSEStatus);
        } else if (ready) {
            set_iso_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_iso_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        const iso2_responseCodeType code =
            (stop || renegotiate || ready) ? iso2_responseCodeType_OK : iso2_responseCodeType_FAILED_PowerDeliveryNotApplied;
        return jpv2g_cbv2g_encode_power_delivery_res(sid, code, &res, out, out_len, written);
    }

    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        din_PowerDeliveryResType res;
        init_din_PowerDeliveryResType(&res);
        if (stop_charging) {
            set_din_dc_status_shutdown(&res.DC_EVSEStatus);
        } else if (ready) {
            set_din_dc_status_ready(&res.DC_EVSEStatus);
        } else {
            set_din_dc_status_not_ready(&res.DC_EVSEStatus);
        }
        const din_responseCodeType code =
            (stop || renegotiate || ready) ? din_responseCodeType_OK : din_responseCodeType_FAILED_PowerDeliveryNotApplied;
        return jpv2g_cbv2g_encode_din_power_delivery_res(sid, code, &res, out, out_len, written);
    }

    return -ENOTSUP;
}

int hlc_handle_welding_detection(HlcAppContext* ctx,
                                 const jpv2g_secc_request_t* req,
                                 uint8_t* out,
                                 size_t out_len,
                                 size_t* written) {
    if (!ctx || !ctx->secc || !req) return -EINVAL;

    const uint32_t now_ms = millis();
    const float present_v = snapshot_welding_present_voltage_v(now_ms);
    const bool shutdown = ctx->stop_requested || !ctx->power_delivery_enabled || !g_relay1_closed;

    Serial.printf("[HLC] {\"msg\":\"WeldingDetectionRuntime\",\"stopRequested\":%d,\"shutdown\":%d,"
                  "\"relay1\":%d,\"presentV\":%.1f,\"measuredV\":%.1f,\"moduleTargetV\":%.1f,"
                  "\"moduleTargetI\":%.2f}\n",
                  ctx->stop_requested ? 1 : 0,
                  shutdown ? 1 : 0,
                  g_relay1_closed ? 1 : 0,
                  present_v,
                  static_cast<double>(g_last_measured_v),
                  static_cast<double>(g_last_module_target_v),
                  static_cast<double>(g_last_module_target_i));

    const uint8_t* sid = ctx->secc->session_id;
    if (req->protocol == JPV2G_PROTOCOL_ISO15118_2) {
        iso2_WeldingDetectionResType res;
        init_iso2_WeldingDetectionResType(&res);
        if (shutdown) {
            set_iso_dc_status_shutdown(&res.DC_EVSEStatus, iso2_EVSENotificationType_StopCharging);
        } else {
            set_iso_dc_status_ready(&res.DC_EVSEStatus);
        }
        set_iso_physical(&res.EVSEPresentVoltage, iso2_unitSymbolType_V, present_v);
        return jpv2g_cbv2g_encode_welding_detection_res(
            sid, iso2_responseCodeType_OK, &res, out, out_len, written);
    }

    if (req->protocol == JPV2G_PROTOCOL_DIN70121) {
        din_WeldingDetectionResType res;
        init_din_WeldingDetectionResType(&res);
        if (shutdown) {
            set_din_dc_status_shutdown(&res.DC_EVSEStatus, din_EVSENotificationType_StopCharging);
        } else {
            set_din_dc_status_ready(&res.DC_EVSEStatus);
        }
        set_din_physical(&res.EVSEPresentVoltage, din_unitSymbolType_V, present_v);
        return jpv2g_cbv2g_encode_din_welding_detection_res(
            sid, din_responseCodeType_OK, &res, out, out_len, written);
    }

    return -ENOTSUP;
}

void hlc_apply_power_side_effects(HlcAppContext* ctx,
                                  jpv2g_message_type_t type,
                                  const jpv2g_secc_request_t* req) {
    if (!ctx || !req) return;

    if (type == JPV2G_SESSION_STOP_REQ) {
        Serial.printf("[HLC] {\"msg\":\"SessionStopRuntime\",\"relay1\":%d,\"presentV\":%.1f,\"measuredV\":%.1f,"
                      "\"stopRequested\":%d}\n",
                      g_relay1_closed ? 1 : 0,
                      snapshot_welding_present_voltage_v(millis()),
                      static_cast<double>(g_last_measured_v),
                      ctx->stop_requested ? 1 : 0);
        ctx->precharge_converged = false;
        ctx->power_delivery_enabled = false;
        ctx->stop_requested = false;
        clear_last_bms_request();
        if (!mode_controller_has_final_decision()) {
            (void)relay1_set(false, "SessionStop");
            request_modules_off("SessionStop");
        }
        stop_session(millis());
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
    log_hlc_request(type, req);
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
    if (type == JPV2G_POWER_DELIVERY_REQ) {
        return hlc_handle_power_delivery(ctx, req, out, out_len, written);
    }
    if (type == JPV2G_CURRENT_DEMAND_REQ) {
        return hlc_handle_current_demand(ctx, req, out, out_len, written);
    }
    if (type == JPV2G_WELDING_DETECTION_REQ) {
        return hlc_handle_welding_detection(ctx, req, out, out_len, written);
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
        const int64_t session_start_us = esp_timer_get_time();
        const int rc = jpv2g_secc_handle_client_detect(&g_secc, client_fd, HLC_FIRST_PACKET_TIMEOUT_MS, HLC_IDLE_TIMEOUT_MS);
        const int64_t session_busy_us = esp_timer_get_time() - session_start_us;
        if (session_busy_us > 0) {
            g_hlc_busy_us += static_cast<uint64_t>(session_busy_us);
        }
        if (g_hlc_active_client_fd == client_fd) {
            g_hlc_active_client_fd = -1;
        }
        jpv2g_socket_close(client_fd);

        const uint32_t done_ms = millis();
        const bool unexpected_disconnect_hold =
            !mode_controller_has_final_decision() && !g_hlc_ctx.stop_requested && cp_connected(g_cp_state) &&
            (g_hlc_ctx.precharge_seen || g_hlc_ctx.power_delivery_enabled || g_relay1_closed || g_module_output_enabled);
        if (unexpected_disconnect_hold) {
            g_hlc_disconnect_hold_until_ms = done_ms + HLC_UNEXPECTED_DISCONNECT_HOLD_MS;
            Serial.printf("[HLC] unexpected disconnect: holding power path for %lu ms"
                          " (cp=%c relay1=%d moduleOn=%d prechargeSeen=%d delivery=%d)\n",
                          static_cast<unsigned long>(HLC_UNEXPECTED_DISCONNECT_HOLD_MS),
                          g_cp_state,
                          g_relay1_closed ? 1 : 0,
                          g_module_output_enabled ? 1 : 0,
                          g_hlc_ctx.precharge_seen ? 1 : 0,
                          g_hlc_ctx.power_delivery_enabled ? 1 : 0);
        } else {
            g_hlc_ctx.precharge_converged = false;
            g_hlc_ctx.power_delivery_enabled = false;
            g_hlc_ctx.stop_requested = false;
            clear_last_bms_request();
            if (!mode_controller_has_final_decision()) {
                request_modules_off("ClientSessionDone");
                (void)relay1_set(false, "ClientSessionDone");
            }
            stop_session(done_ms);
        }

        if (g_hlc_ctx.precharge_seen) {
            Serial.printf("[HLC] precharge reached, count=%lu\n", static_cast<unsigned long>(g_hlc_ctx.precharge_count));
        }
        Serial.printf("[HLC] client session done rc=%d\n", rc);
        g_hlc_active = false;
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
        log_runtime_stats("hlc_worker_ready");
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
    if (g_hlc_active) {
        const uint32_t wait_started_ms = millis();
        while (g_hlc_active && (millis() - wait_started_ms) < 1500u) {
            delay(1);
        }
        if (g_hlc_active) {
            Serial.println("[HLC] stop requested while worker still active; deferring teardown");
            return;
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
    g_hlc_ctx.precharge_converged = false;
    g_hlc_ctx.power_delivery_enabled = false;
    g_hlc_ctx.stop_requested = false;
    controller_clear_managed_feedback_cache();
    clear_last_bms_request();
    if (!mode_controller_has_final_decision()) {
        request_modules_off("StopHlc");
        (void)relay1_set(false, "StopHlc");
    }
    stop_session(millis());
    if (!mode_controller_has_final_decision()) {
        release_controller_allowlist_if_idle("StopHlc");
    }
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
    log_runtime_stats("hlc_stack_ready");
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
    if (g_hlc_active || g_hlc_active_client_fd >= 0) {
        return;
    }

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
        mode_requires_controller_contract()
            ? (g_session_started || g_hlc_active || controller_requests_slac_pwm(now_ms))
            : (mode_is_local_autonomous() || g_session_started || g_hlc_active);
    const bool pwm_enable_delay_elapsed =
        (g_slac_pwm_enable_at_ms == 0u) || static_cast<int32_t>(now_ms - g_slac_pwm_enable_at_ms) >= 0;
    const bool slac_ready_for_pwm = !g_fsm_priming && pwm_enable_delay_elapsed;
    const bool pwm_ok = pwm_connected && !hold_active && !g_ef_pulse_active && controller_permits_pwm && slac_ready_for_pwm;
    if (pwm_ok) {
        if (g_cp_pwm_ready_since_ms == 0u) {
            g_cp_pwm_ready_since_ms = now_ms;
        }
    } else {
        g_cp_pwm_ready_since_ms = 0u;
    }
    uint16_t pct = g_ef_pulse_active ? 0u : (pwm_ok ? 5u : 100u);
    if (pct != g_last_cp_duty_pct) {
        Serial.printf("[CP] duty -> %u%% (phase=%s state=%c hold=%d ef=%d conn_hold=%d ctrl_pwm=%d)\n",
                      pct,
                      cp_phase_label(state, pct),
                      state,
                      hold_active ? 1 : 0,
                      g_ef_pulse_active ? 1 : 0,
                      connected_hold ? 1 : 0,
                      (controller_permits_pwm && slac_ready_for_pwm) ? 1 : 0);
        g_last_cp_duty_pct = pct;
    }
    write_ledc_duty(cp_pct_to_duty(g_last_cp_duty_pct));
}

void start_session(uint32_t now_ms) {
    if (!g_fsm) {
        return;
    }
    if (mode_requires_controller_contract()) {
        controller_clear_managed_feedback_cache();
    }
    if (!mode_controller_has_final_decision()) {
        controller_clear_managed_power_cache();
    }
    reset_session_measurements();
    clear_last_bms_request();
    g_last_seen_ev_mac_valid = false;
    g_fsm->start(now_ms);
    g_fsm_priming = false;
    g_slac_pwm_enable_at_ms = 0u;
    g_session_started = true;
    g_session_started_ms = now_ms;
    g_session_rx_queue_drop_baseline = g_transport ? g_transport->rx_queue_drops() : 0u;
    g_bcd_entered = false;
    g_session_matched = false;
    g_last_fsm_state = g_fsm->get_state();
    Serial.println("[SLAC] session start");
}

void reprime_slac_modem(uint32_t now_ms, const char* reason) {
    if (!g_transport || !g_fsm) {
        return;
    }

    Serial.printf("[SLAC] modem reprime (%s)\n", reason ? reason : "-");
    g_transport->modem_reset();
    if (!g_transport->wait_for_sync(QCA_STARTUP_IO_WAIT_MS, 1u)) {
        Serial.printf("[QCA] reprime sync wait failed: %s\n", g_transport->get_error().c_str());
    }
    g_fsm->invalidate_key_state();
    g_fsm->start(now_ms);
    g_fsm_priming = true;
    g_last_fsm_state = g_fsm->get_state();
}

void stop_session(uint32_t now_ms) {
    const bool should_reprime = g_fsm && g_session_started;
    if (g_fsm && g_session_started) {
        g_fsm->leave_bcd(now_ms);
    }
    if (mode_requires_controller_contract()) {
        controller_clear_managed_feedback_cache();
    }
    if (!mode_controller_has_final_decision()) {
        controller_clear_managed_power_cache();
    }
    g_session_started = false;
    g_session_started_ms = 0;
    g_session_rx_queue_drop_baseline = 0u;
    g_slac_pwm_enable_at_ms = 0u;
    g_bcd_entered = false;
    g_session_matched = false;
    g_hlc_disconnect_hold_until_ms = 0;
    g_last_fsm_state = slac::evse::State::Reset;
    g_last_ev_soc_pct = -1;
    g_last_seen_ev_mac_valid = false;
    clear_last_bms_request();
    reset_session_measurements();
    if (should_reprime) {
        reprime_slac_modem(now_ms, "session stop");
    }
}

bool schedule_soft_slac_retry(uint32_t now_ms, const char* reason) {
    if (!g_session_started || !g_fsm) {
        return false;
    }
    if (g_slac_soft_retries_this_cp >= SLAC_SOFT_RETRY_LIMIT) {
        return false;
    }
    const uint32_t elapsed_ms = (g_session_started_ms <= now_ms) ? (now_ms - g_session_started_ms) : 0u;
    if (elapsed_ms > SLAC_SOFT_RETRY_ELAPSED_MAX_MS) {
        return false;
    }

    g_slac_soft_retries_this_cp++;
    g_slac_hold_until_ms = now_ms + SLAC_SOFT_RETRY_HOLD_MS;
    const std::string last_error = g_fsm->get_last_error();
    stop_session(now_ms);
    Serial.printf("[SLAC] soft retry %u/%u in %s (%s) %lu ms err=%s\n",
                  static_cast<unsigned>(g_slac_soft_retries_this_cp),
                  static_cast<unsigned>(SLAC_SOFT_RETRY_LIMIT),
                  runtime_mode_name(),
                  reason ? reason : "-",
                  static_cast<unsigned long>(SLAC_SOFT_RETRY_HOLD_MS),
                  last_error.empty() ? "-" : last_error.c_str());
    return true;
}

void enter_hold_with_optional_ef_pulse(uint32_t now_ms, const char* reason) {
    g_slac_soft_retries_this_cp = 0;
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
    Serial.printf("[SLAC] enter hold in %s (%s) %lu ms\n",
                  runtime_mode_name(),
                  reason ? reason : "-",
                  static_cast<unsigned long>(SLAC_HOLD_MS));
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
    if (!g_transport->wait_for_sync(QCA_STARTUP_IO_WAIT_MS, 1u)) {
        Serial.printf("[QCA] init sync wait failed: %s\n", g_transport->get_error().c_str());
        g_next_qca_init_ms = now_ms + QCA_INIT_RETRY_MS;
        return false;
    }

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
    g_fsm->set_control_mac(g_wifi_sta_mac);
    ensure_runtime_slac_nmk();
    g_fsm->set_nmk(g_runtime_slac_nmk.data());

    const uint8_t plc_peer_mac[ETH_ALEN] = {
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[0] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[0] >> 0) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[1] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[1] >> 0) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[2] >> 8) & 0xFF),
        static_cast<uint8_t>((PLC_PEER_MAC_DEFAULT[2] >> 0) & 0xFF),
    };
    g_fsm->set_plc_peer_mac(plc_peer_mac);
    g_fsm->start(now_ms);
    g_fsm_priming = true;
    g_last_fsm_state = g_fsm->get_state();
    Serial.println("[SLAC] priming modem key setup");
    g_next_qca_init_ms = 0;
    Serial.println("[APP] ready, waiting for CP B/C/D");
    log_runtime_stats("qca_ready");
    return true;
}

void service_hlc_disconnect_hold(uint32_t now_ms) {
    if (g_hlc_disconnect_hold_until_ms == 0u) {
        return;
    }
    if (g_hlc_active) {
        g_hlc_disconnect_hold_until_ms = 0u;
        return;
    }
    if (!cp_connected(g_cp_state)) {
        g_hlc_disconnect_hold_until_ms = 0u;
        return;
    }
    if (static_cast<int32_t>(now_ms - g_hlc_disconnect_hold_until_ms) < 0) {
        return;
    }

    Serial.printf("[HLC] unexpected disconnect hold expired after %lu ms, stopping power path\n",
                  static_cast<unsigned long>(HLC_UNEXPECTED_DISCONNECT_HOLD_MS));
    g_hlc_disconnect_hold_until_ms = 0u;
    g_hlc_ctx.precharge_converged = false;
    g_hlc_ctx.power_delivery_enabled = false;
    g_hlc_ctx.stop_requested = false;
    clear_last_bms_request();
    if (!mode_controller_has_final_decision()) {
        request_modules_off("HlcDisconnectTimeout");
        (void)relay1_set(false, "HlcDisconnectTimeout");
    }
    stop_session(now_ms);
}

void process_cp_and_fsm(uint32_t now_ms) {
    note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, 0x0001u);
    const int cp_mv = read_cp_mv_robust();
    note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, 0x0002u);
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
            g_cp_pwm_ready_since_ms = 0u;
            g_slac_failures_this_cp = 0;
            g_slac_soft_retries_this_cp = 0;
            g_slac_hold_until_ms = 0;
        } else if (!connected && old_connected) {
            g_cp_connected_since_ms = 0;
            g_cp_last_seen_connected_ms = 0;
            g_cp_pwm_ready_since_ms = 0u;
            g_slac_hold_until_ms = 0;
            g_slac_failures_this_cp = 0;
            g_slac_soft_retries_this_cp = 0;
            // In controller mode, preserve a latched "start digital communication"
            // request across PLC-driven retry pulses (typically F state), but
            // clear it on a real unplug to A so the next plug-in requires a new
            // controller start command.
            if (mode_requires_controller_contract() && raw_cp_state == 'A') {
                controller_clear_slac_contract();
                g_ctrl.auth_state = ControllerAuthState::Denied;
                g_ctrl.auth_expiry_ms = 0u;
                controller_clear_managed_feedback_cache();
            }
            stop_session(now_ms);
            stop_hlc_stack();
            if (!mode_controller_has_final_decision()) {
                request_modules_off("CPDisconnect");
                (void)relay1_set(false, "CPDisconnect");
            }
        }
        g_last_cp_state = g_cp_state;
    }

    if (g_fsm && g_fsm_priming) {
        for (int i = 0; i < SLAC_ACTIVE_MAX_FRAMES_PER_LOOP; ++i) {
            const int timeout_ms = (i == 0) ? 2 : 0;
            note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, static_cast<uint16_t>(0x0100u | (i + 1)));
            const bool got = g_fsm->poll_channel_once(timeout_ms, now_ms);
            if (!got) break;
            if (((i + 1) % FSM_POLL_BREATHER_STRIDE) == 0) {
                cooperative_runtime_breather(CrashBreadcrumbStage::FsmPollActive,
                                             static_cast<uint16_t>(0x0180u | (i + 1)));
            }
        }
        note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, 0x0100u);
        g_fsm->poll(now_ms);
        const slac::evse::State priming_state = g_fsm->get_state();
        if (priming_state != g_last_fsm_state) {
            Serial.printf("[FSM] %s -> %s\n", evse_state_name(g_last_fsm_state), evse_state_name(priming_state));
            g_last_fsm_state = priming_state;
        }
        if (priming_state == slac::evse::State::Idle) {
            g_fsm_priming = false;
            Serial.println("[SLAC] modem key primed");
        }
    }

    apply_cp_output(g_cp_state, now_ms);
    note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, 0x0003u);

    if (!connected) {
        return;
    }

    const bool slac_start_ok = controller_allows_slac_start(now_ms);
    const bool pwm_ready = g_cp_pwm_ready_since_ms != 0u &&
                           static_cast<int32_t>(now_ms - g_cp_pwm_ready_since_ms) >=
                               static_cast<int32_t>(CP_STABLE_MS);
    if (!g_session_started && !g_fsm_priming && g_cp_connected_since_ms != 0 &&
        static_cast<int32_t>(now_ms - g_slac_hold_until_ms) >= 0 &&
        slac_start_ok &&
        pwm_ready) {
        start_session(now_ms);
        if (mode_requires_controller_contract() && !mode_controller_has_final_decision()) {
            g_ctrl.slac_start_latched = false;
        }
    } else if (mode_requires_controller_contract() && !g_session_started &&
               g_cp_connected_since_ms != 0 && !slac_start_ok) {
        static uint32_t next_wait_log_ms = 0;
        if (next_wait_log_ms == 0 || static_cast<int32_t>(now_ms - next_wait_log_ms) >= 0) {
            Serial.printf("[CTRL] waiting SLAC start auth (hb=%d armed=%d start=%d)\n",
                          controller_heartbeat_alive(now_ms) ? 1 : 0,
                          g_ctrl.slac_armed ? 1 : 0,
                          g_ctrl.slac_start_latched ? 1 : 0);
            next_wait_log_ms = now_ms + 2000;
        }
    } else if (mode_requires_controller_contract() && !g_session_started &&
               g_cp_connected_since_ms != 0 && slac_start_ok && !pwm_ready) {
        static uint32_t next_pwm_wait_log_ms = 0;
        if (next_pwm_wait_log_ms == 0 || static_cast<int32_t>(now_ms - next_pwm_wait_log_ms) >= 0) {
            Serial.printf("[CTRL] waiting SLAC start pwm cp=%s duty=%u pwm_ready=%d\n",
                          cp_phase_label(g_cp_state, g_last_cp_duty_pct),
                          static_cast<unsigned>(g_last_cp_duty_pct),
                          pwm_ready ? 1 : 0);
            next_pwm_wait_log_ms = now_ms + 2000;
        }
    }

    if (!g_session_started || !g_fsm) {
        return;
    }

    for (int i = 0; i < SLAC_ACTIVE_MAX_FRAMES_PER_LOOP; ++i) {
        const int timeout_ms = (i == 0) ? 2 : 0;
        note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, static_cast<uint16_t>(0x1000u | (i + 1)));
        const bool got = g_fsm->poll_channel_once(timeout_ms, now_ms);
        if (!got) break;
        if (((i + 1) % FSM_POLL_BREATHER_STRIDE) == 0) {
            cooperative_runtime_breather(CrashBreadcrumbStage::FsmPollActive,
                                         static_cast<uint16_t>(0x1080u | (i + 1)));
        }
    }
    note_crash_breadcrumb(CrashBreadcrumbStage::FsmPollActive, 0x0200u);
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

    const bool matching_in_progress =
        s == slac::evse::State::Idle || s == slac::evse::State::WaitForMatchingStart ||
        s == slac::evse::State::Matching || s == slac::evse::State::Sounding ||
        s == slac::evse::State::FinalizeSounding ||
        s == slac::evse::State::DoAttenChar || s == slac::evse::State::WaitForSlacMatch;
    if (!g_session_matched && matching_in_progress && g_transport) {
        const uint32_t rx_drops = g_transport->rx_queue_drops();
        if (rx_drops != g_session_rx_queue_drop_baseline) {
            const uint32_t delta = rx_drops - g_session_rx_queue_drop_baseline;
            g_session_rx_queue_drop_baseline = rx_drops;
            Serial.printf("[SLAC] rx queue drop during matching state=%s delta=%lu total=%lu\n",
                          evse_state_name(s),
                          static_cast<unsigned long>(delta),
                          static_cast<unsigned long>(rx_drops));
            if (schedule_soft_slac_retry(now_ms, "rx queue drop")) {
                return;
            }
            enter_hold_with_optional_ef_pulse(now_ms, "rx queue drop");
            stop_hlc_stack();
            if (!mode_controller_has_final_decision()) {
                request_modules_off("SlacRxQueueDrop");
                (void)relay1_set(false, "SlacRxQueueDrop");
            }
            return;
        }
    }
    const uint32_t progress_timeout_ms =
        (s == slac::evse::State::WaitForMatchingStart) ? SLAC_INIT_PROGRESS_TIMEOUT_MS : SLAC_PROGRESS_TIMEOUT_MS;
    if (!g_session_matched && g_session_started_ms != 0 && matching_in_progress &&
        static_cast<int32_t>(now_ms - (g_session_started_ms + progress_timeout_ms)) > 0) {
        Serial.printf("[SLAC] progress timeout state=%s elapsed_ms=%lu\n",
                      evse_state_name(s),
                      static_cast<unsigned long>(now_ms - g_session_started_ms));
        enter_hold_with_optional_ef_pulse(now_ms, "matching timeout");
        stop_hlc_stack();
        if (!mode_controller_has_final_decision()) {
            request_modules_off("SlacProgressTimeout");
            (void)relay1_set(false, "SlacProgressTimeout");
        }
        return;
    }

    if (!g_bcd_entered && s == slac::evse::State::Idle) {
        g_fsm->enter_bcd(now_ms);
        g_bcd_entered = true;
        Serial.println("[FSM] EnterBCD");
    }

    if (s == slac::evse::State::Matched && !g_session_matched) {
        g_session_matched = true;
        g_slac_soft_retries_this_cp = 0;
        uint8_t ev_mac[ETH_ALEN]{};
        if (g_fsm->get_ev_mac(ev_mac)) {
            Serial.printf("[SLAC] EV_MAC %02X:%02X:%02X:%02X:%02X:%02X\n",
                          ev_mac[0], ev_mac[1], ev_mac[2], ev_mac[3], ev_mac[4], ev_mac[5]);
            controller_publish_ev_mac_if_changed(ev_mac);
        }
        Serial.println("[SLAC] MATCHED - starting HLC stack");
    } else if (s == slac::evse::State::SignalError) {
        enter_hold_with_optional_ef_pulse(now_ms, "slac init timeout");
        stop_hlc_stack();
        if (!mode_controller_has_final_decision()) {
            request_modules_off("SlacInitTimeout");
            (void)relay1_set(false, "SlacInitTimeout");
        }
        return;
    } else if (s == slac::evse::State::MatchingFailed) {
        if (schedule_soft_slac_retry(now_ms, "fsm early matching failure")) {
            return;
        }
        enter_hold_with_optional_ef_pulse(now_ms, "fsm terminal failure");
        stop_hlc_stack();
        if (!mode_controller_has_final_decision()) {
            request_modules_off("SlacFailure");
            (void)relay1_set(false, "SlacFailure");
        }
    } else if (s == slac::evse::State::NoSlacPerformed) {
        enter_hold_with_optional_ef_pulse(now_ms, "fsm terminal failure");
        stop_hlc_stack();
        if (!mode_controller_has_final_decision()) {
            request_modules_off("SlacFailure");
            (void)relay1_set(false, "SlacFailure");
        }
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
    if (g_crash_breadcrumb.magic != CRASH_BREADCRUMB_MAGIC) {
        memset(&g_crash_breadcrumb, 0, sizeof(g_crash_breadcrumb));
        g_crash_breadcrumb.magic = CRASH_BREADCRUMB_MAGIC;
    }
    g_crash_breadcrumb.boot_count++;
    Serial.printf("[BOOT] reset_reason=%d retained_stage=%s detail=%u last_ms=%lu loop_count=%lu boot_count=%lu\n",
                  static_cast<int>(esp_reset_reason()),
                  crash_breadcrumb_stage_name(static_cast<CrashBreadcrumbStage>(g_crash_breadcrumb.stage)),
                  static_cast<unsigned>(g_crash_breadcrumb.detail),
                  static_cast<unsigned long>(g_crash_breadcrumb.last_ms),
                  static_cast<unsigned long>(g_crash_breadcrumb.loop_count),
                  static_cast<unsigned long>(g_crash_breadcrumb.boot_count));
    note_crash_breadcrumb(CrashBreadcrumbStage::SetupStart);
    g_loop_task = xTaskGetCurrentTaskHandle();
    enable_psram_malloc_policy();
    g_serial_cmd_buf.reserve(256u);
    log_runtime_stats("boot");

    Serial.println();
    Serial.println("cbslac + jpv2g ESP32-S3 SLAC/HLC (to PreCharge)");

    (void)load_runtime_config_from_nvs();
    refresh_runtime_identity_cache();
    print_runtime_config();
    log_runtime_stats("config_loaded");
    init_status_leds();
    service_status_leds(millis());

    esp_read_mac(g_wifi_sta_mac, ESP_MAC_WIFI_STA);
    memcpy(g_local_mac, g_wifi_sta_mac, sizeof(g_local_mac));
    Serial.printf("[NET] WiFi STA MAC %02X:%02X:%02X:%02X:%02X:%02X\n",
                  g_wifi_sta_mac[0], g_wifi_sta_mac[1], g_wifi_sta_mac[2],
                  g_wifi_sta_mac[3], g_wifi_sta_mac[4], g_wifi_sta_mac[5]);
    Serial.printf("[NET] PLC MAC placeholder %02X:%02X:%02X:%02X:%02X:%02X\n",
                  g_local_mac[0], g_local_mac[1], g_local_mac[2], g_local_mac[3], g_local_mac[4], g_local_mac[5]);

    (void)relay1_init();
    (void)relay1_set(false, "Boot");
    (void)relay2_set(false, "Boot");
    (void)relay3_set(false, "Boot");
    g_emergency_stop_active = read_emergency_pressed();
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
    note_crash_breadcrumb(CrashBreadcrumbStage::SetupAfterStartupHold);

    refresh_plc_link_local_identity();

    analogReadResolution(12);
    analogSetPinAttenuation(CP_1_READ_PIN, ADC_11db);
    ledcSetup(CP_1_PWM_CHANNEL, CP_1_PWM_FREQUENCY, CP_1_PWM_RESOLUTION);
    ledcAttachPin(CP_1_PWM_PIN, CP_1_PWM_CHANNEL);
    g_last_cp_duty_pct = 100;
    write_ledc_duty(cp_pct_to_duty(g_last_cp_duty_pct));

    if (mode_uses_plc_module_manager()) {
        g_module_allowlist = {runtime_local_module_id()};
        (void)init_module_manager();
        if (mode_requires_controller_contract()) {
            g_module_allowlist.clear();
            Serial.printf("[CTRL] mode=%s active: starting with empty allowlist\n", runtime_mode_name());
        }
    } else {
        g_module_allowlist.clear();
        g_assignment_power_limit_kw = 0.0f;
        g_active_module_power_limits_kw.clear();
        g_module_ready = false;
        g_module_output_enabled = false;
        Serial.printf("[MOD] bypassed in mode=%s: PLC module CAN/control stack disabled\n", runtime_mode_name());
    }
    request_modules_off("Boot");
    controller_publish_local_identity_once();
    if (mode_requires_controller_contract()) {
        rfid_init_device(millis(), "boot");
    }

    (void)try_init_qca_and_fsm(millis());
    note_crash_breadcrumb(CrashBreadcrumbStage::SetupAfterQcaInit);
}

void loop() {
    uint32_t now_ms = millis();
    const int64_t loop_start_us = esp_timer_get_time();
    g_crash_breadcrumb.loop_count++;
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopEnter);
    service_serial_commands();
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterSerial);
    service_emergency_stop(now_ms);
    if (!g_fsm && static_cast<int32_t>(now_ms - g_next_qca_init_ms) >= 0) {
        (void)try_init_qca_and_fsm(now_ms);
    }
    if (g_transport && g_transport->is_ready()) {
        g_transport->service_ingress_once();
    }
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterQcaIngress);
    controller_service_rx();
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterCtrlRx);
    // Re-sample time after controller RX so same-iteration heartbeats do not
    // look "from the future" to the watchdog/session gate logic.
    now_ms = millis();
    controller_service_watchdogs(now_ms);
    controller_service_periodic_tx(now_ms);
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterCtrlTx);
    process_cp_and_fsm(now_ms);
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterCpFsm);
    service_hlc_disconnect_hold(now_ms);
    if (mode_uses_plc_module_manager()) {
        service_module_manager_once(now_ms);
    }
    if (mode_requires_controller_contract()) {
        rfid_poll(now_ms);
    }
    service_lwip_tx_queue_once();
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopAfterLwipTx);
    service_status_leds(now_ms);
    const int64_t loop_busy_us = esp_timer_get_time() - loop_start_us;
    if (loop_busy_us > 0) {
        g_loop_busy_us += static_cast<uint64_t>(loop_busy_us);
    }
    service_runtime_stats(now_ms);
    const bool hlc_priority = g_session_started || g_hlc_active || cp_connected(g_cp_state);
    feed_loop_watchdog();
    note_crash_breadcrumb(CrashBreadcrumbStage::LoopSleep, hlc_priority ? 1u : 5u);
    delay(hlc_priority ? 1 : 5);
}
