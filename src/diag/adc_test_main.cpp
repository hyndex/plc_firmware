#include <Arduino.h>

#include <algorithm>
#include <array>
#include <cstdint>

extern "C" {
#include "driver/adc.h"
#include "esp_adc_cal.h"
#include "hal/adc_ll.h"
#include "soc/sens_struct.h"
}

namespace {

#ifndef ADC_TEST_PLC_ID
#define ADC_TEST_PLC_ID 0
#endif

#ifndef ADC_TEST_CONNECTOR_ID
#define ADC_TEST_CONNECTOR_ID 0
#endif

constexpr uint32_t kSerialBaud = 115200;
constexpr uint32_t kBannerDelayMs = 1500;
constexpr uint32_t kPrintPeriodMs = 2000;

constexpr int kCpPwmPin = 38;
constexpr int kCpPwmChannel = 0;
constexpr int kCpPwmFrequency = 1000;
constexpr int kCpPwmResolution = 12;
constexpr uint32_t kCpMaxDuty = (1u << kCpPwmResolution) - 1u;
constexpr int kCpReadPin = 1;
constexpr adc1_channel_t kCpAdcChannel = ADC1_CHANNEL_0;

constexpr int kCpT12Mv = 2300;
constexpr int kCpT9Mv = 2000;
constexpr int kCpT6Mv = 1700;
constexpr int kCpT3Mv = 1450;
constexpr int kCpT0Mv = 1250;
constexpr int kCpNegThresholdMv = 500;
constexpr int kCpHysteresisMv = 100;

constexpr int kHlwSampleCount = 256;
constexpr int kHlwSampleDelayUs = 6;
constexpr int kHlwTopK = 48;
constexpr int kHlwEnterBandMv = 20;
constexpr int kHlwDecayMvPerScan = 25;

constexpr int kPlcSamplesPerPoll = 4;
constexpr int kPlcSampleDelayUs = 6;
constexpr uint16_t kPlcPhaseStepUs = 197;
constexpr int kPlcTopK = 12;
constexpr int kPlcDecayMvPerScan = 25;
constexpr uint32_t kPlcAdcMaxWaitSpins = 2000;
constexpr uint32_t kPlcStateDebounceMs = 80;

constexpr int kT12Enter = kCpT12Mv + kHlwEnterBandMv;
constexpr int kT12Exit = kCpT12Mv - kHlwEnterBandMv;
constexpr int kT9Enter = kCpT9Mv + kHlwEnterBandMv;
constexpr int kT9Exit = kCpT9Mv - kHlwEnterBandMv;
constexpr int kT6Enter = kCpT6Mv + kHlwEnterBandMv;
constexpr int kT6Exit = kCpT6Mv - kHlwEnterBandMv;
constexpr int kT3Enter = kCpT3Mv + kHlwEnterBandMv;
constexpr int kT3Exit = kCpT3Mv - kHlwEnterBandMv;
constexpr int kT0Enter = kCpT0Mv + kHlwEnterBandMv;
constexpr int kT0Exit = kCpT0Mv - kHlwEnterBandMv;

struct OneShotReading {
    int analog_raw{0};
    int analog_mv{0};
    int rtc_raw{0};
    int rtc_mv{0};
    bool rtc_ok{false};
};

struct HlwReading {
    int min_mv{0};
    int plateau_mv{0};
    int avg_mv{0};
    int peak_mv{0};
    int robust_mv{0};
    char tentative_state{'A'};
    char stable_state{'A'};
    uint8_t below_b_run{0};
    uint32_t phase_us{0};
};

struct PlcReading {
    int robust_mv{0};
    int plateau_mv{0};
    int peak_mv{0};
    int last_valid_mv{0};
    int last_raw_count{0};
    int valid_samples{0};
    char raw_state{'A'};
    char stable_state{'A'};
    char candidate_state{'A'};
    uint32_t candidate_age_ms{0};
    uint32_t phase_us{0};
};

esp_adc_cal_characteristics_t g_cp_adc_chars{};
bool g_cp_adc_chars_ready = false;
portMUX_TYPE g_cp_adc_mux = portMUX_INITIALIZER_UNLOCKED;

uint32_t g_next_print_ms = 0;
String g_serial_line;

uint16_t g_plc_sample_phase_us = 0;
int g_plc_last_valid_mv = 0;
int g_plc_last_peak_mv = 0;
int g_plc_last_plateau_mv = 0;
int g_plc_robust_mv = 0;
char g_plc_state = 'A';
char g_plc_candidate_state = 'A';
uint32_t g_plc_candidate_since_ms = 0;

uint32_t g_hlw_sample_phase_us = 0;
int g_hlw_robust_mv = 0;
char g_hlw_state = 'A';
uint8_t g_hlw_below_b_run = 0;

bool time_reached(uint32_t now_ms, uint32_t target_ms) {
    return static_cast<int32_t>(now_ms - target_ms) >= 0;
}

char cp_state_from_mv(int mv) {
    if (mv <= kCpNegThresholdMv) return 'F';
    if (mv >= (kCpT12Mv + kCpHysteresisMv)) return 'A';
    if (mv >= (kCpT9Mv + kCpHysteresisMv)) return 'B';
    if (mv >= (kCpT6Mv + kCpHysteresisMv)) return 'C';
    if (mv >= (kCpT3Mv + kCpHysteresisMv)) return 'D';
    if (mv >= (kCpT0Mv + kCpHysteresisMv)) return 'E';
    return 'F';
}

char classify_with_hys_hlw(int mv, char prev) {
    switch (prev) {
        case 'A':
            return (mv <= kT12Exit) ? 'B' : 'A';
        case 'B':
            if (mv >= kT12Enter) return 'A';
            return (mv <= kT9Exit) ? 'C' : 'B';
        case 'C':
            if (mv >= kT9Enter) return 'B';
            return (mv <= kT6Exit) ? 'D' : 'C';
        case 'D':
            if (mv >= kT6Enter) return 'C';
            return (mv <= kT3Exit) ? 'E' : 'D';
        case 'E':
            if (mv >= kT3Enter) return 'D';
            return (mv <= kT0Exit) ? 'F' : 'E';
        default:
            return (mv >= kT0Enter) ? 'E' : 'F';
    }
}

bool read_cp_mv_once_plc(int* out_raw_count, int* out_mv) {
    if (!out_raw_count || !out_mv) {
        return false;
    }

    int raw = 0;
    bool ok = false;

    portENTER_CRITICAL(&g_cp_adc_mux);
    adc_ll_set_controller(ADC_NUM_1, ADC_LL_CTRL_RTC);
    adc_ll_rtc_enable_channel(ADC_NUM_1, kCpAdcChannel);
    adc_ll_rtc_reset();
    SENS.sar_meas1_ctrl2.meas1_start_sar = 0;
    SENS.sar_meas1_ctrl2.meas1_start_sar = 1;
    for (uint32_t spins = 0; spins < kPlcAdcMaxWaitSpins; ++spins) {
        if (adc_ll_rtc_convert_is_done(ADC_NUM_1)) {
            raw = adc_ll_rtc_get_convert_value(ADC_NUM_1);
            if (!adc_ll_rtc_analysis_raw_data(ADC_NUM_1, static_cast<uint16_t>(raw))) {
                ok = true;
            }
            break;
        }
    }
    adc_ll_rtc_disable_channel(ADC_NUM_1);
    adc_ll_rtc_reset();
    portEXIT_CRITICAL(&g_cp_adc_mux);

    if (!ok) {
        return false;
    }

    *out_raw_count = raw;
    *out_mv = g_cp_adc_chars_ready ? static_cast<int>(esp_adc_cal_raw_to_voltage(static_cast<uint32_t>(raw), &g_cp_adc_chars))
                                   : raw;
    return true;
}

OneShotReading capture_one_shot() {
    OneShotReading reading;
    reading.analog_raw = analogRead(kCpReadPin);
    reading.analog_mv = analogReadMilliVolts(kCpReadPin);
    reading.rtc_ok = read_cp_mv_once_plc(&reading.rtc_raw, &reading.rtc_mv);
    return reading;
}

void sample_cp_burst_hlw(int& min_mv, int& plateau_mv, int& avg_mv, int& peak_mv) {
    int minv = INT32_MAX;
    int max_seen = INT32_MIN;
    int64_t acc = 0;
    int topk[kHlwTopK];
    int tk = 0;

    auto insert_topk = [&](int v) {
        if (tk < kHlwTopK) {
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

    if (g_hlw_sample_phase_us) {
        delayMicroseconds(g_hlw_sample_phase_us);
    }
    (void)analogRead(kCpReadPin);
    for (int i = 0; i < kHlwSampleCount; ++i) {
        delayMicroseconds(kHlwSampleDelayUs);
        const int v = analogReadMilliVolts(kCpReadPin);
        acc += v;
        if (v < minv) minv = v;
        if (v > max_seen) max_seen = v;
        insert_topk(v);
    }

    int robust = 0;
    if (tk == 0) {
        robust = (max_seen == INT32_MIN) ? 0 : max_seen;
    } else {
        int start = tk - std::max(3, tk / 6);
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
        robust = (n > 0) ? static_cast<int>(sum / n) : topk[tk - 1];
    }

    min_mv = (minv == INT32_MAX) ? 0 : minv;
    plateau_mv = robust;
    avg_mv = static_cast<int>(acc / static_cast<int64_t>(kHlwSampleCount));
    peak_mv = (max_seen == INT32_MIN) ? 0 : max_seen;
    g_hlw_sample_phase_us = (g_hlw_sample_phase_us + 53u) % 1000u;
}

HlwReading capture_hlw_reference() {
    HlwReading reading;
    sample_cp_burst_hlw(reading.min_mv, reading.plateau_mv, reading.avg_mv, reading.peak_mv);
    g_hlw_robust_mv = std::max(reading.plateau_mv, g_hlw_robust_mv - kHlwDecayMvPerScan);
    reading.robust_mv = g_hlw_robust_mv;
    reading.phase_us = g_hlw_sample_phase_us;
    reading.tentative_state = classify_with_hys_hlw(reading.robust_mv, g_hlw_state);

    const bool burst_has_b = reading.peak_mv >= kT9Enter;
    g_hlw_below_b_run = burst_has_b ? 0u : static_cast<uint8_t>(std::min<int>(g_hlw_below_b_run + 1u, 100));

    char new_state = reading.tentative_state;
    if (g_hlw_state == 'B' &&
        (reading.tentative_state == 'C' || reading.tentative_state == 'D' || reading.tentative_state == 'E' ||
         reading.tentative_state == 'F')) {
        if (g_hlw_below_b_run < 8u) {
            new_state = 'B';
        }
    }
    g_hlw_state = new_state;
    reading.stable_state = g_hlw_state;
    reading.below_b_run = g_hlw_below_b_run;
    return reading;
}

PlcReading capture_plc_current() {
    PlcReading reading;
    int topk[kPlcTopK];
    int tk = 0;
    int peak = 0;
    int last_valid_mv = g_plc_last_valid_mv;
    int last_raw_count = 0;
    int valid_samples = 0;

    auto insert_topk = [&](int v) {
        if (tk < kPlcTopK) {
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

    if (g_plc_sample_phase_us) {
        delayMicroseconds(g_plc_sample_phase_us);
    }
    for (int i = 0; i < kPlcSamplesPerPoll; ++i) {
        if (i > 0) {
            delayMicroseconds(kPlcSampleDelayUs);
        }
        int raw_count = 0;
        int mv = 0;
        if (!read_cp_mv_once_plc(&raw_count, &mv)) {
            continue;
        }
        ++valid_samples;
        last_valid_mv = mv;
        last_raw_count = raw_count;
        if (mv > peak) {
            peak = mv;
        }
        insert_topk(mv);
        if ((i & 1) == 1) {
            taskYIELD();
        }
    }

    g_plc_sample_phase_us = static_cast<uint16_t>((g_plc_sample_phase_us + kPlcPhaseStepUs) % 1000u);

    int plateau_mv = 0;
    int robust_mv = 0;
    if (valid_samples <= 0) {
        robust_mv = (g_plc_robust_mv > 0) ? g_plc_robust_mv : ((last_valid_mv > 0) ? last_valid_mv : 0);
    } else {
        g_plc_last_valid_mv = last_valid_mv;
        if (tk <= 0) {
            plateau_mv = peak;
        } else {
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
            plateau_mv = (n > 0) ? static_cast<int>(sum / n) : topk[tk - 1];
        }
        g_plc_last_peak_mv = peak;
        g_plc_last_plateau_mv = plateau_mv;
        if (g_plc_robust_mv <= 0) {
            g_plc_robust_mv = plateau_mv;
        } else {
            g_plc_robust_mv = std::max(plateau_mv, g_plc_robust_mv - kPlcDecayMvPerScan);
        }
        robust_mv = g_plc_robust_mv;
    }

    const uint32_t now_ms = millis();
    const char raw_state = classify_with_hys_hlw(robust_mv, g_plc_state);
    if (raw_state != g_plc_candidate_state) {
        g_plc_candidate_state = raw_state;
        g_plc_candidate_since_ms = now_ms;
    }
    if (g_plc_candidate_state != g_plc_state &&
        static_cast<int32_t>(now_ms - g_plc_candidate_since_ms) >= static_cast<int32_t>(kPlcStateDebounceMs)) {
        g_plc_state = g_plc_candidate_state;
    }

    reading.robust_mv = robust_mv;
    reading.plateau_mv = (plateau_mv > 0) ? plateau_mv : g_plc_last_plateau_mv;
    reading.peak_mv = (peak > 0) ? peak : g_plc_last_peak_mv;
    reading.last_valid_mv = last_valid_mv;
    reading.last_raw_count = last_raw_count;
    reading.valid_samples = valid_samples;
    reading.raw_state = raw_state;
    reading.stable_state = g_plc_state;
    reading.candidate_state = g_plc_candidate_state;
    reading.candidate_age_ms = now_ms - g_plc_candidate_since_ms;
    reading.phase_us = g_plc_sample_phase_us;
    return reading;
}

void print_help() {
    Serial.println("[ADC-TEST] commands:");
    Serial.println("[ADC-TEST]   help");
    Serial.println("[ADC-TEST]   status");
}

void print_banner() {
    Serial.println();
    Serial.printf("[ADC-TEST] plc_id=%d connector_id=%d\n", ADC_TEST_PLC_ID, ADC_TEST_CONNECTOR_ID);
    Serial.printf("[ADC-TEST] CP read pin=%d pwm pin=%d freq=%dHz duty=100%%\n",
                  kCpReadPin,
                  kCpPwmPin,
                  kCpPwmFrequency);
    Serial.printf("[ADC-TEST] thresholds mv: A=%d B=%d C=%d D=%d E=%d F<=%d\n",
                  kCpT12Mv,
                  kCpT9Mv,
                  kCpT6Mv,
                  kCpT3Mv,
                  kCpT0Mv,
                  kCpNegThresholdMv);
    Serial.println("[ADC-TEST] printing both paths:");
    Serial.println("[ADC-TEST]   ref/type-2-HLW burst sampler");
    Serial.println("[ADC-TEST]   plc_firmware current low-level RTC sampler");
    print_help();
}

void print_status() {
    const OneShotReading one_shot = capture_one_shot();
    const HlwReading hlw = capture_hlw_reference();
    const PlcReading plc = capture_plc_current();

    Serial.printf("[ADC-TEST][RAW] analog_raw=%d analog_mv=%d rtc_ok=%d rtc_raw=%d rtc_mv=%d\n",
                  one_shot.analog_raw,
                  one_shot.analog_mv,
                  one_shot.rtc_ok ? 1 : 0,
                  one_shot.rtc_raw,
                  one_shot.rtc_mv);

    Serial.printf(
        "[ADC-TEST][HLW] min=%d plateau=%d avg=%d peak=%d robust=%d tentative=%c stable=%c belowB=%u phase_us=%lu\n",
        hlw.min_mv,
        hlw.plateau_mv,
        hlw.avg_mv,
        hlw.peak_mv,
        hlw.robust_mv,
        hlw.tentative_state,
        hlw.stable_state,
        static_cast<unsigned>(hlw.below_b_run),
        static_cast<unsigned long>(hlw.phase_us));

    Serial.printf(
        "[ADC-TEST][PLC] robust=%d plateau=%d peak=%d last_valid=%d last_raw=%d valid=%d raw_state=%c candidate=%c "
        "stable=%c candidate_age_ms=%lu phase_us=%lu\n",
        plc.robust_mv,
        plc.plateau_mv,
        plc.peak_mv,
        plc.last_valid_mv,
        plc.last_raw_count,
        plc.valid_samples,
        plc.raw_state,
        plc.candidate_state,
        plc.stable_state,
        static_cast<unsigned long>(plc.candidate_age_ms),
        static_cast<unsigned long>(plc.phase_us));
}

void service_serial() {
    while (Serial.available() > 0) {
        const int ch = Serial.read();
        if (ch < 0) {
            continue;
        }
        if (ch == '\r') {
            continue;
        }
        if (ch == '\n') {
            const String cmd = g_serial_line;
            g_serial_line = "";
            if (cmd.equalsIgnoreCase("help")) {
                print_help();
            } else if (cmd.equalsIgnoreCase("status")) {
                print_status();
            } else if (cmd.length() > 0) {
                Serial.printf("[ADC-TEST] unknown command: %s\n", cmd.c_str());
            }
            continue;
        }
        if (g_serial_line.length() < 96) {
            g_serial_line += static_cast<char>(ch);
        }
    }
}

void setup_cp_io() {
    analogReadResolution(12);
    pinMode(kCpReadPin, INPUT);
    adcAttachPin(kCpReadPin);
    analogSetPinAttenuation(kCpReadPin, ADC_11db);
    adc_power_acquire();
    (void)adc1_config_channel_atten(kCpAdcChannel, ADC_ATTEN_DB_12);
    (void)esp_adc_cal_characterize(ADC_UNIT_1, ADC_ATTEN_DB_12, ADC_WIDTH_BIT_12, 1100, &g_cp_adc_chars);
    g_cp_adc_chars_ready = true;

    ledcSetup(kCpPwmChannel, kCpPwmFrequency, kCpPwmResolution);
    ledcAttachPin(kCpPwmPin, kCpPwmChannel);
    ledcWrite(kCpPwmChannel, kCpMaxDuty);
}

}  // namespace

void setup() {
    Serial.begin(kSerialBaud);
    delay(kBannerDelayMs);
    print_banner();
    setup_cp_io();
    g_next_print_ms = millis();
}

void loop() {
    service_serial();

    const uint32_t now_ms = millis();
    if (time_reached(now_ms, g_next_print_ms)) {
        print_status();
        g_next_print_ms = now_ms + kPrintPeriodMs;
    }
    delay(20);
}
