#pragma once

#include <stdint.h>

#define RD_INSTR(x) __asm__ volatile("rdinstret %0 " \
                                     : "=r"((x)))
#define RD_CYCLE(x) __asm__ volatile("rdcycle %0 " \
                                     : "=r"((x)))
#define MAX(a,b) ((a) > (b) ? a : b)
#define MIN(a,b) ((a) < (b) ? a : b)

#ifdef DEV_CODE
#pragma clang diagnostic ignored "-Wlanguage-extension-token"
#endif

typedef struct dpa_profile_counter
{
    uint64_t sum;
    uint64_t min, max;
    uint32_t count;
} dpa_profile_counter_t;

typedef struct dpa_profile_timer
{
    uint64_t cycle_tmp;
    uint64_t instr_tmp;
    uint8_t flag;

    dpa_profile_counter_t cycles;
    dpa_profile_counter_t instrs;
} dpa_profile_timer_t;

#ifdef DPA_PROFILE

#define DPA_PROFILE_TRACE_DEPTH 8

typedef struct dpa_profile_trace
{
    uint32_t values[DPA_PROFILE_TRACE_DEPTH];
    uint32_t counter;
} dpa_profile_trace_t;

#define DPA_PROFILE_TRACE_ADD(info, val)                                        \
{                                                                               \
    (info).values[(info).counter % DPA_PROFILE_TRACE_DEPTH] = (uint32_t) val;   \
    (info).counter++;                                                           \
}

#define DPA_PROFILE_TRACE_TIME(info)    \
{                                       \
    uint64_t tmp;                       \
    RD_CYCLE(tmp);                      \
    DPA_PROFILE_TRACE_ADD(info, tmp);   \
}

#define DPA_PROFILE_COUNTER_ADD(info, val)            \
{                                                     \
    (info).sum += val;                                \
    (info).max = MAX((info).max, val);                \
    (info).min = MIN((info).min, val);                \
    (info).count++;                                   \
}

#define DPA_PROFILE_TIMER_LAP(info)                             \
{                                                               \
    uint64_t tmp_cycle, tmp_instr;                              \
    RD_CYCLE(tmp_cycle);                                        \
    RD_INSTR(tmp_instr);                                        \
    if ((info).flag)                                            \
    {                                                           \
        uint64_t lap_cycle = tmp_cycle - (info).cycle_tmp;      \
        uint64_t lap_instr = tmp_instr - (info).instr_tmp;      \
        DPA_PROFILE_COUNTER_ADD(info.cycles, lap_cycle);        \
        DPA_PROFILE_COUNTER_ADD(info.instrs, lap_instr);        \
    }                                                           \
    (info).cycle_tmp = tmp_cycle;                               \
    (info).instr_tmp = tmp_instr;                               \
    (info).flag = 1;                                            \
}

#define DPA_PROFILE_TRACE_DECLARE(name) dpa_profile_trace_t name
#define DPA_PROFILE_TIMER_DECLARE(name) dpa_profile_timer_t name
#define DPA_PROFILE_COUNTER_DECLARE(name) dpa_profile_counter_t name

#define DPA_PROFILE_TRACE_INIT(info)                \
{                                                   \
    for (int i=0; i<DPA_PROFILE_TRACE_DEPTH; i++)   \
    {                                               \
        (info).values[i] = 0;                       \
    }                                               \
    info.counter = 0;                               \
}

#define DPA_PROFILE_COUNTER_INIT(info)  \
{                                       \
    info.sum = 0;                       \
    info.min = UINT64_MAX;              \
    info.max = 0;                       \
    info.count = 0;                     \
}

#define DPA_PROFILE_TIMER_INIT(info)        \
{                                           \
    DPA_PROFILE_COUNTER_INIT((info).cycles);\
    DPA_PROFILE_COUNTER_INIT((info).instrs);\
    info.flag = 0;                          \
}

#define DPA_PROFILE_TIMER_START(info)       \
{                                           \
    RD_INSTR((info).instr_tmp);             \
    RD_CYCLE((info).cycle_tmp);             \
}

#define DPA_PROFILE_TIMER_STOP(info)                        \
{                                                           \
    uint64_t tmp_cycle, tmp_instr;                          \
    RD_CYCLE(tmp_cycle);                                    \
    RD_INSTR(tmp_instr);                                    \
    tmp_cycle = (tmp_cycle - (info).cycle_tmp);             \
    tmp_instr = (tmp_instr - (info).instr_tmp);             \
    DPA_PROFILE_COUNTER_ADD(info.cycles, tmp_cycle);        \
    DPA_PROFILE_COUNTER_ADD(info.instrs, tmp_instr);        \
}

#define DPA_PROFILE_COUNTER_AVG(info) ((uint64_t) (info.sum / info.count))

#else
#define DPA_PROFILE_TIMER_DECLARE(name)
#define DPA_PROFILE_TIMER_INIT(info)
#define DPA_PROFILE_TIMER_START(info)
#define DPA_PROFILE_TIMER_STOP(info)
#define DPA_PROFILE_COUNTER_AVG(info) 0
#define DPA_PROFILE_TIMER_LAP(info)
#define DPA_PROFILE_COUNTER_DECLARE(info)
#define DPA_PROFILE_COUNTER_INIT(info)
#define DPA_PROFILE_COUNTER_ADD(info, val)
#define DPA_PROFILE_TRACE_TIME(info)
#define DPA_PROFILE_TRACE_ADD(info, val)
#define DPA_PROFILE_TRACE_DECLARE(name)
#define DPA_PROFILE_TRACE_INIT(info)
#endif
