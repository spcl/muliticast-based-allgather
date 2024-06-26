#ifndef __FLEXIO_HELLO_WORLD_COM_H__
#define __FLEXIO_HELLO_WORLD_COM_H__

//#define DPA_DEBUG
//#define DPA_PROFILE
//#define DPA_CQ_POLL_PROFILE
#define DPA_RELIABILITY_BITMAP
#define DPA_RELIABILITY_BITMAP_NAIVE
//#define DPA_RELIABILITY_BITMAP_DEBUG
//#define DPA_RELIABILITY_COUNTER

// ugly
#define MAX_DATAPATH_WIDTH           128 // witdh == how many threads can be in the activated state at once
#define NET_CQQP_ARRAY_BASE_IDX      0
#define LOOPBACK_CQQP_ARRAY_BASE_IDX (NET_CQQP_ARRAY_BASE_IDX + MAX_DATAPATH_WIDTH)
#define HOST_CQQP_ARRAY_BASE_IDX     (LOOPBACK_CQQP_ARRAY_BASE_IDX + MAX_DATAPATH_WIDTH)
#define MAX_FLEXIO_CQS               (HOST_CQQP_ARRAY_BASE_IDX + MAX_DATAPATH_WIDTH)
#define MAX_FLEXIO_QPS               MAX_FLEXIO_CQS
#define MAX_FLEXIO_DEV_QPS           HOST_CQQP_ARRAY_BASE_IDX // threads need to access only network and loopback QPs
#define MAX_FLEXIO_DEV_CQS           HOST_CQQP_ARRAY_BASE_IDX // threads need to access only network and loopback QPs

#define DPA_WORK_COMPLETED_MAGICNUM 0xDEADBEAF
#define DPA_BRINGUP_TEST_MAGICNUM 46224
#define DPA_MAX_NET_CQ_POLL_BATCH 128
#define DPA_RECVBUF_RELIABILITY_ARRAY_SIZE 1024 // 0.25GB per QP
#define DPA_CQE_NOTIFICATION_FREQUENCY 16

enum dpa_eh_types {
    DPA_EH_PINGPONG_SERVER         = 0x0,
    DPA_EH_TPUT_SERVER             = 0x1,
    DPA_EH_TPUT_STAGING_SERVER     = 0x2,
    DPA_EH_TPUT_CLIENT             = 0x3,
};

// PRM, Table 212
enum dpa_opcode_types {
    DPA_CQE_REQUESTER               = 0x0,
    DPA_CQE_RESPONDER_WRITE_W_IMM   = 0x1,
    DPA_CQE_RESPONDER_SEND          = 0x2,
    DPA_CQE_RESPONDER_SEND_WITH_IMM = 0x3
};

struct dpa_pingpong_server_ctx {
    uint32_t cq_id;
    uint32_t qp_id;
    uint64_t dgram_size;
    uint64_t base_info_size;
    uint64_t lkey;
    uint64_t ptr;
} __attribute__((__packed__, aligned(8)));

typedef struct {
	uint32_t cq_number;
	struct flexio_dev_cqe64 *cq_ring, *cqe;
	uint32_t cq_idx;
	uint8_t cq_hw_owner_bit;
	uint32_t *cq_dbr;
	uint32_t log_cq_depth;
} dpa_cq_ctx_t;

struct dpa_transfer_cq {
    uint32_t cq_num;
    uint32_t log_cq_depth;
    uint32_t ci_idx;
    uint32_t poll_batch_size;
    uint8_t hw_owner_bit;
    uint8_t reserved[7];
    flexio_uintptr_t cq_ring_daddr;
    flexio_uintptr_t cq_dbr_daddr;
} __attribute__((__packed__, aligned(8)));

struct dpa_transfer_qp {
    uint32_t qp_num;
    uint32_t sqd_mkey_id;
    uint32_t rqd_mkey_id;
    uint32_t reserved;
    uint32_t log_qp_sq_depth;
    uint32_t log_qp_rq_depth;
    uint32_t sq_ci_idx;
    uint32_t rq_ci_idx;
    uint32_t sq_pi_idx;
    uint32_t rq_pi_idx;

    uint64_t sqd_lkey;
    uint64_t rqd_lkey;

    flexio_uintptr_t qp_dbr_daddr;
    flexio_uintptr_t qp_sq_daddr;
    flexio_uintptr_t qp_rq_daddr;
    flexio_uintptr_t sqd_daddr;
    flexio_uintptr_t rqd_daddr;
} __attribute__((__packed__, aligned(8)));

struct dpa_tput_ctx {
    uint32_t worker_id;
    uint32_t finisher_id;
    uint32_t n_workers;
    uint32_t chunk_size;
    uint32_t tx_window;
    uint32_t iter;
    uint64_t to_process;
    uint64_t payload_rkey;
    uint64_t payload_ptr;
    uint64_t ack_lkey;
    uint64_t ack_ptr;
    uint64_t counter_lkey;
    uint64_t counter_ptr;
#ifdef DPA_RELIABILITY_BITMAP
    uint64_t recvbuf_bitmaps[MAX_DATAPATH_WIDTH][DPA_RECVBUF_RELIABILITY_ARRAY_SIZE];
#elif defined(DPA_RELIABILITY_COUNTER)
    uint32_t gaps_to_fetch[MAX_DATAPATH_WIDTH][DPA_RECVBUF_RELIABILITY_ARRAY_SIZE * 2];
#endif  
} __attribute__((__packed__, aligned(8)));

/* Transport data from HOST application to HW application */
struct dpa_export_data {
	uint32_t outbox_id;
  	uint32_t window_id;
	struct dpa_transfer_cq cq_transfer[MAX_FLEXIO_DEV_CQS];
	struct dpa_transfer_qp qp_transfer[MAX_FLEXIO_DEV_QPS];
    uint32_t n_cqs;
    uint32_t n_qps;
    flexio_uintptr_t verb;
    flexio_uintptr_t app_ctx;
} __attribute__((__packed__, aligned(8)));

#endif
