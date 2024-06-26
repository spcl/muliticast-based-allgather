#ifndef UCC_TL_SPIN_H_
#define UCC_TL_SPIN_H_

#include "components/tl/ucc_tl.h"
#include "components/tl/ucc_tl_log.h"
#include "components/tl/mlx5/mcast/tl_mlx5_mcast_helper.h"
#include "utils/ucc_rcache.h"
#include "utils/ucc_mpool.h"
#include "tl_spin_rbuf.h"

#define UCC_TL_SPIN_USE_SERVICE_BARRIER
//#define UCC_TL_SPIN_PROFILE_TASK 1
//#define UCC_TL_SPIN_DISABLE_MCAST 1

#ifdef UCC_TL_SPIN_PROFILE_TASK
#include "tl_spin_tsc.h"
#endif

#include <infiniband/verbs.h>
#include <pthread.h>

#ifndef UCC_TL_SPIN_DEFAULT_SCORE
#define UCC_TL_SPIN_DEFAULT_SCORE 42
#endif

typedef struct ucc_tl_spin_iface {
    ucc_tl_iface_t super;
} ucc_tl_spin_iface_t;

/* Extern iface should follow the pattern: ucc_tl_<tl_name> */
extern ucc_tl_spin_iface_t ucc_tl_spin;

typedef struct ucc_tl_spin_lib_config {
    ucc_tl_lib_config_t super;
    uint32_t            dummy_param; // TBD
} ucc_tl_spin_lib_config_t;

typedef struct ucc_tl_spin_context_config {
    ucc_tl_context_config_t super;
    char                   *ib_dev_name;
    int                     n_mcg;
    int                     n_tx_workers;
    int                     n_rx_workers;
    int                     mcast_cq_depth;
    int                     mcast_sq_depth;
    int                     mcast_rq_depth;
    int                     mcast_tx_batch_sz;
    int                     p2p_cq_depth;
    int                     p2p_qp_depth;
    int                     start_core_id;
    int                     link_bw;
    int                     timeout_scaling_param;
    int                     n_ag_mcast_roots;
    unsigned int            max_recv_buf_size;
} ucc_tl_spin_context_config_t;

typedef struct ucc_tl_spin_lib {
    ucc_tl_lib_t             super;
    ucc_tl_spin_lib_config_t cfg;
} ucc_tl_spin_lib_t;
UCC_CLASS_DECLARE(ucc_tl_spin_lib_t, const ucc_base_lib_params_t *,
                  const ucc_base_config_t *);

#define UCC_TL_SPIN_DEFAULT_PKEY 0 
#define UCC_TL_SPIN_GID_TBL_MAX_ENTRIES 32
typedef struct ucc_tl_spin_ib_dev_addr {
    int                 port_num;
    enum ibv_mtu        mtu;
    uint16_t            lid;
    uint8_t             gid_table_index;
	union ibv_gid       gid;
} ucc_tl_spin_ib_dev_addr_t;

typedef struct ucc_tl_spin_qp_addr {
    uint32_t                  qpn;
    ucc_tl_spin_ib_dev_addr_t dev_addr;
} ucc_tl_spin_qp_addr_t;

typedef struct ucc_tl_spin_p2p_context {
    struct ibv_context       *dev;
    struct ibv_pd            *pd;
    ucc_tl_spin_ib_dev_addr_t dev_addr;
    ucc_rcache_t             *rcache;
} ucc_tl_spin_p2p_context_t;

/* Mostly resembles ucc_tl_mlx5_mcast_coll_context */
typedef struct ucc_tl_spin_mcast_context {
    struct ibv_context        *dev;
    struct ibv_pd             *pd;
    int                        max_qp_wr;
    int                        ib_port;
    int                        pkey_index;
    int                        mtu;
    struct rdma_cm_id         *id;
    struct rdma_event_channel *channel;
    unsigned int               gid;
    ucc_rcache_t              *rcache;
} ucc_tl_spin_mcast_context_t;

typedef struct ucc_tl_spin_rcache_region {
    ucc_rcache_region_t super;
    struct ibv_mr      *mr;
} ucc_tl_spin_rcache_region_t;

typedef struct ucc_tl_spin_context {
    ucc_tl_context_t             super;
    ucc_tl_spin_context_config_t cfg;
    ucc_mpool_t                  req_mp;
    char                        *devname;
    int                          ib_port;
    ucc_tl_spin_p2p_context_t    p2p;
    ucc_tl_spin_mcast_context_t  mcast;
    size_t                       cur_core_id;
} ucc_tl_spin_context_t;
UCC_CLASS_DECLARE(ucc_tl_spin_context_t, const ucc_base_context_params_t *,
                  const ucc_base_config_t *);

typedef enum
{
    UCC_TL_SPIN_WORKER_TYPE_CTRL,
    UCC_TL_SPIN_WORKER_TYPE_TX,
    UCC_TL_SPIN_WORKER_TYPE_RX
} ucc_tl_spin_worker_type_t;

typedef enum
{
    UCC_TL_SPIN_WORKER_POLL  = 0,
    UCC_TL_SPIN_WORKER_START = 1,
    UCC_TL_SPIN_WORKER_FIN   = 2
} ucc_tl_spin_worker_signal_t;

// forward declaration
typedef struct ucc_tl_spin_team ucc_tl_spin_team_t;

#define UCC_TL_SPIN_IB_GRH_FOOTPRINT 64 // to make sure it is aligned

typedef enum
{
    UCC_TL_SPIN_RELIABILITY_PKT_NEED_FETCH  = 0,
    UCC_TL_SPIN_RELIABILITY_PKT_FIN         = 1
} ucc_tl_spin_reliability_pkt_type_t;

typedef union ucc_tl_spin_reliability_proto_info {
    struct {
        uint32_t pkt_type : 3;
        uint32_t rank_id  : 29;
    } proto;
    uint32_t imm_data;
} ucc_tl_spin_reliability_proto_info_t;

typedef enum {
    UCC_TL_SPIN_RELIABILITY_PROTO_INIT                = 0,
    UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FETCH_REQ      = 1,
    UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_ACK            = 2,
    UCC_TL_SPIN_RELIABILITY_PROTO_ISSUE_READ          = 3,
    UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FIN            = 4,
    UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_FIN_SEND_COMPL = 5,
    UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE            = 6,
    UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_REQ            = 7,
    UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_LN_FETCH       = 8
} ucc_tl_spin_reliability_proto_state_t;

typedef struct ucc_tl_spin_bitmap_descr {
    uint64_t *buf;
    size_t    size;
} ucc_tl_spin_bitmap_descr_t;

typedef struct ucc_tl_spin_buf_info {
    uint64_t addr;
    uint32_t rkey;
} ucc_tl_spin_buf_info_t;

typedef struct ucc_tl_spin_reliability_proto {
    size_t                                to_recv;
    ucc_tl_spin_reliability_proto_state_t ln_state;
    ucc_tl_spin_reliability_proto_state_t rn_state;
    ucc_tl_spin_bitmap_descr_t            bitmap;
    size_t                                current_bitmap_offset;
    size_t                                last_gap_size;
    ucc_rank_t                            current_rank;
    ucc_tl_spin_reliability_proto_info_t  cached_req;
    size_t                                n_missing_ranks;
    ucc_rank_t                           *missing_ranks;
    size_t                               *recvd_per_rank;
    struct ibv_cq                        *cq;
    struct ibv_qp                        *qps[2];
    struct ibv_sge                        sge;
    struct ibv_send_wr                    rd_swr;
    ucc_tl_spin_buf_info_t               *ln_rbuf_info;
    struct ibv_mr                        *ln_rbuf_info_mr;
    ucc_tl_spin_buf_info_t               *rn_rbuf_info;
    struct ibv_mr                        *rn_rbuf_info_mr;
} ucc_tl_spin_reliability_proto_t;

typedef struct ucc_tl_spin_worker_info {
    ucc_tl_spin_context_t          *ctx;
    ucc_tl_spin_team_t             *team;
    ucc_tl_spin_worker_type_t       type;
    unsigned int                    id;
    pthread_t                       pthread;
    struct ibv_cq                  *cq;
    struct ibv_qp                 **qps;
    struct ibv_ah                 **ahs;
    struct ibv_send_wr            **swrs;
    struct ibv_sge                **ssges;
    struct ibv_recv_wr            **rwrs;
    struct ibv_sge                **rsges;
    struct ibv_mr                 **staging_rbuf_mr;
    char                          **staging_rbuf;
    struct ibv_mr                 **grh_buf_mr;
    char                          **grh_buf;
    size_t                         *tail_idx;
    size_t                          staging_rbuf_len;
    size_t                          grh_buf_len;
    ucc_tl_spin_reliability_proto_t reliability;
    int                            *barrier_scratch;
    uint32_t                        n_mcg;
} ucc_tl_spin_worker_info_t;

ucc_status_t ucc_tl_spin_team_service_barrier_post(ucc_tl_spin_team_t *ctx, int *barrier_scratch,
                                                   ucc_service_coll_req_t **barrier_req);
ucc_status_t ucc_tl_spin_team_service_coll_test(ucc_service_coll_req_t *req, int blocking);

#define UCC_TL_SPIN_JOIN_MAGICNUM 0xDEADBEAF

typedef struct ucc_tl_spin_mcast_join_info {
    ucc_status_t              status;
    struct sockaddr_in6       saddr;
    ucc_tl_spin_ib_dev_addr_t mcg_addr;
    ucc_tl_spin_ib_dev_addr_t mcg_addr_dst;
    unsigned int              magic_num;
} ucc_tl_spin_mcast_join_info_t;

#define UCC_TL_SPIN_MAX_TASKS 256

typedef union ucc_tl_spin_packed_chunk_id {
    struct {
        uint32_t task_id  : 8;
        uint32_t chunk_id : 24;
    } chunk_metadata;
    uint32_t imm_data;
} ucc_tl_spin_packed_chunk_id_t;

typedef enum {
    UCC_TL_SPIN_WORKER_TASK_TYPE_KILL = 0,
    UCC_TL_SPIN_WORKER_TASK_TYPE_BCAST = 1,
    UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER = 3,
} ucc_tl_spin_task_type_t;

typedef struct ucc_tl_spin_task {
    ucc_coll_task_t              super;
    atomic_int                   tx_start;
    atomic_int                   tx_compls;
    atomic_int                   rx_compls;
    ucc_tl_spin_task_type_t      coll_type;
    uint32_t                     id;
    size_t                       src_buf_size;
    size_t                       dst_buf_size;
    size_t                       start_chunk_id;
    size_t                       inplace_start_id;
    size_t                       inplace_end_id;
    size_t                       tx_thread_work;
    size_t                       batch_bsize;
    size_t                       n_batches;
    size_t                       last_batch_size;
    size_t                       last_pkt_size;
    size_t                       pkts_to_send;
    size_t                       pkts_to_recv;
    struct {
        int mcast_seq_starter;
        int mcast_seq_finisher;
    } ag;
    double                       timeout;
    void                        *src_ptr;
    void                        *dst_ptr;
    ucc_tl_spin_rcache_region_t *cached_sbuf_mkey;
    ucc_tl_spin_rcache_region_t *cached_rbuf_mkey;
#ifdef UCC_TL_SPIN_PROFILE_TASK
    tsc_counter                  tmp_counter;
    tsc_counter                  total_cycles;
    tsc_counter                  rx_cycles;
    tsc_counter                  tx_cycles;
    tsc_counter                  tx_mcast_send_cycles;
    tsc_counter                  tx_cq_cycles;
    tsc_counter                  tx_loop;
    tsc_counter                  reliability_cycles;
    int                          tx_collected;
    int                          rx_collected;
#endif
} ucc_tl_spin_task_t;

#define UCC_TL_SPIN_LN_QP_ID    0
#define UCC_TL_SPIN_RN_QP_ID    1
#define UCC_TL_SPIN_MAX_MCG     1
#define UCC_TL_SPIN_P2P_QPS_NUM 2 // 2 QPs to have ring (TODO: check service collectives)
#define UCC_TL_SPIN_MAX_CQS_NUM (UCC_TL_SPIN_P2P_QPS_NUM + 2 * (UCC_TL_SPIN_MAX_MCG))
typedef struct ucc_tl_spin_team {
    ucc_tl_team_t                  super;
    ucc_team_t                    *base_team;
    ucc_subset_t                   subset;
    ucc_rank_t                     size;
    ucc_tl_spin_mcast_join_info_t *mcg_infos;
    ucc_tl_spin_worker_info_t     *ctrl_ctx;
    ucc_tl_spin_worker_info_t     *workers;
    uint32_t                       task_id;
    rbuf_t                         task_rbuf;
} ucc_tl_spin_team_t;
UCC_CLASS_DECLARE(ucc_tl_spin_team_t, ucc_base_context_t *,
                  const ucc_base_team_params_t *);

#define UCC_TL_SPIN_SUPPORTED_COLLS (UCC_COLL_TYPE_BCAST | UCC_COLL_TYPE_ALLGATHER)

#define UCC_TL_SPIN_CTX_LIB(_ctx)                                          \
    (ctx->super.super.lib)

#define UCC_TL_SPIN_TEAM_LIB(_team)                                        \
    (ucc_derived_of((_team)->super.super.context->lib, ucc_tl_spin_lib_t))

#define UCC_TL_SPIN_TEAM_CTX(_team)                                        \
    (ucc_derived_of((_team)->super.super.context, ucc_tl_spin_context_t))

#define UCC_TL_SPIN_TASK_TEAM(_task)                                       \
    (ucc_derived_of((_task)->super.team, ucc_tl_spin_team_t))

#define UCC_TL_SPIN_CHK_PTR(lib, func, ptr, status, err_code, err_handler) \
    {                                                                      \
        ptr = (func);                                                      \
        if (!ptr) {                                                        \
            tl_error(lib, "%s failed with errno %d", #func, errno);        \
            status = (err_code);                                           \
            goto err_handler;                                              \
        } else {                                                           \
            status = UCC_OK;                                               \
        }                                                                  \
    }

#define UCC_TL_SPIN_CHK_ERR(lib, func, status, err_code, err_handler) \
    {                                                                 \
        if (func) {                                                   \
            tl_error(lib, "%s failed with errno %d", #func, errno);   \
            status = (err_code);                                      \
            goto err_handler;                                         \
        } else {                                                      \
            status = UCC_OK;                                          \
        }                                                             \
    }

#endif