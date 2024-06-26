#include "tl_spin.h"

static ucc_config_field_t ucc_tl_spin_lib_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_tl_spin_lib_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_tl_lib_config_table)},

    {"DUMMY_PARAM", "42", "Dummy parameter that affects the behavior or sPIN library",
     ucc_offsetof(ucc_tl_spin_lib_config_t, dummy_param),
     UCC_CONFIG_TYPE_UINT},

    {NULL}};

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_lib_t, ucc_base_lib_t,
                          const ucc_base_lib_params_t *,
                          const ucc_base_config_t *);

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_lib_t, ucc_base_lib_t);

ucc_status_t ucc_tl_spin_get_lib_attr(const ucc_base_lib_t *lib,
                                      ucc_base_lib_attr_t  *base_attr);

ucc_status_t ucc_tl_spin_get_lib_properties(ucc_base_lib_properties_t *prop);

static ucc_config_field_t ucc_tl_spin_context_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_tl_spin_context_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_tl_context_config_table)},

    {"IB_DEV_NAME", "", "IB device that will be used for context creation",
     ucc_offsetof(ucc_tl_spin_context_config_t, ib_dev_name),
     UCC_CONFIG_TYPE_STRING},

    {"MCAST_GROUPS", "1", "Number of multicast groups",
     ucc_offsetof(ucc_tl_spin_context_config_t, n_mcg),
     UCC_CONFIG_TYPE_INT},

    {"TX_WORKERS", "1", "Number of TX workers",
     ucc_offsetof(ucc_tl_spin_context_config_t, n_tx_workers),
     UCC_CONFIG_TYPE_INT},
     
    {"RX_WORKERS", "1", "Number of RX workers",
     ucc_offsetof(ucc_tl_spin_context_config_t, n_rx_workers),
     UCC_CONFIG_TYPE_INT},

    {"MCAST_CQ_DEPTH", "128", "Multicast CQ depth",
     ucc_offsetof(ucc_tl_spin_context_config_t, mcast_cq_depth),
     UCC_CONFIG_TYPE_INT},

    {"MCAST_SQ_DEPTH", "128", "Multicast SQ depth",
     ucc_offsetof(ucc_tl_spin_context_config_t, mcast_sq_depth),
     UCC_CONFIG_TYPE_INT},

    {"MCAST_RQ_DEPTH", "128", "Multicast RQ depth",
     ucc_offsetof(ucc_tl_spin_context_config_t, mcast_rq_depth),
     UCC_CONFIG_TYPE_INT},

    {"MCAST_TX_BATCH_SZ", "128", "Size of batch with MTU-sized multicast sends",
     ucc_offsetof(ucc_tl_spin_context_config_t, mcast_tx_batch_sz),
     UCC_CONFIG_TYPE_INT},

    {"P2P_CQ_DEPTH", "128", "P2P CQ depth",
     ucc_offsetof(ucc_tl_spin_context_config_t, p2p_cq_depth),
     UCC_CONFIG_TYPE_INT},

    {"P2P_QP_DEPTH", "128", "P2P QP depth",
     ucc_offsetof(ucc_tl_spin_context_config_t, p2p_qp_depth),
     UCC_CONFIG_TYPE_INT},

    {"START_CORE_ID", "0", "Start CPU core id to pin worker threads",
     ucc_offsetof(ucc_tl_spin_context_config_t, start_core_id),
     UCC_CONFIG_TYPE_INT},

    {"LINK_BW", "7", "Link bandwidth in GB/s", // CX-3 == 56Gbit/s == 7GB/s
     ucc_offsetof(ucc_tl_spin_context_config_t, link_bw),
     UCC_CONFIG_TYPE_INT},

    {"TIMEOUT_SCALING", "5", "Scaling of receive handler loop timeout", // CX-3 == 56Gbit/s == 7GB/s
     ucc_offsetof(ucc_tl_spin_context_config_t, timeout_scaling_param),
     UCC_CONFIG_TYPE_INT},

    {"ALLGATHER_MCAST_ROOTS", "2", "Number of roots simultaneously multicasting in Allgather",
     ucc_offsetof(ucc_tl_spin_context_config_t, n_ag_mcast_roots),
     UCC_CONFIG_TYPE_INT},

    {"MAX_RECV_BUF_SIZE", "536870912", "Max receive buffer size",
     ucc_offsetof(ucc_tl_spin_context_config_t, max_recv_buf_size),
     UCC_CONFIG_TYPE_UINT},

    {NULL}};

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_context_t, ucc_base_context_t,
                          const ucc_base_context_params_t *,
                          const ucc_base_config_t *);

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_context_t, ucc_base_context_t);

ucc_status_t ucc_tl_spin_get_context_attr(const ucc_base_context_t *context,
                                          ucc_base_ctx_attr_t *     base_attr);

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_team_t, ucc_base_team_t,
                          ucc_base_context_t *, const ucc_base_team_params_t *);

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team);

ucc_status_t ucc_tl_spin_team_destroy(ucc_base_team_t *tl_team);

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *team,
                                   ucc_coll_task_t **task);

ucc_status_t ucc_tl_spin_team_get_scores(ucc_base_team_t   *tl_team,
                                         ucc_coll_score_t **score_p);

UCC_TL_IFACE_DECLARE(spin, SPIN);

__attribute__((constructor)) static void tl_spin_iface_init(void)
{
}