#include "tl_spin_coll.h"
#include "tl_spin_allgather.h"
#include "tl_spin_bcast.h"
#include "tl_spin_mcast.h"
#include "tl_spin_p2p.h"
#include "components/mc/ucc_mc.h"

static ucc_status_t ucc_tl_spin_allgather_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t          *task        = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t          *team        = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t       *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    int                          reg_changed = 0;
    ucc_tl_spin_rcache_region_t *cache_entry;
    ucc_status_t                 status;

    if (!UCC_IS_INPLACE(task->super.bargs.args)) {
        status = ucc_mc_memcpy(PTR_OFFSET(task->dst_ptr, task->src_buf_size * UCC_TL_TEAM_RANK(team)),
                               task->src_ptr,
                               task->src_buf_size,
                               task->super.bargs.args.dst.info.mem_type, 
                               task->super.bargs.args.dst.info.mem_type);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }

    errno = 0;
    reg_changed = 0;
    if (UCC_OK != ucc_rcache_get(ctx->mcast.rcache,
                                 task->src_ptr,
                                 task->src_buf_size,
                                 &reg_changed,
                                 (ucc_rcache_region_t **)&cache_entry)) {
        tl_error(UCC_TASK_LIB(task), "Root failed to register send buffer memory errno: %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }
    task->cached_sbuf_mkey = cache_entry;

    errno = 0;
    reg_changed = 0;
    if (UCC_OK != ucc_rcache_get(ctx->p2p.rcache,
                                 task->dst_ptr,
                                 task->dst_buf_size,
                                 &reg_changed,
                                 (ucc_rcache_region_t **)&cache_entry)) {
        tl_error(UCC_TASK_LIB(task), "Root failed to register receive buffer memory errno: %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }
    task->cached_rbuf_mkey = cache_entry;

    status = ucc_tl_spin_coll_activate_workers(task);
    if (status != UCC_OK) {
        return status;
    }
    if (!UCC_IS_INPLACE(task->super.bargs.args)) {
        status = ucc_mc_memcpy(PTR_OFFSET(task->dst_ptr, task->src_buf_size * UCC_TL_TEAM_RANK(team)),
                               task->src_ptr,
                               task->src_buf_size,
                               task->super.bargs.args.dst.info.mem_type, 
                               task->super.bargs.args.dst.info.mem_type);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }
    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u", task, task->id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

static void ucc_tl_spin_allgather_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_coll_progress(task, &coll_task->status);
}

static ucc_status_t ucc_tl_spin_allgather_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_rcache_region_put(ctx->p2p.rcache, &task->cached_sbuf_mkey->super);
    ucc_rcache_region_put(ctx->mcast.rcache, &task->cached_rbuf_mkey->super);
    return ucc_tl_spin_coll_finalize(task);
}

ucc_status_t ucc_tl_spin_allgather_init(ucc_tl_spin_task_t   *task,
                                        ucc_base_coll_args_t *coll_args,
                                        ucc_tl_spin_team_t   *team)
{
    ucc_tl_spin_context_t *ctx = UCC_TL_SPIN_TEAM_CTX(team);
    size_t                 count         = coll_args->args.dst.info.count;
    size_t                 dt_size       = ucc_dt_size(coll_args->args.dst.info.datatype);
    size_t                 n_mcast_roots = ctx->cfg.n_ag_mcast_roots;;
    uint64_t               seq_length;

    if (UCC_TL_TEAM_SIZE(team) % ctx->cfg.n_ag_mcast_roots != 0) {
        n_mcast_roots = 1;
    }
    seq_length = UCC_TL_TEAM_SIZE(team) / n_mcast_roots;

    task->src_buf_size = count * dt_size / UCC_TL_TEAM_SIZE(team);
    task->dst_buf_size = count * dt_size;
    ucc_assert_always(task->dst_buf_size <= ctx->cfg.max_recv_buf_size);
    ucc_assert_always(task->dst_buf_size % UCC_TL_TEAM_SIZE(team) == 0);
    task->dst_ptr = coll_args->args.dst.info.buffer;
    if (!UCC_IS_INPLACE(task->super.bargs.args)) {
        task->src_ptr = coll_args->args.src.info.buffer;
    } else {
        task->src_ptr = task->dst_ptr + UCC_TL_TEAM_RANK(team) * task->src_buf_size;
    }

    ucc_tl_spin_bcast_init_task_tx_info(task, ctx);
    task->pkts_to_send = task->tx_thread_work / ctx->mcast.mtu;
    if (task->last_pkt_size) {
        task->pkts_to_send++;
    }
    task->pkts_to_recv     = task->pkts_to_send * (UCC_TL_TEAM_SIZE(team) - 1);
    task->start_chunk_id   = task->pkts_to_send * UCC_TL_TEAM_RANK(team);
    task->inplace_start_id = task->start_chunk_id;
    task->inplace_end_id   = task->inplace_start_id + task->pkts_to_send - 1;

    task->ag.mcast_seq_starter  = UCC_TL_TEAM_RANK(team)       % seq_length == 0 ? 1 : 0;
    task->ag.mcast_seq_finisher = (UCC_TL_TEAM_RANK(team) + 1) % seq_length == 0 ? 1 : 0;

    task->super.post      = ucc_tl_spin_allgather_start;
    task->super.progress  = ucc_tl_spin_allgather_progress;
    task->super.finalize  = ucc_tl_spin_allgather_finalize;
    task->coll_type       = UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER;

    ucc_assert_always(ctx->cfg.n_tx_workers == ctx->cfg.n_rx_workers);
    task->timeout = ((double)task->tx_thread_work * (UCC_TL_TEAM_SIZE(team))) /
                    (double)ctx->cfg.link_bw /
                    1000000000.0 * 
                    (double)ctx->cfg.timeout_scaling_param;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task, "
             "ag_seq_starter: %d ag_seq_finisher: %d, "
             "src_ptr: %p, src_buf_size: %zu, dst_ptr: %p, dst_buf_size: %zu "
             "pkts_to_send: %zu, pkts_to_recv: %zu",
             task->ag.mcast_seq_starter, task->ag.mcast_seq_finisher,
             task->src_ptr, task->src_buf_size, task->dst_ptr, task->dst_buf_size,
             task->pkts_to_send, task->pkts_to_recv);

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_tx_allgather_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task)
{
    ucc_status_t        status;
    int                 compls;
    struct ibv_wc       wc[1];

    if (!cur_task->ag.mcast_seq_starter) {
        compls = ib_cq_poll(ctx->reliability.cq, 1, wc);
        ucc_assert_always(compls == 1 && (wc->opcode == IBV_WC_RECV));
        ib_qp_post_recv(ctx->reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0); // re-post
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got mcast signal from left neighbor");
    }

    status = ucc_tl_spin_coll_worker_tx_handler(ctx, cur_task);
    ucc_assert_always(status == UCC_OK);

    if (!cur_task->ag.mcast_seq_finisher) {
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sending mcast signal");
        ib_qp_rc_post_send(ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, 0, cur_task->id);
        compls = ib_cq_poll(ctx->reliability.cq, 1, wc);
        ucc_assert_always(compls == 1 && (wc->opcode == IBV_WC_SEND));
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sent mcast signal to right neighbor. is finisher: %d", cur_task->ag.mcast_seq_finisher);
    }

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_rx_allgather_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task)
{
    ucc_status_t status;

    status = ucc_tl_spin_coll_worker_rx_handler(ctx, cur_task);
    ucc_assert_always(status == UCC_OK);

    status  = ucc_tl_spin_coll_worker_rx_reliability_handler(ctx, cur_task);
    ucc_assert_always(status == UCC_OK);

    return UCC_OK;
}