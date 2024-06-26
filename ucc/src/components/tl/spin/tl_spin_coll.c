#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"
#include "tl_spin_rbuf.h"
#include "tl_spin_bcast.h"
#include "tl_spin_allgather.h"
#include "tl_spin_bitmap.h"

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *tl_team,
                                   ucc_coll_task_t **task_h)
{
    ucc_tl_spin_context_t *ctx    = ucc_derived_of(tl_team->context, ucc_tl_spin_context_t);
    ucc_tl_spin_team_t    *team   = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_task_t    *task   = NULL;
    ucc_status_t           status = UCC_OK;

    task = ucc_mpool_get(&ctx->req_mp);
    ucc_coll_task_init(&task->super, coll_args, tl_team);

    switch (coll_args->args.coll_type) {
    case UCC_COLL_TYPE_BCAST:
        status = ucc_tl_spin_bcast_init(task, coll_args, team);
        break;
    case UCC_COLL_TYPE_ALLGATHER:
        status = ucc_tl_spin_allgather_init(task, coll_args, team);
        break;
    default:
        tl_debug(UCC_TASK_LIB(task),
                 "collective %d is not supported",
                 coll_args->args.coll_type);
        status = UCC_ERR_NOT_SUPPORTED;
        goto err;
    }

    task->id = team->task_id++ % UCC_TL_SPIN_MAX_TASKS;

#ifdef UCC_TL_SPIN_PROFILE_TASK
    task->total_cycles.int64         = 0;
    task->tx_cycles.int64            = 0;
    task->rx_cycles.int64            = 0;
    task->tx_cycles.int64            = 0;
    task->tx_mcast_send_cycles.int64 = 0;
    task->tx_cq_cycles.int64         = 0;
    task->reliability_cycles.int64   = 0;
    task->tx_collected               = 0;
    task->rx_collected               = 0;
#endif

    tl_debug(UCC_TASK_LIB(task), "init coll task ptr=%p tgid=%u", task, task->id);
    *task_h = &task->super;
    return status;

err:
    ucc_mpool_put(task);
    return status;
}

inline ucc_status_t 
ucc_tl_spin_coll_activate_workers(ucc_tl_spin_task_t *task)
{
    ucc_tl_spin_team_t     *team = UCC_TL_SPIN_TASK_TEAM(task);
#ifdef UCC_TL_SPIN_USE_SERVICE_BARRIER
    ucc_service_coll_req_t *barrier_req;
#endif

    if (rbuf_has_space(&team->task_rbuf)) {
#ifdef UCC_TL_SPIN_PROFILE_TASK
        TSC_START_GLOBAL(task->total_cycles);
#endif

        task->tx_start  = 0;
        task->tx_compls = 0;
        task->rx_compls = 0;

        // barrier 1
#ifdef UCC_TL_SPIN_USE_SERVICE_BARRIER
        ucc_tl_spin_team_service_barrier_post(team, team->ctrl_ctx->barrier_scratch, &barrier_req);
        ucc_tl_spin_team_service_coll_test(barrier_req, 1);
#else 
        ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, team->ctrl_ctx);
#endif

#ifdef UCC_TL_SPIN_PROFILE_TASK
        TSC_START_LOCAL(task->rx_cycles);
#endif

        rbuf_push_head(&team->task_rbuf, (uintptr_t)task);
        // rx threads might already see the task and start polling

#ifdef UCC_TL_SPIN_USE_SERVICE_BARRIER
        ucc_tl_spin_team_service_barrier_post(team, team->ctrl_ctx->barrier_scratch, &barrier_req);
        ucc_tl_spin_team_service_coll_test(barrier_req, 1);
#else
        ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, team->ctrl_ctx);
#endif

#ifdef UCC_TL_SPIN_PROFILE_TASK
        TSC_START_LOCAL(task->tx_cycles);
#endif
        // fire up tx threads
        task->tx_start = 1;
    } else {
        return UCC_ERR_NO_RESOURCE;
    }

    return UCC_OK;
}

inline void 
ucc_tl_spin_coll_progress(ucc_tl_spin_task_t *task, ucc_status_t *coll_status)
{
    ucc_tl_spin_team_t    *team = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
#ifdef UCC_TL_SPIN_PROFILE_TASK
    if (!task->tx_collected && (task->tx_compls == ctx->cfg.n_tx_workers)) {
        TSC_STOP(task->tx_cycles);
        task->tx_collected = 1;
    }
    if (!task->rx_collected && (task->rx_compls == ctx->cfg.n_rx_workers)) {
        TSC_STOP(task->rx_cycles);
        task->rx_collected = 1;
    }
#endif
    if ((task->tx_compls + task->rx_compls) != (ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers)) {
        *coll_status = UCC_INPROGRESS;
        return;
    }
    rbuf_pop_tail(&team->task_rbuf);
#ifdef UCC_TL_SPIN_PROFILE_TASK
    TSC_STOP(task->total_cycles);
#endif
    *coll_status = UCC_OK;
}

ucc_status_t ucc_tl_spin_coll_finalize(ucc_tl_spin_task_t *task)
{
#ifdef UCC_TL_SPIN_PROFILE_TASK
    ucc_tl_spin_team_t *team = UCC_TL_SPIN_TASK_TEAM(task);
    tl_error(UCC_TASK_LIB(task),
             "task %u statistics: "
             "to_recv=%zu, n_drops=%zu, "
             "total_cycles=%llu, "
             "tx_cycles=%llu, "
             "tx_mcast_send_cycles=%llu, "
             "tx_cq_cycles=%llu, "
             "tx_loop=%llu, "
             "rx_cycles=%llu, "
             "reliability_cycles=%llu",
             task->id, 
             task->pkts_to_recv,
             team->workers[1].reliability.to_recv, // rx worker
             task->total_cycles.int64,
             task->tx_cycles.int64,
             task->tx_mcast_send_cycles.int64,
             task->tx_cq_cycles.int64,
             task->tx_loop.int64,
             task->rx_cycles.int64,
             task->reliability_cycles.int64);
#endif
    tl_debug(UCC_TASK_LIB(task), "finalizing coll task ptr=%p gid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

void ucc_tl_spin_coll_post_kill_task(ucc_tl_spin_team_t *team)
{
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_tl_spin_task_t    *task = ucc_mpool_get(&ctx->req_mp);
    task->coll_type = UCC_TL_SPIN_WORKER_TASK_TYPE_KILL;
    ucc_assert_always(rbuf_has_space(&team->task_rbuf));
    rbuf_push_head(&team->task_rbuf, (uintptr_t)task);
}

void ucc_tl_spin_coll_kill_task_finalize(ucc_tl_spin_team_t *team)
{
    int idx;
    ucc_tl_spin_task_t *task = (ucc_tl_spin_task_t *)rbuf_get_tail_element(&team->task_rbuf, &idx);
    ucc_assert_always((task != NULL) && (task->coll_type == UCC_TL_SPIN_WORKER_TASK_TYPE_KILL));
    ucc_mpool_put(task);
    rbuf_pop_tail(&team->task_rbuf);
}

void *ucc_tl_spin_coll_worker_main(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx             = (ucc_tl_spin_worker_info_t *)arg;
    int                        got_kill_signal = 0;
    ucc_tl_spin_task_t        *cur_task        = NULL;
    int                        cur_task_idx    = 0;
    int                        prev_task_idx   = INT_MAX;
    ucc_status_t               status;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread started", ctx->id);

    while (1) {
        cur_task = (ucc_tl_spin_task_t *)rbuf_get_tail_element(&ctx->team->task_rbuf, &cur_task_idx);
        if (!cur_task) {
            continue;
        }
        if (cur_task_idx == prev_task_idx) {
            continue;
        }
        switch (ctx->type) {
            case (UCC_TL_SPIN_WORKER_TYPE_TX):
                switch (cur_task->coll_type) {
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_BCAST):
                        status = ucc_tl_spin_coll_worker_tx_bcast_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER):
                        status = ucc_tl_spin_coll_worker_tx_allgather_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_KILL):
                        got_kill_signal = 1;
                        status = UCC_OK;
                        break;
                    default:
                        ucc_assert_always(0);
                }
                cur_task->tx_compls++;
                break;
            case (UCC_TL_SPIN_WORKER_TYPE_RX):
                switch (cur_task->coll_type) {
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_BCAST):
                        status = ucc_tl_spin_coll_worker_rx_bcast_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER):
                        status = ucc_tl_spin_coll_worker_rx_allgather_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_KILL):
                        got_kill_signal = 1;
                        status = UCC_OK;
                        break;
                    default:
                        ucc_assert_always(0);
                }
                cur_task->rx_compls++;
                break;
            default:
                ucc_assert_always(0);
        }
        ucc_assert_always(status == UCC_OK);
        if (got_kill_signal) {
            break;
        }
        prev_task_idx = cur_task_idx;
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread exits", ctx->id);

    return arg;
}