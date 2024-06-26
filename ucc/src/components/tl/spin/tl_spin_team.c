#include "coll_score/ucc_coll_score.h"
#include "core/ucc_service_coll.h"
#include "core/ucc_team.h"

#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"
#include "tl_spin_bitmap.h"

inline ucc_status_t
ucc_tl_spin_team_service_barrier_post(ucc_tl_spin_team_t *ctx, 
                                      int *barrier_scratch,
                                      ucc_service_coll_req_t **barrier_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = {.map    = UCC_TL_TEAM_MAP(ctx),
                                      .myrank = UCC_TL_TEAM_RANK(ctx)};
    ucc_service_coll_req_t *req    = NULL;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx), "barrier");

    status = ucc_service_allreduce(team, &barrier_scratch[0], &barrier_scratch[1],
                                   UCC_DT_INT32, 1, UCC_OP_SUM, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team barrier failed");
        return status;
    }

    *barrier_req = req;

    return status;
}


static ucc_status_t
ucc_tl_spin_team_service_bcast_post(ucc_tl_spin_team_t *ctx, 
                                    void *buf, size_t size, ucc_rank_t root,
                                    ucc_service_coll_req_t **bcast_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = {.map    = UCC_TL_TEAM_MAP(ctx),
                                      .myrank = UCC_TL_TEAM_RANK(ctx)};
    ucc_service_coll_req_t *req    = NULL;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx), "bcasting");

    status = ucc_service_bcast(team, buf, size, root, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team bcast failed");
        return status;
    }

    *bcast_req = req;

    return status;
}

static ucc_status_t
ucc_tl_spin_team_service_allgather_post(ucc_tl_spin_team_t *ctx, void *sbuf, void *rbuf,
                                        size_t size, ucc_service_coll_req_t **ag_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = {.map    = UCC_TL_TEAM_MAP(ctx),
                                      .myrank = UCC_TL_TEAM_RANK(ctx)};
    ucc_service_coll_req_t *req    = NULL;

    status = ucc_service_allgather(team, sbuf, rbuf, size, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team allgather failed");
        return status;
    }

    *ag_req = req;

    return status;
}

ucc_status_t
ucc_tl_spin_team_service_coll_test(ucc_service_coll_req_t *req, int blocking)
{
    ucc_status_t status = UCC_OK;

test:
    status = ucc_service_coll_test(req);

    if (blocking && (status == UCC_INPROGRESS)) {
        goto test;
    }

    if (UCC_INPROGRESS != status) {
        ucc_service_coll_finalize(req);
    }

    return status;
}

UCC_CLASS_INIT_FUNC(ucc_tl_spin_team_t, ucc_base_context_t *tl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_tl_spin_context_t     *ctx       = ucc_derived_of(tl_context, ucc_tl_spin_context_t);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    size_t                     alignment = sysconf(_SC_PAGESIZE);
    int                        i, j;
    ucc_topo_t                *topo;

    // TODO: support multiple QPs/multicast subgroups per CQ
    ucc_assert_always(ctx->cfg.mcast_sq_depth >= ctx->cfg.mcast_tx_batch_sz);
    ucc_assert_always(ctx->cfg.n_mcg == ctx->cfg.n_tx_workers);
    ucc_assert_always(ctx->cfg.n_mcg == ctx->cfg.n_rx_workers);
    //ucc_assert_always(ctx->cfg.n_mcg % ctx->cfg.n_tx_workers == 0);
    //ucc_assert_always(ctx->cfg.n_mcg % ctx->cfg.n_rx_workers == 0);

    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);

    self->base_team     = params->team;
    self->subset.myrank = params->rank;
    self->size          = params->size;

    status = ucc_ep_map_create_nested(&UCC_TL_CORE_TEAM(self)->ctx_map,
                                      &UCC_TL_TEAM_MAP(self),
                                      &self->subset.map);
    if (UCC_OK != status) {
        tl_debug(tl_context->lib, "failed to create ctx map");
        return status;
    }

    status = ucc_topo_init(self->subset, UCC_TL_CORE_CTX(self)->topo, &topo);
    if (UCC_OK != status) {
        tl_error(tl_context->lib, "failed to init team topo");
        return status;
    }

    if (ucc_topo_max_ppn(topo) > 1) {
        tl_debug(tl_context->lib, "spin team not supported with ppn > 1, min ppn = %zu, max ppn = %zu, team size = %zu", 
                 (size_t)ucc_topo_max_ppn(topo), (size_t)ucc_topo_max_ppn(topo), (size_t)self->size);
        status = UCC_ERR_NOT_SUPPORTED;
        goto cleanup;
    }

    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(n_workers + 1, sizeof(ucc_tl_spin_worker_info_t)),
                        self->workers,
                        status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(ctx->cfg.n_mcg, sizeof(ucc_tl_spin_mcast_join_info_t)),
                        self->mcg_infos,
                        status, UCC_ERR_NO_MEMORY, ret);

    // FIXME in case of error this will leak
    for (i = 0; i < n_workers; i++) {
        worker         = &self->workers[i];
        worker->ctx    = ctx;
        worker->team   = self;
        worker->type   = i < ctx->cfg.n_tx_workers ?
                         UCC_TL_SPIN_WORKER_TYPE_TX :
                         UCC_TL_SPIN_WORKER_TYPE_RX;
        worker->n_mcg = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ?
                         ctx->cfg.n_mcg / ctx->cfg.n_tx_workers :
                         ctx->cfg.n_mcg / ctx->cfg.n_rx_workers;
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcg, sizeof(struct ibv_qp *)),
                            worker->qps, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcg, sizeof(struct ibv_ah *)),
                            worker->ahs, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ibv_create_cq(ctx->mcast.dev, ctx->cfg.mcast_cq_depth, NULL, NULL, 0), 
                            worker->cq, status, UCC_ERR_NO_MEMORY, ret);
        tl_debug(tl_context->lib, "worker %d created cq %p", i, worker->cq);

        if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_recv_wr *)),
                                worker->swrs, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_sge *)),
                                worker->ssges, status, UCC_ERR_NO_MEMORY, ret);
            for (j = 0; j < worker->n_mcg; j++) {
                if (posix_memalign((void **)&worker->swrs[j], alignment, ctx->cfg.mcast_tx_batch_sz * sizeof(struct ibv_send_wr))) {
                    tl_error(tl_context->lib, "allocation of swrs buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                if (posix_memalign((void **)&worker->ssges[j], alignment, ctx->cfg.mcast_tx_batch_sz * sizeof(struct ibv_sge))) {
                    tl_error(tl_context->lib, "allocation of ssges buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
            }
        }

        if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(size_t)),
                                worker->tail_idx, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(char *)),
                                worker->staging_rbuf, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_mr *)),
                                worker->staging_rbuf_mr, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(char *)),
                                worker->grh_buf, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_mr *)),
                                worker->grh_buf_mr, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_recv_wr *)),
                                worker->rwrs, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_sge *)),
                                worker->rsges, status, UCC_ERR_NO_MEMORY, ret);
            
            worker->staging_rbuf_len = ctx->mcast.mtu * ctx->cfg.mcast_rq_depth;
            worker->grh_buf_len      = UCC_TL_SPIN_IB_GRH_FOOTPRINT * ctx->cfg.mcast_rq_depth;
            for (j = 0; j < worker->n_mcg; j++) {
                if (posix_memalign((void **)&worker->staging_rbuf[j], alignment, worker->staging_rbuf_len)) {
                    tl_error(tl_context->lib, "allocation of staging buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                worker->staging_rbuf_mr[j] = ibv_reg_mr(ctx->mcast.pd, worker->staging_rbuf[j], 
                                                        worker->staging_rbuf_len,
                                                        IBV_ACCESS_REMOTE_WRITE |
                                                        IBV_ACCESS_LOCAL_WRITE);
                if (!worker->staging_rbuf_mr[j]) {
                    tl_error(tl_context->lib, "registration of staging buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }

                if (posix_memalign((void **)&worker->grh_buf[j], alignment, worker->grh_buf_len)) {
                    tl_error(tl_context->lib, "allocation of ghr buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                worker->grh_buf_mr[j] = ibv_reg_mr(ctx->mcast.pd, worker->grh_buf[j], 
                                                   worker->grh_buf_len,
                                                   IBV_ACCESS_REMOTE_WRITE |
                                                   IBV_ACCESS_LOCAL_WRITE);
                if (!worker->grh_buf_mr[j]) {
                    tl_error(tl_context->lib, "registration of ghr buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                
                if (posix_memalign((void **)&worker->rwrs[j], alignment, ctx->cfg.mcast_rq_depth * sizeof(struct ibv_recv_wr))) {
                    tl_error(tl_context->lib, "allocation of swrs buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                if (posix_memalign((void **)&worker->rsges[j], alignment, ctx->cfg.mcast_rq_depth * 2 * sizeof(struct ibv_sge))) {
                    tl_error(tl_context->lib, "allocation of ssges buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                status = ucc_tl_spin_prepare_mcg_rwrs(worker->rwrs[j], worker->rsges[j],
                                                      worker->grh_buf[j], worker->grh_buf_mr[j],
                                                      worker->staging_rbuf[j], worker->staging_rbuf_mr[j],
                                                      ctx->mcast.mtu, ctx->cfg.mcast_rq_depth, j);
                ucc_assert_always(status == UCC_OK);
                worker->tail_idx[j] = 0;
            }

            memset(&worker->reliability, 0, sizeof(worker->reliability));
            worker->reliability.bitmap.size = ucc_tl_spin_get_bitmap_size(ctx->cfg.max_recv_buf_size,  ctx->mcast.mtu);
            tl_debug(tl_context->lib, "bitmap size is %zu\n", worker->reliability.bitmap.size);
            if (posix_memalign((void **)&worker->reliability.bitmap.buf, alignment, worker->reliability.bitmap.size)) {
                tl_error(tl_context->lib, "allocation of bitmap buffer failed");
                return UCC_ERR_NO_MEMORY;
            }
            ucc_tl_spin_bitmap_cleanup(&worker->reliability.bitmap);

            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(UCC_TL_TEAM_SIZE(self), sizeof(ucc_rank_t)),
                                worker->reliability.missing_ranks, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(UCC_TL_TEAM_SIZE(self), sizeof(size_t)),
                                worker->reliability.recvd_per_rank, status, UCC_ERR_NO_MEMORY, ret);

            if (posix_memalign((void **)&worker->reliability.ln_rbuf_info, alignment, sizeof(ucc_tl_spin_buf_info_t))) {
                    tl_error(tl_context->lib, "allocation of ln_rbuf_info buffer failed");
                    return UCC_ERR_NO_MEMORY;
            }
            worker->reliability.ln_rbuf_info_mr = ibv_reg_mr(ctx->p2p.pd, worker->reliability.ln_rbuf_info, 
                                                             sizeof(ucc_tl_spin_buf_info_t),
                                                             IBV_ACCESS_REMOTE_WRITE |
                                                             IBV_ACCESS_LOCAL_WRITE  |
                                                             IBV_ACCESS_REMOTE_READ);
            if (!worker->reliability.ln_rbuf_info_mr) {
                tl_error(tl_context->lib, "registration of ln_rbuf_info failed");
                return UCC_ERR_NO_MEMORY;
            }
            if (posix_memalign((void **)&worker->reliability.rn_rbuf_info, alignment, sizeof(ucc_tl_spin_buf_info_t))) {
                    tl_error(tl_context->lib, "allocation of rn_rbuf_info buffer failed");
                    return UCC_ERR_NO_MEMORY;
            }
            worker->reliability.rn_rbuf_info_mr = ibv_reg_mr(ctx->p2p.pd, worker->reliability.rn_rbuf_info, 
                                                             sizeof(ucc_tl_spin_buf_info_t),
                                                             IBV_ACCESS_REMOTE_WRITE |
                                                             IBV_ACCESS_LOCAL_WRITE  |
                                                             IBV_ACCESS_REMOTE_READ);
            if (!worker->reliability.rn_rbuf_info_mr) {
                tl_error(tl_context->lib, "registration of rn_rbuf_info failed");
                return UCC_ERR_NO_MEMORY;
            }
        }
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ibv_create_cq(ctx->p2p.dev, ctx->cfg.p2p_cq_depth, NULL, NULL, 0), 
                            worker->reliability.cq,
                            status, UCC_ERR_NO_MEMORY, ret);
    }

    // construct p2p worker
    self->ctrl_ctx       = &self->workers[n_workers];
    self->ctrl_ctx->type = UCC_TL_SPIN_WORKER_TYPE_CTRL;
    UCC_TL_SPIN_CHK_PTR(tl_context->lib, ucc_calloc(2, sizeof(struct ibv_qp *)),
                        self->ctrl_ctx->qps, status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ibv_create_cq(ctx->p2p.dev, ctx->cfg.p2p_cq_depth, NULL, NULL, 0), 
                        self->ctrl_ctx->cq,
                        status, UCC_ERR_NO_MEMORY, ret);
    if (posix_memalign((void **)&self->ctrl_ctx->barrier_scratch, alignment, sizeof(int) * 2)) {
                    tl_error(tl_context->lib, "allocation of barrier scratch buffer failed");
                    return UCC_ERR_NO_MEMORY;
    }

    tl_info(tl_context->lib, "posted tl team: %p, n threads: %d", self, n_workers);

cleanup:
    ucc_topo_cleanup(topo);
    ucc_ep_map_destroy_nested(&self->subset.map);
ret:
    return status;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_team_t)
{
    ucc_tl_spin_lib_t         *lib       = UCC_TL_SPIN_TEAM_LIB(self);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(self);
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    ucc_tl_spin_worker_info_t *worker;
    int i, j, mcg_id = 0;

    if (self->ctrl_ctx->barrier_scratch) {
        free(self->ctrl_ctx->barrier_scratch);
    }
    if (self->ctrl_ctx->qps) {
        if (ibv_destroy_qp(self->ctrl_ctx->qps[0])) {
            tl_error(lib, "ctrl ctx failed to destroy qp 0, errno: %d", errno);
        }
        if (ibv_destroy_qp(self->ctrl_ctx->qps[1])) {
            tl_error(lib, "ctrl ctx failed to destroy qp 1, errno: %d", errno);
        }
        ucc_free(self->ctrl_ctx->qps);
    }

    if (self->ctrl_ctx->cq) {
        if (ibv_destroy_cq(self->ctrl_ctx->cq)) {
            tl_error(lib, "ctrl ctx failed to destroy cq, errno: %d", errno);
        }
    }

    ucc_tl_spin_coll_post_kill_task(self);

    for (i = 0; i < n_workers; i++) {
        worker = &self->workers[i];
        if (pthread_join(worker->pthread, NULL)) {
            tl_error(lib, "worker %d thread joined with error", i);
        }
        if (worker->qps) {
            for (j = 0; j < worker->n_mcg; j++) {
                if (ibv_detach_mcast(worker->qps[j], &self->mcg_infos[mcg_id].mcg_addr.gid, self->mcg_infos[mcg_id].mcg_addr.lid)) {
                    tl_error(lib, "worker %d failed to detach qp[%d] from mcg[%d], errno: %d", i, j, mcg_id, errno);
                }
                tl_debug(lib, "worker %d destroys qp %d %p", i, j, worker->qps[j]);
                if (ibv_destroy_qp(worker->qps[j])) {
                    tl_error(lib, "worker %d failed to destroy qp[%d], errno: %d", i, j, errno);
                }
                if (ibv_destroy_ah(worker->ahs[j])) {
                    tl_error(lib, "worker %d failed to destroy ah[%d], errno: %d", i, j, errno);
                }
                if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                    if (ibv_dereg_mr(worker->staging_rbuf_mr[j])) {
                        tl_error(lib, "worker %d failed to destroy staging_rbuf_mr[%d], errno: %d", i, j, errno);
                    }
                    free(worker->staging_rbuf[j]);
                    if (ibv_dereg_mr(worker->grh_buf_mr[j])) {
                        tl_error(lib, "worker %d failed to destroy grh_buf_mr[%d], errno: %d", i, j, errno);
                    }
                    free(worker->grh_buf[j]);
                }
                if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
                    ucc_free(worker->swrs[j]);
                    ucc_free(worker->ssges[j]);
                }
            }
            ucc_free(worker->qps);
            ucc_free(worker->ahs);
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
                    ucc_free(worker->swrs);
                    ucc_free(worker->ssges);
            }
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                ucc_free(worker->staging_rbuf);
                ucc_free(worker->staging_rbuf_mr);
                ucc_free(worker->grh_buf);
                ucc_free(worker->grh_buf_mr);
                ucc_free(worker->tail_idx);
                ucc_free(worker->reliability.bitmap.buf);
                ucc_free(worker->reliability.missing_ranks);
                ucc_free(worker->reliability.recvd_per_rank);
                if (ibv_dereg_mr(worker->reliability.ln_rbuf_info_mr)) {
                    tl_error(lib, "worker %d failed to destroy ln_rbuf_info_mr, errno: %d", i, errno);
                }
                if (ibv_dereg_mr(worker->reliability.rn_rbuf_info_mr)) {
                    tl_error(lib, "worker %d failed to destroy ln_rbuf_info_mr, errno: %d", i, errno);
                }
                ucc_free(worker->reliability.ln_rbuf_info);
                ucc_free(worker->reliability.rn_rbuf_info);
            }
            if (ibv_destroy_qp(worker->reliability.qps[UCC_TL_SPIN_LN_QP_ID])) {
                tl_error(lib, "worker %d failed to destroy reliability qp 0, errno: %d", i, errno);
            }
            if (ibv_destroy_qp(worker->reliability.qps[UCC_TL_SPIN_RN_QP_ID])) {
                tl_error(lib, "worker %d failed to destroy reliability qp 1, errno: %d", i, errno);
            }
            if (ibv_destroy_cq(worker->reliability.cq)) {
                tl_error(lib, "worker %d failed to reliability destroy cq, errno: %d", i, errno);
            }
            mcg_id = (mcg_id + 1) % ctx->cfg.n_mcg;
        }
        if (worker->cq) {
            tl_debug(lib, "worker %d destroys cq %p", i, worker->cq);
            if (ibv_destroy_cq(worker->cq)) {
                tl_error(lib, "worker %d failed to destroy cq, errno: %d", i, errno);
            }
        }
    }

    ucc_tl_spin_coll_kill_task_finalize(self);

    ucc_free(self->workers);

    for (i = 0; i < ctx->cfg.n_mcg; i++) {
        ucc_tl_spin_team_fini_mcg(ctx, &self->mcg_infos[i].saddr);
    }
    ucc_free(self->mcg_infos);

    tl_info(self->super.super.context->lib, "finalizing tl team: %p", self);
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_tl_spin_team_t, ucc_tl_team_t);

ucc_status_t ucc_tl_spin_team_destroy(ucc_base_team_t *tl_team)
{
    UCC_CLASS_DELETE_FUNC_NAME(ucc_tl_spin_team_t)(tl_team);
    return UCC_OK;
}

static ucc_status_t 
ucc_tl_spin_team_init_rc_qp_ring(ucc_base_team_t *tl_team, struct ibv_cq *cq, struct ibv_qp **qps, int prepost)
{
    ucc_status_t               status      = UCC_OK;
    ucc_tl_spin_team_t        *team        = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib         = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_qp_addr_t     *recv_av     = NULL;
    ucc_tl_spin_qp_addr_t      send_av[2];
    ucc_rank_t                 l_neighbor, r_neighbor;
    struct ibv_qp_init_attr    qp_init_attr;
    ucc_service_coll_req_t    *req;
    int i;

    ib_qp_rc_init_attr(&qp_init_attr, cq, ctx->cfg.p2p_qp_depth);
    for (i = 0; i < 2; i++) {
        UCC_TL_SPIN_CHK_PTR(lib,
                            ibv_create_qp(ctx->p2p.pd, &qp_init_attr), qps[i],
                            status, UCC_ERR_NO_RESOURCE, ret);
        send_av[i].dev_addr = ctx->p2p.dev_addr;
        send_av[i].qpn      = qps[i]->qp_num;
    }

    // Collect team address vector (e.g., rank-to-qp-address mapping)
    UCC_TL_SPIN_CHK_PTR(lib,
                        ucc_calloc(team->size, sizeof(send_av)), recv_av,
                        status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_ERR(lib,
                        ucc_tl_spin_team_service_allgather_post(team,
                                                                (void *)send_av,
                                                                (void *)recv_av,
                                                                sizeof(send_av),
                                                                &req),
                        status, UCC_ERR_NO_RESOURCE, ret);
    UCC_TL_SPIN_CHK_ERR(lib, // TODO: remove blocking behaviour
                        ucc_tl_spin_team_service_coll_test(req, 1),
                        status, UCC_ERR_NO_RESOURCE, ret);

    // Connect QPs in a virtual ring
    l_neighbor = (team->subset.myrank + 1)              % team->size;
    r_neighbor = (team->subset.myrank - 1 + team->size) % team->size;
    assert(team_size == 2);
    status = ib_qp_rc_connect(lib, qps[UCC_TL_SPIN_LN_QP_ID], &send_av[0], &recv_av[l_neighbor * 2 + 1]);
    ucc_assert_always(status == UCC_OK);
    status = ib_qp_rc_connect(lib, qps[UCC_TL_SPIN_RN_QP_ID], &send_av[1], &recv_av[r_neighbor * 2]);
    ucc_assert_always(status == UCC_OK);
    if (prepost) {
        status = ib_qp_rc_prepost_empty_rwrs(qps[UCC_TL_SPIN_LN_QP_ID], ctx->cfg.p2p_qp_depth);
        ucc_assert_always(status == UCC_OK);
        status = ib_qp_rc_prepost_empty_rwrs(qps[UCC_TL_SPIN_RN_QP_ID], ctx->cfg.p2p_qp_depth);
        ucc_assert_always(status == UCC_OK);
    }

    tl_debug(lib,
             "connected p2p context of rank %d, "
             "left neighbor rank %d (lid:qpn): %d:%d to %d:%d, "
             "right neighbor rank %d (lid:qpn): %d:%d to %d:%d",
             team->subset.myrank,
             l_neighbor, send_av[0].qpn, send_av[0].dev_addr.lid, 
             recv_av[l_neighbor * 2 + 1].qpn, recv_av[l_neighbor * 2 + 1].dev_addr.lid,
             r_neighbor, send_av[1].qpn, send_av[1].dev_addr.lid,
             recv_av[r_neighbor * 2].qpn, recv_av[r_neighbor * 2].dev_addr.lid);

    ucc_free(recv_av);
ret:
    return status;
}

static ucc_status_t ucc_tl_spin_team_prepare_mcg(ucc_base_team_t *tl_team,
                                                  ucc_tl_spin_mcast_join_info_t *worker_mcg_info,
                                                  unsigned int mcg_gid)
{
    ucc_tl_spin_team_t            *team       = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t         *ctx        = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t                *lib        = UCC_TL_SPIN_CTX_LIB(ctx);
    ucc_status_t                   status     = UCC_OK;
    struct sockaddr_in6            mcg_saddr = {.sin6_family = AF_INET6};
    char                           saddr_str[40];
    ucc_tl_spin_mcast_join_info_t  mcg_info;
    ucc_service_coll_req_t        *bcast_req;

    memset(&mcg_info, 0, sizeof(mcg_info));

    if (team->subset.myrank == 0) {                 // root obtains group gid/lid
        ucc_assert_always(mcg_gid < 65536);        // number of mcg'es that can be encoded in the last on ipv6 four-tet
        ucc_assert_always(snprintf(saddr_str, 40, "0:0:0::0:0:%x", mcg_gid) > 0);
        ucc_assert_always(inet_pton(AF_INET6, saddr_str, &(mcg_saddr.sin6_addr)));
        //mcg_saddr.sin6_flowinfo = subgroup_id++; // TODO: enumerate multicast subgroup in IP address
        status = ucc_tl_spin_team_join_mcg(ctx, &mcg_saddr, &mcg_info, 1);
        (void)status;                               // send status to non-root ranks first and then fail
        mcg_info.magic_num = UCC_TL_SPIN_JOIN_MAGICNUM;
    }

    status = ucc_tl_spin_team_service_bcast_post(team,
                                                 &mcg_info,
                                                 sizeof(ucc_tl_spin_mcast_join_info_t), 
                                                 0,
                                                 &bcast_req);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_service_coll_test(bcast_req, 1); // TODO: make non-blocking
    ucc_assert_always(status == UCC_OK);
    if (mcg_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    } else {
        ucc_assert_always(mcg_info.magic_num == UCC_TL_SPIN_JOIN_MAGICNUM);
    }

    memcpy(&mcg_saddr.sin6_addr, mcg_info.mcg_addr.gid.raw, sizeof(mcg_info.mcg_addr.gid.raw));
    status = ucc_tl_spin_team_join_mcg(ctx, &mcg_saddr, &mcg_info, 0);

    if (mcg_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    }

    mcg_info.saddr   = mcg_saddr;
    *worker_mcg_info = mcg_info;

    tl_debug(lib, "team: %p, rank: %d joined multicast group: %d",
             tl_team, team->subset.myrank, mcg_gid);

    return UCC_OK;
}

static ucc_status_t ucc_tl_spin_team_init_mcast_qps(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                        i, j, mcg_id = 0;

    for (i = 0; i < n_workers; i++) {
        worker = &team->workers[i];
        for (j = 0; j < worker->n_mcg; j++) {
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcg_infos[mcg_id], 1, j);
                assert(status == UCC_OK);
            } else {
                ucc_assert(worker->type == UCC_TL_SPIN_WORKER_TYPE_RX);
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcg_infos[mcg_id], 0, j);
                ucc_assert(status == UCC_OK);
                status = ucc_tl_spin_team_prepost_mcast_qp(ctx, worker, j);
                ucc_assert(status == UCC_OK);
            }
            tl_debug(lib, "worker %d qp %d %p attached to cq %p", i, j, worker->qps[j], worker->cq);
            mcg_id = (mcg_id + 1) % ctx->cfg.n_mcg;
        }
    }
    tl_debug(lib, "initialized multicast QPs");

    return status;
}

static ucc_status_t ucc_tl_spin_team_spawn_workers(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                        num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int                        i;
    cpu_set_t                  cpuset;

    for (i = 0; i < n_workers; i++) {
        worker     = &(team->workers[i]);
        worker->id = i % ctx->cfg.n_mcg;
        UCC_TL_SPIN_CHK_ERR(lib,
                            pthread_create(&worker->pthread, NULL, ucc_tl_spin_coll_worker_main, (void *)worker), 
                            status, UCC_ERR_NO_RESOURCE, err);

        /* Round-robin workers pinning */
        if (ctx->cur_core_id > num_cores) {
            ctx->cur_core_id = ctx->cfg.start_core_id;
            tl_warn(lib, "number of workers is larger than number of available cores");
        }
        CPU_ZERO(&cpuset);
        CPU_SET(ctx->cur_core_id, &cpuset);
        UCC_TL_SPIN_CHK_ERR(lib,
                            pthread_setaffinity_np(worker->pthread, sizeof(cpu_set_t), &cpuset),
                            status, UCC_ERR_NO_RESOURCE, err);
        tl_debug(lib, "worker %d is pinned to core %zu", i, ctx->cur_core_id);
        ctx->cur_core_id++;
    }

err:
    return status;
}

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *worker;
    ucc_status_t               status;
    int                        i, j;

    // Create and connect barrier ring of RC QPs
    ucc_tl_spin_team_init_rc_qp_ring(tl_team, team->ctrl_ctx->cq, team->ctrl_ctx->qps, 1);
    // Create syncronization rings between workers
    for (i = 0; i < ctx->cfg.n_rx_workers + ctx->cfg.n_rx_workers; i++) {
        worker = &team->workers[i];
        ucc_tl_spin_team_init_rc_qp_ring(tl_team, worker->reliability.cq, worker->reliability.qps, 0);
        if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
            ib_qp_post_recv(worker->reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
        } else if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
            ib_qp_rc_prepare_read_swr(&worker->reliability.rd_swr, &team->workers[i].reliability.sge, 1);
            for (j = 0; j < ctx->cfg.p2p_qp_depth; j++) {
                ib_qp_post_recv(worker->reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
                ib_qp_post_recv(worker->reliability.qps[UCC_TL_SPIN_LN_QP_ID],
                                worker->reliability.ln_rbuf_info_mr,
                                worker->reliability.ln_rbuf_info,
                                sizeof(ucc_tl_spin_buf_info_t), 0);
            }
        }
    }

    for (i = 0; i < ctx->cfg.n_mcg; i++) {
        status = ucc_tl_spin_team_prepare_mcg(tl_team, &team->mcg_infos[i], ctx->mcast.gid++);
        if (status != UCC_OK) {
            return status;
        }
    }

    status = ucc_tl_spin_team_init_mcast_qps(tl_team);
    if (status != UCC_OK) {
        return status;
    }

    team->task_id = 0;
    rbuf_init(&team->task_rbuf);

    // Spawn worker threads
    ucc_tl_spin_team_spawn_workers(tl_team);

    tl_info(lib, "initialized tl team: %p", team);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_team_get_scores(ucc_base_team_t *tl_team,
                                         ucc_coll_score_t **score_p)
{
    ucc_tl_spin_team_t *team  = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_base_context_t *ctx   = UCC_TL_TEAM_CTX(team);
    ucc_base_lib_t     *lib   = UCC_TL_TEAM_LIB(team);
    ucc_memory_type_t   mt[1] = {UCC_MEMORY_TYPE_HOST};
    ucc_coll_score_t          *score;
    ucc_status_t               status;
    ucc_coll_score_team_info_t team_info;

    team_info.alg_fn              = NULL;
    team_info.default_score       = UCC_TL_SPIN_DEFAULT_SCORE;
    team_info.init                = ucc_tl_spin_coll_init;
    team_info.num_mem_types       = 1;
    team_info.supported_mem_types = mt;
    team_info.supported_colls     = UCC_TL_SPIN_SUPPORTED_COLLS;
    team_info.size                = UCC_TL_TEAM_SIZE(team);

    status = ucc_coll_score_build_default(
        tl_team, UCC_TL_SPIN_DEFAULT_SCORE, ucc_tl_spin_coll_init,
        UCC_TL_SPIN_SUPPORTED_COLLS, mt, 1, &score);
    if (UCC_OK != status) {
        tl_debug(lib, "failed to build score map");
        return status;
    }
    if (strlen(ctx->score_str) > 0) {
        status = ucc_coll_score_update_from_str(ctx->score_str, &team_info,
                                                &team->super.super, score);
        /* If INVALID_PARAM - User provided incorrect input - try to proceed */
        if ((status < 0) && (status != UCC_ERR_INVALID_PARAM) &&
            (status != UCC_ERR_NOT_SUPPORTED)) {
            goto err;
        }
    }

    *score_p = score;
    return UCC_OK;

err:
    ucc_coll_score_free(score);
    *score_p = NULL;
    return status;
}