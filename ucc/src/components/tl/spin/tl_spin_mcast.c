#include "tl_spin_mcast.h"

ucc_status_t
ucc_tl_spin_team_fini_mcg(ucc_tl_spin_context_t *ctx, struct sockaddr_in6 *mcg_addr)
{
    ucc_base_lib_t *lib = UCC_TL_SPIN_CTX_LIB(ctx);
    char            buf[40];
    const char     *dst;

    dst = inet_ntop(AF_INET6, &mcg_addr->sin6_addr, buf, 40);
    if (NULL == dst) {
        tl_error(lib, "inet_ntop failed");
        return UCC_ERR_NO_RESOURCE;
    }

    tl_debug(lib, "mcast leave: ctx %p, buf: %s", ctx, buf);

    if (rdma_leave_multicast(ctx->mcast.id, (struct sockaddr*)mcg_addr)) {
        tl_error(lib, "mcast rdma_leave_multicast failed");
        return UCC_ERR_NO_RESOURCE;
    }

    return UCC_OK;
}

ucc_status_t ucc_tl_spin_mcast_join_mcast_post(ucc_tl_spin_context_t *ctx,
                                               struct sockaddr_in6 *net_addr,
                                               int is_root)
{
    ucc_base_lib_t *lib = UCC_TL_SPIN_CTX_LIB(ctx);
    char            buf[40];
    const char     *dst;

    dst = inet_ntop(AF_INET6, &net_addr->sin6_addr, buf, 40);
    if (NULL == dst) {
        tl_error(lib, "inet_ntop failed");
        return UCC_ERR_NO_RESOURCE;
    }

    tl_debug(lib, "joining addr: %s is_root %d", buf, is_root);

    if (rdma_join_multicast(ctx->mcast.id, (struct sockaddr*)net_addr, NULL)) {
        tl_error(lib, "rdma_join_multicast failed errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    return UCC_OK;
}

ucc_status_t ucc_tl_spin_mcast_join_mcast_test(ucc_tl_spin_context_t *ctx,
                                               struct rdma_cm_event **event,
                                               int is_root,
                                               int is_blocking)
{
    ucc_base_lib_t *lib      = UCC_TL_SPIN_CTX_LIB(ctx);
    char            buf[40];
    const char     *dst;

get_cm_event:
    if (rdma_get_cm_event(ctx->mcast.channel, event) < 0) {
        if (EINTR != errno) {
            tl_error(lib, "rdma_get_cm_event failed, errno %d %s",
                     errno, strerror(errno));
            return UCC_ERR_NO_RESOURCE;
        } else {
            if (is_blocking) {
                goto get_cm_event;
            }
            return UCC_INPROGRESS;
        }
    }

    if (RDMA_CM_EVENT_MULTICAST_JOIN != (*event)->event) {
        tl_error(lib, "failed to join multicast, is_root %d. unexpected event was"
                 " received: event=%d, str=%s, status=%d",
                 is_root, (*event)->event, rdma_event_str((*event)->event),
                 (*event)->status);
        if (rdma_ack_cm_event(*event) < 0) {
            tl_error(lib, "rdma_ack_cm_event failed");
        }
        return UCC_ERR_NO_RESOURCE;
    }

    dst = inet_ntop(AF_INET6, (*event)->param.ud.ah_attr.grh.dgid.raw, buf, 40);
    if (NULL == dst) {
        tl_error(lib, "inet_ntop failed");
        return UCC_ERR_NO_RESOURCE;
    }

    tl_debug(lib, "is_root %d: joined dgid: %s, mlid 0x%x, sl %d", is_root, buf,
             (*event)->param.ud.ah_attr.dlid, (*event)->param.ud.ah_attr.sl);

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_team_join_mcg(ucc_tl_spin_context_t *ctx, struct sockaddr_in6 *mcg_saddr, 
                           ucc_tl_spin_mcast_join_info_t *info, int is_root)
{
    ucc_base_lib_t       *lib      = UCC_TL_SPIN_CTX_LIB(ctx);
    struct rdma_cm_event *cm_event;

    info->status = ucc_tl_spin_mcast_join_mcast_post(ctx, mcg_saddr, is_root);
    if (info->status != UCC_OK) {
        tl_error(lib, "unable to join mcast group error %d", info->status);
        goto ret;
    }

    /* it is time to wait for the rdma event to confirm the join */
    info->status = ucc_tl_spin_mcast_join_mcast_test(ctx, &cm_event, is_root, 1); // TODO: make nonblocking
    if ((info->status == UCC_OK)) {
        ucc_assert(cm_event);
        info->mcg_addr.gid = cm_event->param.ud.ah_attr.grh.dgid;
        info->mcg_addr.lid = cm_event->param.ud.ah_attr.dlid;

    }

    if (info->status == UCC_OK) {
        usleep(100000);
    }

    if (cm_event) {
        rdma_ack_cm_event(cm_event);
    }
ret:
    return info->status;
}

static
ucc_status_t ucc_tl_spin_mcast_create_ah(ucc_tl_spin_context_t *ctx,
                                         ucc_tl_spin_worker_info_t *worker,
                                         ucc_tl_spin_mcast_join_info_t *mcg_info,
                                         int qp_id)
{
    struct ibv_ah_attr ah_attr = {
        .is_global     = 1,
        .grh           = {.sgid_index = 0},
        .dlid          = mcg_info->mcg_addr.lid,
        .sl            = DEF_SL,
        .src_path_bits = DEF_SRC_PATH_BITS,
        .port_num      = ctx->ib_port
    };

    memcpy(ah_attr.grh.dgid.raw,
           mcg_info->mcg_addr.gid.raw,
           sizeof(ah_attr.grh.dgid.raw));

    worker->ahs[qp_id] = ibv_create_ah(ctx->mcast.pd, &ah_attr);
    if (!worker->ahs[qp_id]) {
        tl_error(UCC_TL_SPIN_CTX_LIB(ctx), "failed to create AH");
        return UCC_ERR_NO_RESOURCE;
    }
    return UCC_OK;
}

static ucc_status_t ucc_tl_spin_mcast_connect_qp(ucc_tl_spin_context_t *ctx,
                                                 ucc_tl_spin_worker_info_t *worker,
                                                 ucc_tl_spin_mcast_join_info_t *mcg_info,
                                                 int qp_id)
{
    ucc_base_lib_t      *lib = UCC_TL_SPIN_CTX_LIB(ctx);
    struct ibv_port_attr port_attr;
    struct ibv_qp_attr   attr;
    uint16_t             pkey;
    int                  pkey_index;

    ibv_query_port(ctx->mcast.dev, ctx->ib_port, &port_attr);

    for (pkey_index = 0; pkey_index < port_attr.pkey_tbl_len; pkey_index++) {
        ibv_query_pkey(ctx->mcast.dev, ctx->ib_port, pkey_index, &pkey);
        if (pkey == DEF_PKEY) {
            break;
        }
    }

    if (pkey_index >= port_attr.pkey_tbl_len) {
        pkey_index = 0;
        ibv_query_pkey(ctx->mcast.dev, ctx->ib_port, pkey_index, &pkey);
        if (!pkey) {
            tl_error(lib, "cannot find valid PKEY");
            return UCC_ERR_NO_RESOURCE;
        }

        tl_debug(lib, "cannot find default pkey 0x%04x on port %d, using "
                 "index 0 pkey:0x%04x", DEF_PKEY, ctx->ib_port, pkey);
    }

    attr.qp_state   = IBV_QPS_INIT;
    attr.pkey_index = pkey_index;
    attr.port_num   = ctx->ib_port;
    attr.qkey       = DEF_QKEY;

    if (ibv_modify_qp(worker->qps[qp_id], &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
        tl_error(lib, "failed to move mcast qp to INIT, errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    if (ibv_attach_mcast(worker->qps[qp_id],
                         &mcg_info->mcg_addr.gid,
                         mcg_info->mcg_addr.lid)) {
        tl_error(lib, "failed to attach QP to the mcast group, errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(worker->qps[qp_id], &attr, IBV_QP_STATE)) {
        tl_error(lib, "failed to modify QP to RTR, errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn   = DEF_PSN;
    if (ibv_modify_qp(worker->qps[qp_id], &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
        tl_error(lib, "failed to modify QP to RTS, errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    if (ucc_tl_spin_mcast_create_ah(ctx, worker, mcg_info, qp_id) != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    }

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_team_setup_mcast_qp(ucc_tl_spin_context_t *ctx,
                                ucc_tl_spin_worker_info_t *worker,
                                ucc_tl_spin_mcast_join_info_t *mcg_info,
                                int is_tx_qp, int qp_id)
{
    ucc_base_lib_t         *lib          = UCC_TL_SPIN_CTX_LIB(ctx);
    struct ibv_qp_init_attr qp_init_attr = {0};
    
    qp_init_attr.qp_type             = IBV_QPT_UD;
    qp_init_attr.send_cq             = worker->cq;
    qp_init_attr.recv_cq             = worker->cq;
    qp_init_attr.sq_sig_all          = 0;
    qp_init_attr.cap.max_send_wr     = is_tx_qp  ? ctx->cfg.mcast_sq_depth : 0;
    qp_init_attr.cap.max_recv_wr     = !is_tx_qp ? ctx->cfg.mcast_rq_depth : 0;
    //qp_init_attr.cap.max_inline_data = sr_inline;
    qp_init_attr.cap.max_send_sge    = 1;
    qp_init_attr.cap.max_recv_sge    = 2;

    worker->qps[qp_id] = ibv_create_qp(ctx->mcast.pd, &qp_init_attr);
    if (!worker->qps[qp_id]) {
        tl_error(lib, "failed to create mcast qp, errno %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }

    return ucc_tl_spin_mcast_connect_qp(ctx, worker, mcg_info, qp_id);
}

ucc_status_t 
ucc_tl_spin_team_prepost_mcast_qp(ucc_tl_spin_context_t *ctx,
                                  ucc_tl_spin_worker_info_t *worker,
                                  int qp_id)
{
    struct ibv_qp *qp       = worker->qps[qp_id];
    int            i;

    for (i = 0; i < ctx->cfg.mcast_rq_depth; i++) {
        ib_qp_post_recv_wr(qp, &worker->rwrs[qp_id][i]);
    }

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_prepare_mcg_rwrs(struct ibv_recv_wr *wrs, struct ibv_sge *sges,
                             char *grh_buf, struct ibv_mr *grh_buf_mr,
                             char *staging_rbuf, struct ibv_mr *staging_rbuf_mr,
                             size_t mtu, size_t qp_depth, uint64_t wr_id)
{
    int i, j;

    for (i = 0, j = 0; i < qp_depth; i++, j += 2) {
        memset(&sges[j], 0, 2 * sizeof(sges[j]));

        // GRH
        sges[j].addr   = (uint64_t)grh_buf;
        sges[j].lkey   = grh_buf_mr->lkey;
        sges[j].length = 40;
        grh_buf += UCC_TL_SPIN_IB_GRH_FOOTPRINT;

        // Payload
        sges[j + 1].addr   = (uint64_t)staging_rbuf;
        sges[j + 1].lkey   = staging_rbuf_mr->lkey;
        sges[j + 1].length = mtu;
        staging_rbuf += mtu;

        memset(&wrs[i], 0, sizeof(struct ibv_recv_wr));
        wrs[i].sg_list = &sges[j];
        wrs[i].num_sge = 2;
        wrs[i].wr_id   = wr_id;
    }

    return UCC_OK;
}

inline void ib_qp_post_recv_wr(struct ibv_qp *qp, struct ibv_recv_wr *wr)
{
    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(qp, wr, &bad_wr)) {
        ucc_error("failed to post recv request, errno: %d", errno);
        return;
    }
}

inline void
ib_qp_post_recv(struct ibv_qp *qp, struct ibv_mr *mr,
                void *buf, uint32_t len, uint64_t wr_id)
{
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr;

    if (len) {
        memset(&sg, 0, sizeof(sg));
        sg.addr	  = (uint64_t)buf;
        sg.length = len;
        sg.lkey	  = mr->lkey;
    }

    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = wr_id;
    wr.sg_list    = len ? &sg : NULL;
    wr.num_sge    = len ? 1 : 0;

    if (ibv_post_recv(qp, &wr, &bad_wr)) {
        ucc_error("failed to post recv request, bufsize=%d, errno: %d", len, errno);
        ucc_assert_always(0);
    }
}

inline void
ib_qp_ud_post_mcast_send(struct ibv_qp *qp, struct ibv_ah *ah, struct ibv_send_wr *wr,
                         struct ibv_mr *mr, void *buf, uint32_t len, 
                         uint32_t imm_data, uint64_t wr_id)
{
    struct ibv_sge sg;
    struct ibv_send_wr *bad_wr;

    memset(&sg, 0, sizeof(sg));
    sg.addr	  = (uintptr_t)buf;
    sg.length = len;
    sg.lkey	  = mr->lkey;

    memset(wr, 0, sizeof(*wr));
    wr->imm_data   = imm_data;
    wr->wr_id      = wr_id;
    wr->sg_list    = &sg;
    wr->num_sge    = 1;
    wr->opcode     = IBV_WR_SEND_WITH_IMM;
    wr->send_flags = IBV_SEND_SIGNALED;

    wr->wr.ud.ah          = ah;
    wr->wr.ud.remote_qpn  = 0xFFFFFF;
    wr->wr.ud.remote_qkey = DEF_QKEY;

    if (ibv_post_send(qp, wr, &bad_wr)) {
        ucc_error("failed to post multicast send request, errno: %d", errno);
    }
}

inline void
ib_qp_ud_post_mcast_send_batch(struct ibv_qp *qp, struct ibv_ah *ah, 
                               struct ibv_send_wr *wrs, struct ibv_sge *sges,
                               struct ibv_mr *mr, void *buf, uint32_t len, 
                               size_t batch_size, 
                               ucc_tl_spin_packed_chunk_id_t start_chunk,
                               uint64_t wr_id)
{
    struct ibv_send_wr *bad_wr;
    int i;

    for (i = 0; i < batch_size; i++, buf += len, start_chunk.chunk_metadata.chunk_id++) {
        memset(&sges[i], 0, sizeof(struct ibv_sge));
        sges[i].addr   = (uintptr_t)buf;
        sges[i].length = len;
        sges[i].lkey   = mr->lkey;

        memset(&wrs[i], 0, sizeof(struct ibv_send_wr));
        wrs[i].imm_data = start_chunk.imm_data;
        wrs[i].wr_id    = wr_id;
        wrs[i].next     = (i == (batch_size - 1)) ? NULL : &wrs[i + 1];
        wrs[i].sg_list  = &sges[i];
        wrs[i].num_sge  = 1;
        wrs[i].opcode   = IBV_WR_SEND_WITH_IMM;

        wrs[i].wr.ud.ah          = ah;
        wrs[i].wr.ud.remote_qpn  = 0xFFFFFF;
        wrs[i].wr.ud.remote_qkey = DEF_QKEY;
    }
    wrs[batch_size - 1].send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(qp, wrs, &bad_wr)) {
        ucc_error("failed to post multicast send request, errno: %d", errno);
    }
}

int ib_cq_poll(struct ibv_cq *cq, int max_batch_size, struct ibv_wc *wcs) {
    int i, ncomp = 0;

    do {
        ncomp = ibv_poll_cq(cq, max_batch_size, wcs);
    } while (ncomp == 0);

    if (ncomp < 0) {
        ucc_error("poll_cq err=%d errno=%d", ncomp, errno);
    }

    for (i = 0; i < ncomp; i++) {
        if (wcs[i].status != IBV_WC_SUCCESS) {
            ucc_error("WCE err=%d", wcs[i].status);
        }
    }

    return ncomp;
}

int ib_cq_try_poll(struct ibv_cq *cq, int max_batch_size, struct ibv_wc *wcs) {
    int i, ncomp = 0;

    ncomp = ibv_poll_cq(cq, max_batch_size, wcs);

    if (ncomp < 0) {
        ucc_error("poll_cq err=%d errno=%d", ncomp, errno);
    }

    if (ncomp > 0) {
        for (i = 0; i < ncomp; i++) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                ucc_error("WCE err=%d", wcs[i].status);
            }
        }
    }

    return ncomp;
}