#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"

ucc_status_t
ucc_tl_spin_team_rc_ring_barrier(ucc_rank_t rank, ucc_tl_spin_worker_info_t *ctx)
{
    int comps;
    struct ibv_wc wc[1];

    // ring pass 1
    if (rank == 0) {
        ib_qp_rc_post_send(ctx->qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, 0, 0);
        comps = ib_cq_poll(ctx->cq, 1, wc); // send to the right neighbor completed
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctx->qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
    } else {
        comps = ib_cq_poll(ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctx->qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        ib_qp_rc_post_send(ctx->qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
    }

    // ring pass 2
    if (rank == 0) {
        ib_qp_rc_post_send(ctx->qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, 0, 0);
        comps = ib_cq_poll(ctx->cq, 1, wc); // send to the right neighbor completed
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctx->qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
    } else {
        comps = ib_cq_poll(ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctx->qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        ib_qp_rc_post_send(ctx->qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
    }

    return UCC_OK;
}

void ib_qp_rc_init_attr(struct ibv_qp_init_attr *attr,
                        struct ibv_cq *cq,
                        uint32_t qp_depth)
{
    memset(attr, 0, sizeof(*attr));
    attr->qp_type          = IBV_QPT_RC;
    attr->sq_sig_all       = 1;
    attr->send_cq          = cq;
    attr->recv_cq          = cq;
    attr->cap.max_send_wr  = qp_depth;
    attr->cap.max_recv_wr  = qp_depth;
    attr->cap.max_send_sge = 1;
    attr->cap.max_recv_sge = 1;
    //attr.cap.max_inline_data = 128;
}

ucc_status_t
ib_qp_rc_connect(ucc_base_lib_t *lib, struct ibv_qp *qp, 
                 ucc_tl_spin_qp_addr_t *local_addr,
                 ucc_tl_spin_qp_addr_t *remote_addr)
{
    ucc_status_t       status = UCC_OK;
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
	attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = UCC_TL_SPIN_DEFAULT_PKEY;
    attr.port_num        = local_addr->dev_addr.port_num;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE   |
                           IBV_ACCESS_REMOTE_WRITE  |
                           IBV_ACCESS_REMOTE_ATOMIC |
                           IBV_ACCESS_REMOTE_READ;
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      IBV_QP_ACCESS_FLAGS),
                        status, UCC_ERR_NO_RESOURCE, ret);

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state                  = IBV_QPS_RTR;
    attr.path_mtu		           = local_addr->dev_addr.mtu;
    attr.dest_qp_num	           = remote_addr->qpn;
    attr.rq_psn                    = 0;
    attr.max_dest_rd_atomic        = 16;
    attr.min_rnr_timer             = 30;
    attr.ah_attr.is_global         = 0;
    //attr.ah_attr.grh.flow_label    = 0;
    //attr.ah_attr.grh.hop_limit     = 1;
    //attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.dlid              = remote_addr->dev_addr.lid;
    attr.ah_attr.sl	     	       = 0;
    attr.ah_attr.src_path_bits     = 0;
    attr.ah_attr.port_num	       = local_addr->dev_addr.port_num;
    //attr.ah_attr.grh.dgid          = remote_addr.dev_addr.gid;
    //attr.ah_attr.grh.sgid_index    = remote_addr.dev_addr.gid_table_index;
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE    | IBV_QP_AV     | IBV_QP_PATH_MTU |
                                      IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                                      IBV_QP_MIN_RNR_TIMER), 
                        status, UCC_ERR_NO_RESOURCE, ret);

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.sq_psn        = 0;
    attr.timeout       = 0;
    attr.retry_cnt     = 0; //rnr_retry ? 7 : 0;
    attr.rnr_retry     = 0; //rnr_retry ? 7 : 0;
    attr.max_rd_atomic = 16; //ok
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE     | IBV_QP_SQ_PSN    | IBV_QP_TIMEOUT |
                                      IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC),
                        status, UCC_ERR_NO_RESOURCE, ret);

ret:
    return status;
}

ucc_status_t ib_qp_rc_prepost_empty_rwrs(struct ibv_qp *qp, uint32_t qp_depth)
{
    int            i;

    for (i = 0; i < qp_depth; i++) {
        ib_qp_post_recv(qp, NULL, NULL, 0, 0);
    }

    return UCC_OK;
}

void ib_qp_rc_prepare_read_swr(struct ibv_send_wr *swr, struct ibv_sge *sgl, size_t sgl_size)
{
    memset(sgl, 0, sizeof(*sgl) * sgl_size);
    memset(swr, 0, sizeof(*swr));
    swr->wr_id      = 0;
    swr->sg_list    = sgl;
    swr->num_sge    = sgl_size;
    swr->opcode     = IBV_WR_RDMA_READ;
}

void ib_qp_rc_post_swr(struct ibv_qp *qp, struct ibv_send_wr *swr)
{
    struct ibv_send_wr *bad_wr;

    if (ibv_post_send(qp, swr, &bad_wr)) {
        ucc_error("failed to post rc send request, errno: %d", errno);
    }
}

void ib_qp_rc_post_send(struct ibv_qp *qp, struct ibv_mr *mr, void *buf, 
                        uint32_t len, uint32_t imm_data, uint64_t id)
{
    struct ibv_sge      sg;
    struct ibv_send_wr  wr;
    struct ibv_send_wr *bad_wr;

    if (len > 0) {
        memset(&sg, 0, sizeof(sg));
        sg.addr	  = (uintptr_t)buf;
        sg.length = len;
        sg.lkey	  = mr->lkey;
    }

    memset(&wr, 0, sizeof(wr));
    wr.imm_data   = imm_data;
    wr.wr_id      = id;
    wr.sg_list    = len > 0 ? &sg : NULL;
    wr.num_sge    = len > 0 ? 1   : 0;
    wr.opcode     = IBV_WR_SEND_WITH_IMM;

    if (ibv_post_send(qp, &wr, &bad_wr)) {
        ucc_error("failed to post rc send request, errno: %d", errno);
    }

    ucc_debug("posted rc send request, buf=%p bufsize=%d", buf, len);
}