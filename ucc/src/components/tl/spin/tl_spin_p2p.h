#ifndef UCC_TL_SPIN_P2P_H_
#define UCC_TL_SPIN_P2P_H_

#include "tl_spin.h"

ucc_status_t 
ucc_tl_spin_team_rc_ring_barrier(ucc_rank_t rank, ucc_tl_spin_worker_info_t *ctx);
void ib_qp_rc_init_attr(struct ibv_qp_init_attr *attr, struct ibv_cq *cq, uint32_t qp_depth);
ucc_status_t
ib_qp_rc_connect(ucc_base_lib_t *lib, struct ibv_qp *qp, ucc_tl_spin_qp_addr_t *local_addr,
                 ucc_tl_spin_qp_addr_t *remote_addr);
ucc_status_t ib_qp_rc_prepost_empty_rwrs(struct ibv_qp *qp, uint32_t qp_depth);
void ib_qp_rc_prepare_read_swr(struct ibv_send_wr *swr, struct ibv_sge *sgl, size_t sgl_size);
void ib_qp_rc_post_swr(struct ibv_qp *qp, struct ibv_send_wr *swr);
void ib_qp_rc_post_send(struct ibv_qp *qp, struct ibv_mr *mr, void *buf, 
                        uint32_t len, uint32_t imm_data, uint64_t id);
#endif