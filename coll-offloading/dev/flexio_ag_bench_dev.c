#include <libflexio-os/flexio_os.h>
#include <libflexio-os/flexio_os_syscall.h>
#include <libflexio-os/flexio_os_mb.h>
#include <libflexio-dev/flexio_dev_queue_access.h>
#include <libflexio-dev/flexio_dev_endianity.h>
#include <libflexio-dev/flexio_dev.h>

#include <stddef.h>
#include <dpaintrin.h>

#include "../host/flexio_ag_bench_com.hpp"

#include "dpa_profile.h"

#define L2V(l) (1UL << (l))
#define L2M(l) (L2V(l) - 1)
#define FAST_LOG2(x) (sizeof(unsigned long)*8 - 1 - __builtin_clzl((unsigned long)(x)))
#define FAST_LOG2_UP(x) (((x) - (1 << FAST_LOG2(x))) ? FAST_LOG2(x) + 1 : FAST_LOG2(x))

flexio_dev_rpc_handler_t dpa_bringup_test_rpc;
__dpa_rpc__ uint64_t dpa_bringup_test_rpc(uint64_t arg)
{
    (void)arg;
    return DPA_BRINGUP_TEST_MAGICNUM;
}

static inline void rq_db_ring(flexio_uintptr_t dbr_daddr, uint32_t n)
{
    uint32_t *dbr = (uint32_t *)dbr_daddr;
    uint32_t pi;

    __dpa_thread_fence(__DPA_HEAP, __DPA_W, __DPA_W);

    pi = be32_to_cpu(*dbr) + n;
	*dbr = cpu_to_be32(pi & 0xffff);
}

static inline void
sq_db_ring(struct flexio_dev_thread_ctx *dtctx, flexio_uintptr_t dbr_daddr, uint32_t qpn)
{
	uint32_t *dbr = (uint32_t *)dbr_daddr + 1;
    uint32_t pi;

    __dpa_thread_fence(__DPA_HEAP, __DPA_W, __DPA_W);

    pi = be32_to_cpu(*dbr) + 1;
    *dbr = cpu_to_be32(pi & 0xffff);
	flexio_dev_qp_sq_ring_db(dtctx, pi & 0xffff, qpn);
}

static inline void step_cq(dpa_cq_ctx_t *cq_ctx)
{
	uint32_t cq_idx_mask = L2M(cq_ctx->log_cq_depth);

	cq_ctx->cq_idx++;
	cq_ctx->cqe = &cq_ctx->cq_ring[cq_ctx->cq_idx & cq_idx_mask];
	/* check for wrap around */
	if (!(cq_ctx->cq_idx & cq_idx_mask))
		cq_ctx->cq_hw_owner_bit = !cq_ctx->cq_hw_owner_bit;

	flexio_dev_dbr_cq_set_ci(cq_ctx->cq_dbr, cq_ctx->cq_idx);
}

static inline uint8_t
cq_dummy_poll(dpa_cq_ctx_t *cq_ctx, uint32_t *consumed_cqes, uint32_t max_batch, uint8_t expected_opcode)
{
    while ((flexio_dev_cqe_get_owner(cq_ctx->cqe) != cq_ctx->cq_hw_owner_bit) &&
           (*consumed_cqes < max_batch)) {
        if (flexio_dev_cqe_get_opcode(cq_ctx->cqe) != expected_opcode)
            return -1;
        step_cq(cq_ctx);
        (*consumed_cqes)++;
	}

	return 0;
}

static void cq_ctx_init(dpa_cq_ctx_t *cq_ctx,
                        uint32_t num,
                        uint32_t log_depth,
                        uint32_t ci_idx,
                        flexio_uintptr_t ring_addr,
                        flexio_uintptr_t dbr_addr,
                        uint8_t hw_owner_bit)
{
  	uint32_t cq_idx_mask = L2M(log_depth);
	cq_ctx->cq_number = num;
	cq_ctx->cq_ring = (struct flexio_dev_cqe64 *)ring_addr;
	cq_ctx->cq_dbr = (uint32_t *)dbr_addr;
	cq_ctx->cqe = &cq_ctx->cq_ring[ci_idx & cq_idx_mask];
	cq_ctx->cq_idx = ci_idx;
	cq_ctx->cq_hw_owner_bit = hw_owner_bit;
	cq_ctx->log_cq_depth = log_depth;
}

static inline uint32_t cqe_get_imm_data(struct flexio_dev_cqe64 *cqe)
{
    return (uint32_t)cqe->rsvd36[0]; // we assume that there is no need to ntohl
}

#ifdef DPA_RELIABILITY_BITMAP
static inline void bitmap_set_bit(uint64_t *bitmap, uint32_t bit_id)
{
#ifdef DPA_RELIABILITY_BITMAP_NAIVE
    uint32_t arr_id     = bit_id / 64; // we store bitmap as array of uint64_ts
    uint32_t bit_offset = bit_id % 64;
    bitmap[arr_id] |= ((uint64_t)1 << bit_offset);
#else
    uint32_t arr_id_opt     = bit_id >> 6;
    uint32_t bit_offset_opt = bit_id & 63;
    uint64_t tmp = bitmap[arr_id_opt];
    __asm__ ("bset %[rd], %[rs1], %[rs2]"
             : [rd] "=r"(tmp)
             : [rs1] "r"(tmp),
               [rs2] "r"(bit_offset_opt));
    bitmap[arr_id_opt] = tmp;
#endif
#ifdef DPA_RELIABILITY_BITMAP_DEBUG
    flexio_dev_print("bit_id=%u arr_id=%u bit_offset=%u arr_id_opt=%u bit_offset_opt=%u bitmap_val=%lu val=%lu\n",
                     bit_id, arr_id, bit_offset, arr_id_opt, bit_offset_opt,
                     bitmap[arr_id], res);
#endif
}
#endif

static inline uint32_t get_wqe_idx(uint32_t xq_log_depth, uint32_t pi_idx)
{
    int mask = L2M(xq_log_depth);

    if ((pi_idx & mask) == 0) {
        return 0;
    } else {
        return pi_idx % L2V(xq_log_depth);
    }
}

static inline void post_send(struct flexio_dev_thread_ctx *dtctx,
                             union flexio_dev_sqe_seg *swqe,
                             uint32_t qpn, flexio_uintptr_t qp_dbr_daddr, uint32_t sq_pi,
                             uint32_t data_size, uint64_t lkey, uint64_t ptr)
{
    flexio_dev_swqe_seg_ctrl_set(swqe, sq_pi, qpn,
                                 MLX5_CTRL_SEG_CE_CQE_ALWAYS,
                                 FLEXIO_CTRL_SEG_SEND_RC);
    swqe++;
    flexio_dev_swqe_seg_mem_ptr_data_set(swqe, data_size, lkey, ptr);
    sq_db_ring(dtctx, qp_dbr_daddr, qpn);
}

static inline void post_write(struct flexio_dev_thread_ctx *dtctx,
                              union flexio_dev_sqe_seg *swqe,
                              uint32_t qpn, flexio_uintptr_t qp_dbr_daddr, uint32_t sq_pi,
                              uint32_t data_size,
                              uint64_t lkey, uint64_t laddr,
                              uint64_t rkey, uint64_t raddr,
                              uint64_t last_write_seq_no,
                              uint64_t writes_per_cqe)
{
#ifdef DPA_DEBUG
    flexio_dev_print("Posting write on qpn=%u sq_pi=%u data_size=%u lkey=%lu laddr=%lu rkey=%lu raddr=%lu seq_no=%lu notify=%lu\n",
                     qpn, sq_pi, data_size, lkey, laddr, rkey, raddr,
                     last_write_seq_no, ((last_write_seq_no + 1) % writes_per_cqe));
#endif
    flexio_dev_swqe_seg_ctrl_set(swqe, sq_pi, qpn,
                                 ((last_write_seq_no + 1) % writes_per_cqe) ?
                                 MLX5_CTRL_SEG_CE_CQE_ON_CQE_ERROR : MLX5_CTRL_SEG_CE_CQE_ALWAYS,
                                 FLEXIO_CTRL_SEG_RDMA_WRITE);
    swqe++;
    flexio_dev_swqe_seg_rdma_set(swqe, rkey, raddr);
    swqe++;
    flexio_dev_swqe_seg_mem_ptr_data_set(swqe, data_size, lkey, laddr);
    sq_db_ring(dtctx, qp_dbr_daddr, qpn);
}

static inline void write_seq_no_update(uint64_t *last_write_seq_no, uint8_t write_seq_no_active)
{
    if (write_seq_no_active) ++(*last_write_seq_no);
}

static inline void post_write_w_imm(struct flexio_dev_thread_ctx *dtctx,
                                    union flexio_dev_sqe_seg *swqe,
                                    uint32_t qpn, flexio_uintptr_t qp_dbr_daddr, uint32_t sq_pi,
                                    uint32_t data_size,
                                    uint64_t lkey, uint64_t laddr,
                                    uint64_t rkey, uint64_t raddr,
                                    uint32_t imm_data)
{
    flexio_dev_swqe_seg_ctrl_set(swqe, sq_pi, qpn,
                                 MLX5_CTRL_SEG_CE_CQE_ALWAYS,
                                 FLEXIO_CTRL_SEG_RDMA_WRITE_IMM);
    swqe[0].ctrl.general_id = imm_data;
    swqe++;
    flexio_dev_swqe_seg_rdma_set(swqe, rkey, raddr);
    swqe++;
    flexio_dev_swqe_seg_mem_ptr_data_set(swqe, data_size, lkey, laddr);
    sq_db_ring(dtctx, qp_dbr_daddr, qpn);
}

static inline void dpa_pingpong_server_eh(struct dpa_export_data *export_data)
{
    struct dpa_pingpong_server_ctx *serv_ctx =
        (struct dpa_pingpong_server_ctx *)export_data->app_ctx;
    uint32_t cq_id = serv_ctx->cq_id;
    uint32_t qp_id = serv_ctx->qp_id;
    uint32_t cqes_consumed = 0;
    flexio_uintptr_t qp_dbr_daddr = export_data->qp_transfer[qp_id].qp_dbr_daddr;
    uint32_t sq_pi;
    uint32_t swqe_idx;
    union flexio_dev_sqe_seg *swqe;
    struct flexio_dev_thread_ctx *dtctx;
    dpa_cq_ctx_t cq_ctx;

#ifdef DPA_DEBUG
    flexio_dev_print("PingPong server qp_id=%d cq_id=%d qpn=%d cqn=%d: Started\n",
                     qp_id, cq_id,
                     export_data->qp_transfer[qp_id].qp_num,
                     export_data->cq_transfer[cq_id].cq_num);
#endif

    flexio_dev_get_thread_ctx(&dtctx);

    cq_ctx_init(&cq_ctx,
                export_data->cq_transfer[cq_id].cq_num,
                export_data->cq_transfer[cq_id].log_cq_depth,
                export_data->cq_transfer[cq_id].ci_idx,
                export_data->cq_transfer[cq_id].cq_ring_daddr,
                export_data->cq_transfer[cq_id].cq_dbr_daddr,
                export_data->cq_transfer[cq_id].hw_owner_bit);

    while (!cqes_consumed) {
        if (cq_dummy_poll(&cq_ctx, &cqes_consumed, DPA_MAX_NET_CQ_POLL_BATCH, DPA_CQE_RESPONDER_SEND)) {
            flexio_dev_print("PingPong server: Ping Receive Poll failed. CQe opcode=%d\n",
                             flexio_dev_cqe_get_opcode(cq_ctx.cqe));
            return;
        }
    }

    if (cqes_consumed != 1) {
        flexio_dev_print("PingPong server: Ping failed. Only 1 CQE shall be received, but got %d\n",
                         cqes_consumed);
        return;
    }

    sq_pi = be32_to_cpu(*((uint32_t *)qp_dbr_daddr + 1));
    swqe_idx = get_wqe_idx(export_data->qp_transfer[qp_id].log_qp_sq_depth, sq_pi);
    swqe = (union flexio_dev_sqe_seg *)(export_data->qp_transfer[qp_id].qp_sq_daddr) + swqe_idx * 4;

    post_send(dtctx, swqe,
              export_data->qp_transfer[qp_id].qp_num,
              export_data->qp_transfer[qp_id].qp_dbr_daddr,
              ++sq_pi,
              serv_ctx->base_info_size,
              serv_ctx->lkey,
              serv_ctx->ptr + serv_ctx->dgram_size); // send adjacent chunk

    cqes_consumed = 0;
    while (!cqes_consumed) {
        if (cq_dummy_poll(&cq_ctx, &cqes_consumed, 1, DPA_CQE_REQUESTER)) {
            flexio_dev_print("PingPong server: Pong Send Poll failed. CQe opcode=%d\n",
                             flexio_dev_cqe_get_opcode(cq_ctx.cqe));
            return;
        }
    }

    if (cqes_consumed != 1) {
        flexio_dev_print("PingPong server: Pong failed. Only 1 CQE shall be received, but got %d\n",
                         cqes_consumed);
        return;
    }

    export_data->cq_transfer[cq_id].ci_idx += 2;
    export_data->cq_transfer[cq_id].hw_owner_bit = cq_ctx.cq_hw_owner_bit;

#ifdef DPA_DEBUG
    flexio_dev_print("PingPong server qp_id=%d cq_id=%d qpn=%d cqn=%d: OK!\n",
                     qp_id, cq_id,
                     export_data->qp_transfer[qp_id].qp_num,
                     export_data->cq_transfer[cq_id].cq_num);
#endif

    flexio_dev_cq_arm(dtctx, cq_ctx.cq_idx, cq_ctx.cq_number);
    flexio_dev_thread_reschedule();
}

static inline void dpa_tput_server_eh(struct dpa_export_data *export_data)
{
    struct dpa_tput_ctx *serv_ctx = (struct dpa_tput_ctx *)export_data->app_ctx;
    uint64_t worker_id = __atomic_fetch_add(&serv_ctx->worker_id, 1, __ATOMIC_RELAXED);
    flexio_uintptr_t qp_dbr_daddr = export_data->qp_transfer[worker_id].qp_dbr_daddr;
    uint64_t to_process           = serv_ctx->to_process;
    uint32_t last_recvd_chunk_id  = 0 - 1; // wrap it up to max uint64_t val
#ifdef DPA_RELIABILITY_BITMAP
    uint64_t *recvbuf_bitmap      = serv_ctx->recvbuf_bitmaps[worker_id];
    uint32_t to_fetch             = 0;
#elif defined (DPA_RELIABILITY_COUNTER)
    uint32_t *gaps_to_fetch       = serv_ctx->gaps_to_fetch[worker_id];
    uint32_t gap_size             = 0;
    uint32_t gap_id               = 0;
#else
    (void)last_recvd_chunk_id;
#endif
    uint32_t recvd_chunk_id;
    flexio_uintptr_t host_counter_daddr;
    uint64_t finish_place;
    uint32_t sq_pi;
    uint32_t cqes_consumed;
    uint32_t swqe_idx;
    union flexio_dev_sqe_seg *swqe;
    struct flexio_dev_thread_ctx *dtctx;
    dpa_cq_ctx_t cq_ctx;
    DPA_PROFILE_TIMER_DECLARE(cq_poll_profile_ctx);
    DPA_PROFILE_TIMER_DECLARE(cq_poll_profile_ctx2);

    flexio_dev_get_thread_ctx(&dtctx);

    cq_ctx_init(&cq_ctx,
                export_data->cq_transfer[worker_id].cq_num,
                export_data->cq_transfer[worker_id].log_cq_depth,
                export_data->cq_transfer[worker_id].ci_idx,
                export_data->cq_transfer[worker_id].cq_ring_daddr,
                export_data->cq_transfer[worker_id].cq_dbr_daddr,
                export_data->cq_transfer[worker_id].hw_owner_bit);

#ifdef DPA_DEBUG
    flexio_dev_print("[tput server worker_id=%lu qpn=%d cqn=%d] Started, to_process=%lu\n",
                     worker_id,
                     export_data->qp_transfer[worker_id].qp_num,
                     export_data->cq_transfer[worker_id].cq_num,
                     to_process);
#endif

    /* Datapath */

    DPA_PROFILE_TIMER_INIT(cq_poll_profile_ctx);
    DPA_PROFILE_TIMER_INIT(cq_poll_profile_ctx2);
    while (to_process) {
        DPA_PROFILE_TIMER_START(cq_poll_profile_ctx);
        if (flexio_dev_cqe_get_owner(cq_ctx.cqe) != cq_ctx.cq_hw_owner_bit) {
            if (flexio_dev_cqe_get_opcode(cq_ctx.cqe) != DPA_CQE_RESPONDER_WRITE_W_IMM) {
                flexio_dev_print("[tput server] write with imm responder failed. CQe opcode=%d\n",
                                 flexio_dev_cqe_get_opcode(cq_ctx.cqe));
                return;
            }

            recvd_chunk_id = cqe_get_imm_data(cq_ctx.cqe);

            step_cq(&cq_ctx);
            rq_db_ring(qp_dbr_daddr, 1);

#ifdef DPA_RELIABILITY_BITMAP
            bitmap_set_bit(recvbuf_bitmap, recvd_chunk_id);
            if (recvd_chunk_id - 1 != last_recvd_chunk_id)
                to_fetch++;
#elif defined(DPA_RELIABILITY_COUNTER)
            if (recvd_chunk_id - 1 != last_recvd_chunk_id) {
                gap_size++;
            } else if (gap_size) {
                gaps_to_fetch[gap_id * 2]     = last_recvd_chunk_id;
                gaps_to_fetch[gap_id * 2 + 1] = gap_size;
                gap_size = 0;
                gap_id++;
            }
#endif
            last_recvd_chunk_id = recvd_chunk_id;
            to_process--;
#ifdef DPA_DEBUG
            flexio_dev_print("[tput server] chunk_id=%u to_process=%lu\n", recvd_chunk_id, to_process);
#endif
            DPA_PROFILE_TIMER_STOP(cq_poll_profile_ctx);
            DPA_PROFILE_TIMER_LAP(cq_poll_profile_ctx2);           
        }
    }

    /* Reliability, e.g., Ack to the neighbor */

#ifdef DPA_RELIABILITY_BITMAP
    if (to_fetch)
#elif defined (DPA_RELIABILITY_COUNTER)
    if (gap_id)
#else
    if (0)
#endif
    {
        flexio_dev_print("[tput server] reliability is not supported!\n");
        /* TODO: reliably fetch chunks from neighbor here */
        return;
    }

    sq_pi = be32_to_cpu(*((uint32_t *)qp_dbr_daddr + 1));
    swqe_idx = get_wqe_idx(export_data->qp_transfer[worker_id].log_qp_sq_depth, sq_pi);
    swqe = (union flexio_dev_sqe_seg *)(export_data->qp_transfer[worker_id].qp_sq_daddr) + swqe_idx * 4;

    post_send(dtctx, swqe,
              export_data->qp_transfer[worker_id].qp_num,
              export_data->qp_transfer[worker_id].qp_dbr_daddr,
              ++sq_pi,
              sizeof(uint64_t),
              serv_ctx->ack_lkey,
              serv_ctx->ack_ptr);

    cqes_consumed = 0;
    while (!cqes_consumed) {
        if (cq_dummy_poll(&cq_ctx, &cqes_consumed, 1, DPA_CQE_REQUESTER)) {
            flexio_dev_print( "[tput server] Ack send failed. CQe opcode=%d\n",
                           flexio_dev_cqe_get_opcode(cq_ctx.cqe));
            return;
        }
    }

    export_data->cq_transfer[worker_id].ci_idx += serv_ctx->to_process + 1;
    export_data->cq_transfer[worker_id].hw_owner_bit = cq_ctx.cq_hw_owner_bit;

    /* Notify host, e.g., the receive buffer can be released */
    finish_place = __atomic_fetch_add(&serv_ctx->finisher_id, 1, __ATOMIC_RELAXED);
    if (finish_place == (serv_ctx->n_workers - 1)) {
        flexio_dev_window_config(dtctx, export_data->window_id, serv_ctx->counter_lkey);
        flexio_dev_window_ptr_acquire(dtctx, serv_ctx->counter_ptr, &host_counter_daddr);
        *(uint64_t *)host_counter_daddr = DPA_WORK_COMPLETED_MAGICNUM + serv_ctx->iter;
        __dpa_thread_window_writeback();
    }

#ifdef DPA_DEBUG
    flexio_dev_print("[tput server worker_id=%lu qpn=%d cqn=%d] OK\n",
                     worker_id,
                     export_data->qp_transfer[worker_id].qp_num,
                     export_data->cq_transfer[worker_id].cq_num);
#endif

#ifdef DPA_CQ_POLL_PROFILE
    flexio_dev_print("[tput server worker_id=%lu] avg_cycles_per_cqe=%lu avg_instrs_per_cqe=%lu total_cycles_cqe=%lu total_instrs_cqe=%lu avg_cycles_per_poll=%lu avg_instrs_per_poll=%lu total_cycles_poll=%lu total_instrs_poll=%lu\n",
                     worker_id,
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx.cycles),
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx.instrs),
                     cq_poll_profile_ctx.cycles.sum,
                     cq_poll_profile_ctx.instrs.sum,
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx2.cycles),
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx2.instrs),
                     cq_poll_profile_ctx2.cycles.sum,
                     cq_poll_profile_ctx2.instrs.sum);
#endif

    flexio_dev_cq_arm(dtctx, cq_ctx.cq_idx, cq_ctx.cq_number);
    flexio_dev_thread_reschedule();
}

static inline void dpa_tput_staging_server_eh(struct dpa_export_data *export_data)
{
    struct dpa_tput_ctx *serv_ctx = (struct dpa_tput_ctx *)export_data->app_ctx;
    uint64_t nworker_id           = __atomic_fetch_add(&serv_ctx->worker_id, 1, __ATOMIC_RELAXED);
    uint64_t lworker_id           = LOOPBACK_CQQP_ARRAY_BASE_IDX + nworker_id;
    struct dpa_transfer_qp nqp    = export_data->qp_transfer[nworker_id];
    struct dpa_transfer_qp lqp    = export_data->qp_transfer[lworker_id];
    uint64_t lsq_depth            = L2V(lqp.log_qp_sq_depth);
    uint64_t to_process           = serv_ctx->to_process;
    uint64_t chunk_size           = serv_ctx->chunk_size;
    uint64_t hostmem_rkey         = serv_ctx->payload_rkey;
    uint64_t hostmem_base_ptr     = serv_ctx->payload_ptr + nworker_id * to_process * chunk_size;
    uint32_t last_recvd_chunk_id  = 0 - 1; // wrap it up to max uint64_t val
#ifdef DPA_RELIABILITY_BITMAP
    uint64_t *recvbuf_bitmap      = serv_ctx->recvbuf_bitmaps[nworker_id];
    uint32_t to_fetch             = 0;
#elif defined (DPA_RELIABILITY_COUNTER)
    uint32_t *gaps_to_fetch       = serv_ctx->gaps_to_fetch[nworker_id];
    uint32_t gap_size             = 0;
    uint32_t gap_id               = 0;
#else
    (void)last_recvd_chunk_id;
#endif
    uint8_t write_seq_no_active    = to_process >= DPA_CQE_NOTIFICATION_FREQUENCY;
    uint64_t last_write_seq_no     = write_seq_no_active ? 0 : (DPA_CQE_NOTIFICATION_FREQUENCY - 1);
    uint64_t writes_per_cqe        = write_seq_no_active ? DPA_CQE_NOTIFICATION_FREQUENCY : 1;
    uint32_t recvd_chunk_id;
    flexio_uintptr_t host_counter_daddr;
    uint64_t lsq_capacity;
    uint64_t finish_place;
    uint32_t nsq_pi, lsq_pi;
    uint32_t ncqes_consumed;
    uint32_t nswqe_idx, lswqe_idx;
    union flexio_dev_sqe_seg *nswqe, *lswqe;
    struct flexio_dev_thread_ctx *dtctx;
    dpa_cq_ctx_t ncq_ctx;
    dpa_cq_ctx_t lcq_ctx;

    DPA_PROFILE_TIMER_DECLARE(staging_ncq_poll_profile_ctx);
    DPA_PROFILE_TIMER_DECLARE(staging_ncqe_processing_profile_ctx);
    DPA_PROFILE_TIMER_DECLARE(staging_lcq_poll_profile_ctx);

    flexio_dev_get_thread_ctx(&dtctx);

    cq_ctx_init(&ncq_ctx,
                export_data->cq_transfer[nworker_id].cq_num,
                export_data->cq_transfer[nworker_id].log_cq_depth,
                export_data->cq_transfer[nworker_id].ci_idx,
                export_data->cq_transfer[nworker_id].cq_ring_daddr,
                export_data->cq_transfer[nworker_id].cq_dbr_daddr,
                export_data->cq_transfer[nworker_id].hw_owner_bit);
    cq_ctx_init(&lcq_ctx,
                export_data->cq_transfer[lworker_id].cq_num,
                export_data->cq_transfer[lworker_id].log_cq_depth,
                export_data->cq_transfer[lworker_id].ci_idx,
                export_data->cq_transfer[lworker_id].cq_ring_daddr,
                export_data->cq_transfer[lworker_id].cq_dbr_daddr,
                export_data->cq_transfer[lworker_id].hw_owner_bit);

#ifdef DPA_DEBUG
    flexio_dev_print("[staging proxy server worker_id=%lu nqpn=%d ncqn=%d lqpn=%d lcqn=%d] Started, to_process=%lu\n",
                     nworker_id,
                     nqp.qp_num,
                     export_data->cq_transfer[nworker_id].cq_num,
                     lqp.qp_num,
                     export_data->cq_transfer[lworker_id].cq_num,
                     to_process);
#endif

    /* Datapath loop */
    DPA_PROFILE_TIMER_INIT(staging_ncq_poll_profile_ctx);
    DPA_PROFILE_TIMER_INIT(staging_ncqe_processing_profile_ctx);
    lsq_capacity = lsq_depth;
    while (to_process) {
        if (lsq_capacity == 0) {
            if ((flexio_dev_cqe_get_owner(lcq_ctx.cqe) != lcq_ctx.cq_hw_owner_bit)) {
                if (flexio_dev_cqe_get_opcode(lcq_ctx.cqe) != DPA_CQE_REQUESTER) {
                    flexio_dev_print("[staging proxy server] write with imm responder failed. CQe opcode=%d\n",
                                     flexio_dev_cqe_get_opcode(lcq_ctx.cqe));
                    return;
                }
                rq_db_ring(nqp.qp_dbr_daddr, writes_per_cqe); // we want to re-post network RRs ASAP
                step_cq(&lcq_ctx);
                lsq_capacity += writes_per_cqe;
            }
        }

        if ((lsq_capacity > 0) && (flexio_dev_cqe_get_owner(ncq_ctx.cqe) != ncq_ctx.cq_hw_owner_bit)) {
            DPA_PROFILE_TIMER_START(staging_ncqe_processing_profile_ctx);
            if (flexio_dev_cqe_get_opcode(ncq_ctx.cqe) != DPA_CQE_RESPONDER_SEND_WITH_IMM) {
                flexio_dev_print("[staging proxy server] write with imm responder failed. CQe opcode=%d\n",
                                 flexio_dev_cqe_get_opcode(ncq_ctx.cqe));
                return;
            }

            recvd_chunk_id = cqe_get_imm_data(ncq_ctx.cqe);
            step_cq(&ncq_ctx);

            // issue write on the loopback QP for each new CQe
            // receives in the staging buffer are in sync with writes in user buf,
            // i.e., lswqe_idx is always behind nrq_pi

            lsq_pi = be32_to_cpu(*((uint32_t *)lqp.qp_dbr_daddr + 1));
            lswqe_idx = get_wqe_idx(lqp.log_qp_sq_depth, lsq_pi);
            lswqe = (union flexio_dev_sqe_seg *)(lqp.qp_sq_daddr) + lswqe_idx * 4;
            post_write(dtctx,
                       lswqe,
                       lqp.qp_num,
                       lqp.qp_dbr_daddr,
                       ++lsq_pi,
                       chunk_size,
                       lqp.rqd_lkey,
                       lqp.rqd_daddr + lswqe_idx * chunk_size,    // from staging
                       hostmem_rkey,
                       hostmem_base_ptr + recvd_chunk_id * chunk_size, // to host
                       last_write_seq_no,
                       writes_per_cqe);
            write_seq_no_update(&last_write_seq_no, write_seq_no_active);

#ifdef DPA_RELIABILITY_BITMAP
            bitmap_set_bit(recvbuf_bitmap, recvd_chunk_id);
            if (recvd_chunk_id - 1 != last_recvd_chunk_id) {
                to_fetch++;
            }
#elif defined(DPA_RELIABILITY_COUNTER)
            if (recvd_chunk_id - 1 != last_recvd_chunk_id) {
                gap_size++;
            } else if (gap_size) {
                gaps_to_fetch[gap_id * 2]     = last_recvd_chunk_id;
                gaps_to_fetch[gap_id * 2 + 1] = gap_size;
                gap_size = 0;
                gap_id++;
            }
#endif
            last_recvd_chunk_id = recvd_chunk_id;
            lsq_capacity--;
            to_process--;

            if ((lsq_capacity != lsq_depth) && (flexio_dev_cqe_get_owner(lcq_ctx.cqe) != lcq_ctx.cq_hw_owner_bit)) {
                if (flexio_dev_cqe_get_opcode(lcq_ctx.cqe) != DPA_CQE_REQUESTER) {
                    flexio_dev_print("[staging proxy server] write with imm responder failed. CQe opcode=%d\n",
                                     flexio_dev_cqe_get_opcode(lcq_ctx.cqe));
                    return;
                }
                rq_db_ring(nqp.qp_dbr_daddr, writes_per_cqe); // we want to re-post network RRs ASAP
                step_cq(&lcq_ctx);
                lsq_capacity += writes_per_cqe;
            }

#ifdef DPA_DEBUG
            flexio_dev_print("[staging proxy server] chunk_id=%u lsq_capacity=%lu to_process=%lu\n",
                             recvd_chunk_id, lsq_capacity, to_process);
#endif
            DPA_PROFILE_TIMER_STOP(staging_ncqe_processing_profile_ctx);
            DPA_PROFILE_TIMER_LAP(staging_ncq_poll_profile_ctx);
        }
    }

    // consume the rest
    DPA_PROFILE_TIMER_INIT(staging_lcq_poll_profile_ctx);
    while (lsq_capacity != lsq_depth) {
        while ((flexio_dev_cqe_get_owner(lcq_ctx.cqe) != lcq_ctx.cq_hw_owner_bit)) {
            if (flexio_dev_cqe_get_opcode(lcq_ctx.cqe) != DPA_CQE_REQUESTER) {
                flexio_dev_print("[staging proxy server] write with imm responder failed. CQe opcode=%d\n",
                                 flexio_dev_cqe_get_opcode(lcq_ctx.cqe));
                return;
            }
            rq_db_ring(nqp.qp_dbr_daddr, writes_per_cqe);
            step_cq(&lcq_ctx);
            lsq_capacity += writes_per_cqe;
#ifdef DPA_DEBUG
            flexio_dev_print("[staging proxy server] consuming the rest. lsq_capacity=%lu lsq_depth=%lu\n",
                             lsq_capacity, lsq_depth);
#endif
            DPA_PROFILE_TIMER_LAP(staging_lcq_poll_profile_ctx);
        }
    }

    /* Reliability, e.g., Ack to the neighbor */
#ifdef DPA_RELIABILITY_BITMAP
    if (to_fetch)
#elif defined (DPA_RELIABILITY_COUNTER)
    if (gap_id)
#else
    if (0)
#endif
    {
        flexio_dev_print( "[staging proxy server] reliability is not supported!\n");
        return;
        /* TODO: reliably fetch chunks from neighbor here */
    }

    nsq_pi    = be32_to_cpu(*((uint32_t *)nqp.qp_dbr_daddr + 1));
    nswqe_idx = get_wqe_idx(nqp.log_qp_sq_depth, nsq_pi);
    nswqe     = (union flexio_dev_sqe_seg *)(nqp.qp_sq_daddr) + nswqe_idx * 4;

    post_send(dtctx, nswqe,
              nqp.qp_num,
              nqp.qp_dbr_daddr,
              ++nsq_pi,
              sizeof(uint64_t),
              serv_ctx->ack_lkey,
              serv_ctx->ack_ptr);

    ncqes_consumed = 0;
    while (!ncqes_consumed) {
        if (cq_dummy_poll(&ncq_ctx, &ncqes_consumed, 1, DPA_CQE_REQUESTER)) {
            flexio_dev_print("[staging proxy server] Ack send failed. CQe opcode=%d imm_data=%u\n",
                             flexio_dev_cqe_get_opcode(ncq_ctx.cqe),
                             cqe_get_imm_data(ncq_ctx.cqe));
            return;
        }
    }

    export_data->cq_transfer[nworker_id].ci_idx       = ncq_ctx.cq_idx;
    export_data->cq_transfer[nworker_id].hw_owner_bit = ncq_ctx.cq_hw_owner_bit;
    export_data->cq_transfer[lworker_id].ci_idx       = lcq_ctx.cq_idx;
    export_data->cq_transfer[lworker_id].hw_owner_bit = lcq_ctx.cq_hw_owner_bit;

    /* Notify host, e.g., the receive buffer can be released */
    finish_place = __atomic_fetch_add(&serv_ctx->finisher_id, 1, __ATOMIC_RELAXED);
    if (finish_place == (serv_ctx->n_workers - 1)) {
        flexio_dev_window_config(dtctx, export_data->window_id, serv_ctx->counter_lkey);
        flexio_dev_window_ptr_acquire(dtctx, serv_ctx->counter_ptr, &host_counter_daddr);
        *(uint64_t *)host_counter_daddr = DPA_WORK_COMPLETED_MAGICNUM + serv_ctx->iter;
        __dpa_thread_window_writeback();

#ifdef DPA_CQ_POLL_PROFILE
        flexio_dev_print("[staging proxy server worker_id=%lu profile]"
                         "avg_cycles_ncq_poll=%lu avg_instrs_ncq_poll=%lu tot_cycles_ncq_poll=%lu tot_instrs_ncq_poll=%lu "
                         "avg_cycles_ncqe_processing=%lu avg_instrs_ncqe_processing=%lu tot_cycles_ncqe_processing=%lu tot_instrs_ncqe_processing=%lu "
                         "avg_cycles_lcq_poll=%lu avg_instrs_lcq_poll=%lu tot_cycles_lcq_poll=%lu tot_instrs_lcq_poll=%lu\n",
                         nworker_id,
                         DPA_PROFILE_COUNTER_AVG(staging_ncq_poll_profile_ctx.cycles),
                         DPA_PROFILE_COUNTER_AVG(staging_ncq_poll_profile_ctx.instrs),
                         staging_ncq_poll_profile_ctx.cycles.sum,
                         staging_ncq_poll_profile_ctx.instrs.sum,
                         DPA_PROFILE_COUNTER_AVG(staging_ncqe_processing_profile_ctx.cycles),
                         DPA_PROFILE_COUNTER_AVG(staging_ncqe_processing_profile_ctx.instrs),
                         staging_ncqe_processing_profile_ctx.cycles.sum,
                         staging_ncqe_processing_profile_ctx.instrs.sum,
                         DPA_PROFILE_COUNTER_AVG(staging_lcq_poll_profile_ctx.cycles),
                         DPA_PROFILE_COUNTER_AVG(staging_lcq_poll_profile_ctx.instrs),
                         staging_lcq_poll_profile_ctx.cycles.sum,
                         staging_lcq_poll_profile_ctx.instrs.sum);
#endif
    }

#ifdef DPA_DEBUG
    flexio_dev_print("[staging proxy server worker_id=%lu nqpn=%d ncqn=%d lqpn=%d lcqn=%d] OK\n",
                     nworker_id,
                     nqp.qp_num,
                     export_data->cq_transfer[nworker_id].cq_num,
                     lqp.qp_num,
                     export_data->cq_transfer[lworker_id].cq_num);
#endif

    flexio_dev_cq_arm(dtctx, ncq_ctx.cq_idx, ncq_ctx.cq_number);
    flexio_dev_thread_reschedule();
}

static inline void dpa_tput_client_eh(struct dpa_export_data *export_data)
{
    struct dpa_tput_ctx *client_ctx = (struct dpa_tput_ctx *)export_data->app_ctx;
    uint64_t worker_id              = __atomic_fetch_add(&client_ctx->worker_id, 1, __ATOMIC_RELAXED);
    uint64_t lworker_id             = LOOPBACK_CQQP_ARRAY_BASE_IDX + worker_id;
    struct dpa_transfer_qp qp       = export_data->qp_transfer[worker_id];
    uint64_t sq_depth               = client_ctx->tx_window;
    uint64_t sq_capacity            = sq_depth;
    uint64_t to_process             = client_ctx->to_process;
    uint64_t lkey                   = client_ctx->payload_rkey;
    uint64_t lptr                   = client_ctx->payload_ptr;
    uint64_t rkey                   = client_ctx->ack_lkey; // dirty: we re-use ack fields
    uint64_t rptr                   = client_ctx->ack_ptr;
    uint32_t chunk_size             = client_ctx->chunk_size;
    uint64_t data_offset            = worker_id * to_process * chunk_size;
    uint32_t chunk_id               = 0;
    uint32_t poll_ack               = 1;
    flexio_uintptr_t host_counter_daddr;
    uint64_t finish_place;
    uint32_t sq_pi;
    uint32_t cqes_consumed;
    uint32_t swqe_idx;
    union flexio_dev_sqe_seg *swqe;
    struct flexio_dev_thread_ctx *dtctx;
    dpa_cq_ctx_t cq_ctx;
    dpa_cq_ctx_t lcq_ctx;
    DPA_PROFILE_TIMER_DECLARE(cq_poll_profile_ctx);

    flexio_dev_get_thread_ctx(&dtctx);

#ifdef DPA_DEBUG
    flexio_dev_print("[tput client worker_id=%lu qpn=%d cqn=%d cqn_ci=%d lqpn=%d lcqn=%d lcqn_ci=%d] Started, sq_depth=%lu to_process=%lu\n",
                     worker_id,
                     export_data->qp_transfer[worker_id].qp_num,
                     export_data->cq_transfer[worker_id].cq_num,
                     export_data->cq_transfer[worker_id].ci_idx,
                     export_data->qp_transfer[lworker_id].qp_num,
                     export_data->cq_transfer[lworker_id].cq_num,
                     export_data->cq_transfer[lworker_id].ci_idx,
                     sq_depth,
                     to_process);
#endif

    cq_ctx_init(&lcq_ctx,
                export_data->cq_transfer[lworker_id].cq_num,
                export_data->cq_transfer[lworker_id].log_cq_depth,
                export_data->cq_transfer[lworker_id].ci_idx,
                export_data->cq_transfer[lworker_id].cq_ring_daddr,
                export_data->cq_transfer[lworker_id].cq_dbr_daddr,
                export_data->cq_transfer[lworker_id].hw_owner_bit);

    cqes_consumed = 0;
    while (!cqes_consumed) {
        if (cq_dummy_poll(&lcq_ctx, &cqes_consumed, 1, DPA_CQE_RESPONDER_SEND)) {
            flexio_dev_print("[tput client] Thread activation failed. CQe opcode=%d\n",
                             flexio_dev_cqe_get_opcode(lcq_ctx.cqe));
            return;
        }
    }

    export_data->cq_transfer[lworker_id].ci_idx++;
    export_data->cq_transfer[lworker_id].hw_owner_bit = lcq_ctx.cq_hw_owner_bit;

    cq_ctx_init(&cq_ctx,
                export_data->cq_transfer[worker_id].cq_num,
                export_data->cq_transfer[worker_id].log_cq_depth,
                export_data->cq_transfer[worker_id].ci_idx,
                export_data->cq_transfer[worker_id].cq_ring_daddr,
                export_data->cq_transfer[worker_id].cq_dbr_daddr,
                export_data->cq_transfer[worker_id].hw_owner_bit);

    /* Datapath */

    DPA_PROFILE_TIMER_INIT(cq_poll_profile_ctx);
    while (chunk_id < to_process) {
        while (sq_capacity == 0) {
            if (flexio_dev_cqe_get_owner(cq_ctx.cqe) != cq_ctx.cq_hw_owner_bit) {
                if (flexio_dev_cqe_get_opcode(cq_ctx.cqe) != DPA_CQE_REQUESTER) {
                    flexio_dev_print("[tput client (main loop)] write with imm requester failed. CQe opcode=%d chunk_id=%u sq_capacity=%lu bmark_iter=%u worker_id=%lu\n",
                                     flexio_dev_cqe_get_opcode(cq_ctx.cqe),
                                     chunk_id,
                                     sq_capacity,
                                     client_ctx->iter,
                                     worker_id);
                    return;
                }
                step_cq(&cq_ctx);
                sq_capacity++;
            }
        }

        sq_pi = be32_to_cpu(*((uint32_t *)qp.qp_dbr_daddr + 1));
        swqe_idx = get_wqe_idx(qp.log_qp_sq_depth, sq_pi);
        swqe = (union flexio_dev_sqe_seg *)(qp.qp_sq_daddr) + swqe_idx * 4;
        post_write_w_imm(dtctx,
                         swqe,
                         qp.qp_num,
                         qp.qp_dbr_daddr,
                         ++sq_pi,
                         chunk_size,
                         lkey,
                         lptr + data_offset,
                         rkey,
                         rptr + data_offset,
                         chunk_id);

        data_offset += chunk_size;
        chunk_id++;
        sq_capacity--;

#ifdef DPA_DEBUG
        flexio_dev_print("[tput client] chunk_id=%u\n", chunk_id);
#endif
        DPA_PROFILE_TIMER_LAP(cq_poll_profile_ctx);
    }

    while (sq_capacity < sq_depth) {
        if ((flexio_dev_cqe_get_owner(cq_ctx.cqe) != cq_ctx.cq_hw_owner_bit)) {
            if (flexio_dev_cqe_get_opcode(cq_ctx.cqe) != DPA_CQE_REQUESTER) {
                if (flexio_dev_cqe_get_opcode(cq_ctx.cqe) != DPA_CQE_RESPONDER_SEND) {
                    flexio_dev_print("[tput client (secondary loop)] write with imm requester failed. CQe opcode=%d chunk_id=%u sq_capacity=%lu bmark_iter=%u worker_id=%lu\n",
                                     flexio_dev_cqe_get_opcode(cq_ctx.cqe),
                                     chunk_id,
                                     sq_capacity,
                                     client_ctx->iter,
                                     worker_id);
                    return;
                } else {
                    poll_ack = 0;
                    break;
                }
            }
            step_cq(&cq_ctx);
            sq_capacity++;
        }
    }

    /* Reliability, e.g., Ack from the neighbor */

    cqes_consumed = 0;
    while (poll_ack && !cqes_consumed) {
        if (cq_dummy_poll(&cq_ctx, &cqes_consumed, 1, DPA_CQE_RESPONDER_SEND)) {
            flexio_dev_print("[tput client] Ack receive failed. CQe opcode=%d\n",
                             flexio_dev_cqe_get_opcode(cq_ctx.cqe));
            return;
        }
    }

    export_data->cq_transfer[worker_id].ci_idx += to_process + 1;
    export_data->cq_transfer[worker_id].hw_owner_bit = cq_ctx.cq_hw_owner_bit;

    /* Notify host, e.g., the receive buffer can be released */
    finish_place = __atomic_fetch_add(&client_ctx->finisher_id, 1, __ATOMIC_RELAXED);
    if (finish_place == (client_ctx->n_workers - 1)) {
        flexio_dev_window_config(dtctx, export_data->window_id, client_ctx->counter_lkey);
        flexio_dev_window_ptr_acquire(dtctx, client_ctx->counter_ptr, &host_counter_daddr);
        *(uint64_t *)host_counter_daddr = DPA_WORK_COMPLETED_MAGICNUM + client_ctx->iter;
        __dpa_thread_window_writeback();
    }

#ifdef DPA_DEBUG
    flexio_dev_print("[tput client worker_id=%lu qpn=%d cqn=%d] OK\n",
                     worker_id,
                     export_data->qp_transfer[worker_id].qp_num,
                     export_data->cq_transfer[worker_id].cq_num);
#endif

#ifdef DPA_CQ_POLL_PROFILE
    flexio_dev_print("[tput client worker_id=%lu] avg_cycles_per_poll=%lu avg_instrs_per_poll=%lu total_cycles=%lu total_instrs=%lu\n",
                     worker_id,
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx.cycles),
                     DPA_PROFILE_COUNTER_AVG(cq_poll_profile_ctx.instrs),
                     cq_poll_profile_ctx.cycles.sum,
                     cq_poll_profile_ctx.instrs.sum);
#endif

    flexio_dev_cq_arm(dtctx, lcq_ctx.cq_idx, lcq_ctx.cq_number);
    flexio_dev_thread_reschedule();
}

flexio_dev_event_handler_t dpa_main_eh;
__dpa_global__ void dpa_main_eh(uint64_t export_data_daddr)
{
    struct dpa_export_data *export_data = (struct dpa_export_data *)export_data_daddr;
    uint64_t verb;

    __dpa_thread_system_fence();

#ifdef DPA_DEBUG
    flexio_dev_print("dpa main invoked\n");
#endif

    /* 
     * Note about this design:
     *
     * EH is accociated with CQ during its creation.
     * This means, that all QPs associated with this CQ will be handled with the same EH.
     * This switch statement allows us to choose which function to call.
     */

    verb = *((uint64_t *)export_data->verb);
    switch(verb) {
    case DPA_EH_PINGPONG_SERVER:
        dpa_pingpong_server_eh(export_data);
        break;
    case DPA_EH_TPUT_SERVER:
        dpa_tput_server_eh(export_data);
        break;
    case DPA_EH_TPUT_STAGING_SERVER:
        dpa_tput_staging_server_eh(export_data);
        break;
    case DPA_EH_TPUT_CLIENT:
        dpa_tput_client_eh(export_data);
        break;
    default:
        flexio_dev_print("Wrong EH verb!\n");
        return;
    }
}
