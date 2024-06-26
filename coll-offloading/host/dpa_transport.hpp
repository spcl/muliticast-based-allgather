#ifndef _DPA_TRANSPORT_HPP_
#define _DPA_TRANSPORT_HPP_

#include <string>
#include <libflexio/flexio.h>

#include "utils.hpp"
#include "ibv_transport.hpp"

#include "flexio_ag_bench_com.hpp"

#define L2V(l) (1UL << (l))
#define L2M(l) (L2V(l) - 1)
#define FAST_LOG2(x) (sizeof(unsigned long)*8 - 1 - __builtin_clzl((unsigned long)(x)))
#define FAST_LOG2_UP(x) (((x) - (1 << FAST_LOG2(x))) ? FAST_LOG2(x) + 1 : FAST_LOG2(x))
#define FLEXIO_CHKERR(x) CHKERR(FLEXIO, x)
#define FLEXIO_CHKERR_PTR(x, ptr) CHKERR_PTR(FLEXIO, x, ptr)
#define FLEXIO_HARTS_PER_CORE 16
#define FLEXIO_HART_NUM(core_num, hart_num_in_core) ((core_num) * FLEXIO_HARTS_PER_CORE + hart_num_in_core)

#define LOG_FLEXIO_CQE_BSIZE 6
#define LOG_FLEXIO_RWQE_BSIZE 4
#define LOG_FLEXIO_SWQE_BSIZE 6
#define LOG_FLEXIO_CQ_BSIZE(log_q_depth) ((log_q_depth) + LOG_FLEXIO_CQE_BSIZE)
#define LOG_FLEXIO_RQ_BSIZE(log_q_depth) ((log_q_depth) + LOG_FLEXIO_RWQE_BSIZE)

const std::size_t flexio_stream_buff_size = 2048 * 2048;

struct dpa_dev_ctx : public ib_dev_ctx
{
    dpa_dev_ctx(std::string device_name);
    ~dpa_dev_ctx();

    void stream_flush();
    void dev_ctx_flush();
    void app_ctx_alloc(std::size_t max_ctx_size);
    void app_ctx_free();
    void app_ctx_export(uint64_t app_id, void *ctx, std::size_t ctx_size);
    uint64_t rpc_call(flexio_func_t &rpc);

    struct flexio_uar *_dpa_uar;
    struct flexio_process *_process;
    struct flexio_outbox *_outbox;
    struct flexio_window *_window;
    struct flexio_msg_stream *_msg_stream;

    struct dpa_export_data _export_data;
    flexio_uintptr_t _export_data_daddr;
};

struct dpa_pe_ctx : public pe_ctx
{
    dpa_pe_ctx(struct dev_ctx *dev, std::size_t batch_size, std::size_t idx,
               std::size_t q_len, bool active = true);
    ~dpa_pe_ctx();

    int poll();
    void poll_worker(std::function<bool(struct ibv_wc *wc, struct bmark_ctx &ctx)> cb,
                     struct bmark_ctx &ctx);

    struct dpa_dev_ctx *_dev;
    struct flexio_event_handler *_eh;
    struct flexio_cq *_cq;
    struct dpa_transfer_cq _cq_transfer;
};

struct dpa_rc_ep_ctx : public ep_ctx
{
    dpa_rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe, std::size_t idx, std::size_t q_len,
                  bool rnr_retry,
                  bool root = true,
                  bool active_loopback = true,
                  bool remote_loopback_end = false);
    ~dpa_rc_ep_ctx();

    void connect(struct ep_addr remote_addr);
    void post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled);
    void post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);

    // for now we use host2dpa only for thread activation
    void post_send_host2dpa(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);
    void post_recv_host2dpa(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);

    struct dpa_dev_ctx *_dev;
    struct dpa_pe_ctx *_pe;
    struct flexio_qp *_qp;

    struct ib_cq_pe_ctx *_host_cq;
    struct rc_ep_ctx *_host_qp;

    struct dpa_pe_ctx *_loopback_dpa_cq;
    struct dpa_rc_ep_ctx *_loopback_dpa_qp;

    flexio_uintptr_t _wq_daddr;
    struct flexio_mkey *_rqd_mkey;
    struct dpa_transfer_qp _qp_transfer;
};

struct dpa_rc_staging_ep_ctx : public dpa_rc_ep_ctx
{
    enum staging_mem_type {
        MEM_TYPE_MEMIC = 0x0,
        MEM_TYPE_DPA   = 0x1,
        MEM_TYPE_HOST  = 0x2
    };
    static int str2memtype(const std::string &str_mem_type);

    dpa_rc_staging_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe,
                              std::size_t idx, std::size_t q_len,
                              bool rnr_retry,
                              int staging_mem_type = MEM_TYPE_HOST,
                              bool remote_loopback_end = false);
    ~dpa_rc_staging_ep_ctx();

    void connect_to_hmem_daemon(struct ep_addr hmem_daemon_addr);
    void connect(struct ep_addr remote_addr) { dpa_rc_ep_ctx::connect(remote_addr); };
    void post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);

    std::size_t _max_pkt_size;
    std::size_t _staging_rq_pi;
    std::size_t _staging_rq_size;
    struct ibv_dm *_staging_dm;
    struct ibv_mr *_staging_mr;
    struct flexio_mkey *_staging_dpa_mkey;
    uint64_t _staging_addr;
    int _staging_mem_type;
};

#endif
