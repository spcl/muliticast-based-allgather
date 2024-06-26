#ifndef _IBV_TRANSPORT_HPP_
#define _IBV_TRANSPORT_HPP_

#include <string>

#include "transport_def.hpp"
#include "utils.hpp"

#define IBV_CHKERR(x) CHKERR(IBV, x)
#define IBV_CHKERR_PTR(x, ptr) CHKERR_PTR(IBV, x, ptr)

extern std::string mlx5_iface_name;
extern const std::size_t mlx5_max_gid_table_entries;
extern const std::size_t mlx5_max_mtu;
extern const int mlx5_default_qkey;
extern const int mlx5_default_pkey_index;
extern const int mlx5_default_port_num;
extern const int mlx5_default_sl;
extern const std::size_t mlx5_if_namesize;
extern const std::size_t max_path_len;

bool ib_ctx_query_params(struct ibv_context *ctx, struct ep_addr &addr);
struct ibv_mr *
ib_mr_reg(struct ibv_pd *pd, void *buf, std::size_t len);
void ib_mr_dereg(struct ibv_mr *mr);

struct ib_dev_ctx : public dev_ctx
{
    ib_dev_ctx(const std::string &dev_name);
    ~ib_dev_ctx();

    struct ibv_mr* reg_mr(void *buf, std::size_t len);
    void dereg_mr(struct ibv_mr* mr);

    struct ibv_context *_ctx;
    struct ibv_pd *_pd;
};

struct ib_cq_pe_ctx : public pe_ctx
{
    ib_cq_pe_ctx(struct dev_ctx *dev, std::size_t batch_size, std::size_t idx, std::size_t q_len);
    ~ib_cq_pe_ctx();

    int poll();
    int poll(struct ibv_wc **wcs);
    void poll_worker(std::function<bool(struct ibv_wc *wc, struct bmark_ctx &ctx)> cb,
                     struct bmark_ctx &ctx);

    struct ibv_cq *_cq;

protected:
    std::vector<struct ibv_wc> _wcs;
};

struct ud_ep_ctx : public ep_ctx
{
    ud_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe, std::size_t idx, std::size_t q_len);
    ~ud_ep_ctx();

    void connect(struct ep_addr remote_addr);
    void post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled);
    void post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);

protected:

    struct ib_dev_ctx *_dev;
    struct ib_cq_pe_ctx *_pe;
    struct ibv_qp *_qp;
    struct ibv_ah * _ah;
};

struct rc_ep_ctx : public ep_ctx
{
    rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe, std::size_t idx, std::size_t q_len, bool rnr_retry = false);
    ~rc_ep_ctx();

    void connect(struct ep_addr remote_addr);
    void post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled);
    void post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id);
    void post_send_batch(struct ibv_mr *smr, void *sbuf, std::size_t len,
                         std::size_t batch_size, uint64_t id, uint32_t send_start_idx);
    void post_write_with_imm(struct ibv_mr *smr, void *sbuf, uint64_t rkey, uint64_t rbuf,
                             std::size_t len, uint64_t id);
    void post_write_with_imm_batch(struct ibv_mr *smr, void *sbuf, uint64_t rkey, uint64_t rbuf,
                                   std::size_t len, std::size_t batch_size, uint64_t id,
                                   uint32_t write_start_idx);

protected:

    struct ib_dev_ctx *_dev;
    struct ib_cq_pe_ctx *_pe;
    struct ibv_qp *_qp;

private:
    std::vector<struct ibv_send_wr> _wrs_pool;
    std::vector<struct ibv_sge> _sge_pool;
    bool _rnr_retry;
};

struct dummy_rc_ep_ctx : public rc_ep_ctx
{
    dummy_rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe);
    void connect(struct ep_addr remote_addr);
};

#endif
