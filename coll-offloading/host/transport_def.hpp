#ifndef _TRANSPORT_DEF_HPP_
#define _TRANSPORT_DEF_HPP_

#include <iostream>
#include <functional>
#include <unistd.h>

#include <infiniband/verbs.h>

const std::size_t mac_addr_len = 6;

struct ep_addr
{
    uint8_t _mac_addr[mac_addr_len];
	union ibv_gid _gid;
    uint8_t _gid_table_index;
    uint16_t _lid;
    uint32_t _qpn;
    bool _is_client;

    std::string debug_get_str()
    {
        return std::string("lid=" + std::to_string(_lid) +
                           ";qpn=" + std::to_string(_qpn));
    }
};

struct packed_mkey
{
    uint64_t lkey;
    uint64_t rkey;
    uint64_t rptr;

    std::string debug_get_str()
    {
        return std::string("lkey=" + std::to_string(lkey) +
                           ";rkey=" + std::to_string(rkey) +
                           ";rptr=" + std::to_string(rptr));
    }
};


struct dev_ctx
{
    dev_ctx() {};
    virtual ~dev_ctx() {};
    virtual struct ibv_mr* reg_mr(void *buf, std::size_t len) = 0;
    virtual void dereg_mr(struct ibv_mr *mr) = 0;
};

struct pe_ctx
{
    pe_ctx(std::size_t poll_batch_size, std::size_t idx, std::size_t q_len)
        :
        _idx(idx),
        _poll_batch_size(poll_batch_size),
        _q_len(q_len)
    {};

    virtual ~pe_ctx() {};
    virtual int poll() = 0;
    virtual int poll(struct ibv_wc **wcs)
    {
        (void)wcs;
        std::cerr << "poll method not supported" << std::endl;
        exit(EXIT_FAILURE);
        return 1;
    }

    virtual void
    poll_worker(std::function<bool(struct ibv_wc *, struct bmark_ctx &ctx)> cb,
                struct bmark_ctx &ctx) = 0;

    std::size_t _idx;
    std::size_t _poll_batch_size;
    std::size_t _q_len;
};

struct ep_ctx
{
    ep_ctx(struct pe_ctx *pe, std::size_t idx, std::size_t q_len)
        :
        _pe(pe),
        _idx(idx),
        _q_len(q_len)
    {
        memset(&_addr, 0, sizeof(_addr));
        memset(&_loopback_ep_addr, 0, sizeof(_loopback_ep_addr));
        memset(&_remote_addr, 0, sizeof(_remote_addr));
    }

    virtual ~ep_ctx() {};

    virtual void connect(struct ep_addr remote_addr)
    {
        _remote_addr = remote_addr;
    }

    virtual void post_write_with_imm(struct ibv_mr *smr, void *sbuf, uint64_t rkey, uint64_t rbuf,
                             std::size_t len, uint64_t id = 0)
    {
        (void)smr;
        (void)sbuf;
        (void)rkey;
        (void)rbuf;
        (void)len;
        (void)id;
        std::cerr << "RDMA Write with Immediate is not supported" << std::endl;
        exit(EXIT_FAILURE);
    }

    virtual void
    post_write_with_imm_batch(struct ibv_mr *smr, void *sbuf, uint64_t rkey, uint64_t rbuf,
                              std::size_t len, std::size_t batch_size, uint64_t id = 0,
                              uint32_t write_start_idx = 0)
    {
        (void)smr;
        (void)sbuf;
        (void)rkey;
        (void)rbuf;
        (void)len;
        (void)batch_size;
        (void)id;
        (void)write_start_idx;
        std::cerr << "Batched RDMA Write with Immediate is not supported" << std::endl;
        exit(EXIT_FAILURE);
    }

    virtual void
    post_send_batch(struct ibv_mr *smr, void *sbuf,
                    std::size_t len, std::size_t batch_size, uint64_t id = 0,
                    uint32_t send_start_idx = 0)
    {
        (void)smr;
        (void)sbuf;
        (void)len;
        (void)batch_size;
        (void)id;
        (void)send_start_idx;
        std::cerr << "Batched RDMA Send is not supported" << std::endl;
        exit(EXIT_FAILURE);
    }

    virtual void post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id = 0, bool unsignaled = false) = 0;
    virtual void post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id = 0) = 0;

    struct pe_ctx *_pe;
    struct ep_addr _addr;
    struct ep_addr _loopback_ep_addr;
    struct ep_addr _remote_addr;
    std::size_t _idx;
    std::size_t _q_len;
};

#endif
