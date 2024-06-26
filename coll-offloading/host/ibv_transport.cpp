#include "ibv_transport.hpp"

#include <cassert>

#include <net/if.h>
#include <arpa/inet.h>

std::string mlx5_iface_name = "mlx5_2"; // can be overwritten
const std::size_t mlx5_max_gid_table_entries = 32;
const int mlx5_default_qkey = 0x11111111;
const int mlx5_default_pkey_index = 0;
const int mlx5_default_port_num = 1;
const int mlx5_default_sl = 0;
const std::size_t mlx5_max_mtu = 4096;
const std::size_t mlx5_if_namesize = 32;
const std::size_t max_path_len = 8192;

bool ib_ctx_query_params(struct ibv_context *ctx, struct ep_addr &addr)
{
    struct ibv_port_attr port_attr;

    IBV_CHKERR(ibv_query_port(ctx, mlx5_default_port_num, &port_attr));
    addr._lid = port_attr.lid;

    struct ibv_gid_entry *gid_tbl_entries = new struct ibv_gid_entry[mlx5_max_gid_table_entries];
    memset(gid_tbl_entries, 0, mlx5_max_gid_table_entries * sizeof(struct ibv_gid_entry));

	auto num_entries = ibv_query_gid_table(ctx, gid_tbl_entries, mlx5_max_gid_table_entries, 0);

    auto dev_found = false;
	for (auto i = 0; i < num_entries && !dev_found; i++) {
        if (gid_tbl_entries[i].gid_type == IBV_GID_TYPE_IB) {
 			addr._gid_table_index = gid_tbl_entries[i].gid_index;
			addr._gid = gid_tbl_entries[i].gid;
            addr._mac_addr[0] = 0;           
            addr._mac_addr[1] = 0;
            addr._mac_addr[2] = 0;
            addr._mac_addr[3] = 0;
            addr._mac_addr[4] = 0;
            addr._mac_addr[5] = 0;
            dev_found = true;
        }

		if (gid_tbl_entries[i].gid_type == IBV_GID_TYPE_ROCE_V2) {
            char ifname[mlx5_if_namesize];
            char sys_path[max_path_len];
            if_indextoname(gid_tbl_entries[i].ndev_ifindex, ifname);
			snprintf(sys_path, max_path_len, "/sys/class/net/%s/address", ifname);
            FILE *maddr_file = fopen(sys_path, "r");
			assert(maddr_file);
            auto ret = fscanf(maddr_file, "%hhx:%hhx:%hhx:%hhx:%hhx:%hhx%*c",
                              &addr._mac_addr[0],
                              &addr._mac_addr[1],
                              &addr._mac_addr[2],
                              &addr._mac_addr[3],
                              &addr._mac_addr[4],
                              &addr._mac_addr[5]);
            assert(ret == 6);
			fclose(maddr_file);
			addr._gid_table_index = gid_tbl_entries[i].gid_index;
			addr._gid             = gid_tbl_entries[i].gid;
			dev_found = true;
		}
	}

    delete[] gid_tbl_entries;
    return dev_found;
}

static void ib_dev_create(std::string device_name, struct ibv_context **ctx, struct ibv_pd **pd)
{
    struct ibv_device **dev_list = nullptr;
    IBV_CHKERR_PTR(ibv_get_device_list(nullptr), dev_list);

    int i = 0;
    for (; dev_list[i]; i++)
        if (!strcmp(ibv_get_device_name(dev_list[i]), device_name.c_str()))
            break;

    struct ibv_device *dev = dev_list[i];
    if (!dev) {
        std::cout << "Not found IB device named " << device_name << std::endl;
        exit(EXIT_FAILURE);
    }

    ibv_context *_ctx;
    IBV_CHKERR_PTR(ibv_open_device(dev), _ctx);

    struct ibv_port_attr port_attr;
    IBV_CHKERR(ibv_query_port(_ctx, mlx5_default_port_num, &port_attr));
    if (port_attr.state != IBV_PORT_ACTIVE) {
        std::cout << "Device port " << ibv_get_device_name(dev)
                  << " is not in the active state" << std::endl;
        exit(EXIT_FAILURE);
    }

    struct ibv_pd *_pd;
    IBV_CHKERR_PTR(ibv_alloc_pd(_ctx), _pd);

    *ctx = _ctx;
    *pd = _pd;

    ibv_free_device_list(dev_list);
}

static void ib_dev_destroy(struct ibv_context *ctx, struct ibv_pd *pd)
{
    IBV_CHKERR(ibv_dealloc_pd(pd));
    IBV_CHKERR(ibv_close_device(ctx));
}

static void ib_cq_create(struct ibv_context *ctx, std::size_t depth, struct ibv_cq **cq)
{
    IBV_CHKERR_PTR(ibv_create_cq(ctx, depth, nullptr, nullptr, 0), *cq);
}

static void ib_cq_destroy(struct ibv_cq *cq)
{
    IBV_CHKERR(ibv_destroy_cq(cq));
}

static void
ib_ah_create(struct ibv_pd *pd, const struct ep_addr &remote_addr, struct ibv_ah **ah)
{
    struct ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr)); 
    ah_attr.is_global     = 0;
    ah_attr.dlid          = remote_addr._lid;
    ah_attr.sl            = mlx5_default_sl;
    ah_attr.port_num      = mlx5_default_port_num;
    ah_attr.src_path_bits = 0;
    IBV_CHKERR_PTR(ibv_create_ah(pd, &ah_attr), *ah);
}

static void ib_ah_destroy(struct ibv_ah *ah)
{
    IBV_CHKERR(ibv_destroy_ah(ah));
}

static void
ib_qp_create(struct ibv_context* ctx, struct ibv_pd* pd,
             struct ibv_cq *cq, ibv_qp_type qp_type,
             std::size_t sq_rq_depth, struct ibv_qp **qp)
{
    struct ibv_qp_init_attr_ex qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type             = qp_type;
    qp_init_attr.sq_sig_all          = 0;
    qp_init_attr.send_cq             = cq;
    qp_init_attr.recv_cq             = cq;
    qp_init_attr.cap.max_send_wr     = sq_rq_depth;
    qp_init_attr.cap.max_recv_wr     = sq_rq_depth;
    qp_init_attr.cap.max_send_sge    = 1;
    qp_init_attr.cap.max_recv_sge    = 1;
    //qp_init_attr.cap.max_inline_data = 128;
    qp_init_attr.comp_mask           = IBV_QP_INIT_ATTR_PD;
    qp_init_attr.pd                  = pd;

    IBV_CHKERR_PTR(ibv_create_qp_ex(ctx, &qp_init_attr), *qp);
}

static void ib_qp_destroy(struct ibv_qp *qp)
{
    IBV_CHKERR(ibv_destroy_qp(qp));
}

static void ib_qp_rc_transit_to_rts(struct ibv_qp *qp, struct ep_addr &remote_addr, bool rnr_retry)
{
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
	attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = mlx5_default_pkey_index; // ok
    attr.port_num        = mlx5_default_port_num;   
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_ATOMIC |
                           IBV_ACCESS_REMOTE_READ;
    IBV_CHKERR(ibv_modify_qp(qp, &attr,
                             IBV_QP_STATE |
                             IBV_QP_PKEY_INDEX |
                             IBV_QP_PORT |
                             IBV_QP_ACCESS_FLAGS));

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state              = IBV_QPS_RTR;
    attr.path_mtu		       = IBV_MTU_4096; //ok
    attr.dest_qp_num	       = remote_addr._qpn; //ok
    attr.rq_psn                = 0; //ok
    attr.max_dest_rd_atomic    = 16; //ok
    attr.min_rnr_timer         = 30;//30;
    attr.ah_attr.is_global     = 1;
    attr.ah_attr.grh.dgid      = remote_addr._gid;
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.sgid_index = remote_addr._gid_table_index;
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.dlid          = remote_addr._lid;
    attr.ah_attr.sl	     	   = 0;//mlx5_default_sl;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num	   = mlx5_default_port_num;
    IBV_CHKERR(ibv_modify_qp(qp, &attr,
                             IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                             IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                             IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER));

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.sq_psn        = 0; //ok
    attr.timeout       = 0; //ok
    attr.retry_cnt     = rnr_retry ? 7 : 0; //ok
    attr.rnr_retry     = rnr_retry ? 7 : 0; //ok
    attr.max_rd_atomic = 16; //ok
    IBV_CHKERR(ibv_modify_qp(qp, &attr,
                             IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                             IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                             IBV_QP_MAX_QP_RD_ATOMIC));
}

static void ib_qp_ud_transit_to_rts(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
	attr.qp_state   = IBV_QPS_INIT;
    attr.pkey_index = mlx5_default_pkey_index;
    attr.port_num   = mlx5_default_port_num;
    attr.qkey       = mlx5_default_qkey;
    IBV_CHKERR(ibv_modify_qp(qp, &attr,
                             IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                             IBV_QP_PORT | IBV_QP_QKEY));

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTR;
    IBV_CHKERR(ibv_modify_qp(qp, &attr, IBV_QP_STATE));

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0; // TODO: set me from remote addr!
    IBV_CHKERR(ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN));
}

static struct ep_addr
ib_qp_pack_addr(struct ibv_context *ctx, struct ibv_qp *qp)
{
    struct ep_addr addr;

    memset(&addr, 0, sizeof(addr));
    ib_ctx_query_params(ctx, addr);
    addr._qpn = qp->qp_num;

    return addr;
}

struct ibv_mr *
ib_mr_reg(struct ibv_pd *pd, void *buf, std::size_t len)
{
    struct ibv_mr *mr;
    IBV_CHKERR_PTR(ibv_reg_mr(pd, buf, len,
                              IBV_ACCESS_LOCAL_WRITE |
                              IBV_ACCESS_REMOTE_WRITE |
                              IBV_ACCESS_RELAXED_ORDERING),
                   mr);
    return mr;
}

void ib_mr_dereg(struct ibv_mr *mr)
{
    IBV_CHKERR(ibv_dereg_mr(mr));
}

static inline int
ib_cq_poll(struct ibv_cq *cq, std::size_t max_batch_size, struct ibv_wc *wcs)
{
    int ncomp = 0;
    do {
        ncomp = ibv_poll_cq(cq, max_batch_size, wcs);
    } while (ncomp == 0);

    if (ncomp < 0) {
        std::cerr << "ibv_poll_cq() failed" << std::endl;
        return -1;
    }

    for (auto i = 0; i < ncomp; i++) {
        if (wcs[i].status != IBV_WC_SUCCESS) {
            std::cerr << "WCE err=" << wcs[i].status << std::endl;
            return -1;
        }
    }

    return ncomp;
}

static inline void
ib_qp_post_recv(struct ibv_qp *qp, struct ibv_mr *mr,
                void *buf, std::size_t len, uint64_t id)
{
    struct ibv_sge sg;
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad_wr;

    if (len) {
        memset(&sg, 0, sizeof(sg));
        sg.addr	  = (uintptr_t)buf;
        sg.length = len;
        sg.lkey	  = mr->lkey;
    }

    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = id;
    wr.sg_list    = len ? &sg : nullptr;
    wr.num_sge    = len ? 1 : 0;

    IBV_CHKERR(ibv_post_recv(qp, &wr, &bad_wr));
}

static inline void
ib_qp_rc_post_send(struct ibv_qp *qp, struct ibv_mr *mr,
                   void *buf, std::size_t len, uint64_t id,
                   bool unsignaled)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    if (len) {
        memset(&sg, 0, sizeof(sg));
        sg.addr	  = (uintptr_t)buf;
        sg.length = len;
        sg.lkey	  = mr->lkey;
    }

    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = id;
    wr.sg_list    = len ? &sg : nullptr;
    wr.num_sge    = len ? 1 : 0;
    wr.opcode     = IBV_WR_SEND;
    wr.send_flags = unsignaled ? 0 : IBV_SEND_SIGNALED;

    IBV_CHKERR(ibv_post_send(qp, &wr, &bad_wr));
}

static inline void
ib_qp_rc_post_send_batch(struct ibv_qp *qp, struct ibv_mr *mr,
                         void *sbuf, std::size_t len, std::size_t batch_size,
                         struct ibv_sge *sge_pool,
                         struct ibv_send_wr *wrs_pool,
                         uint64_t id,
                         uint32_t send_start_idx)
{
    struct ibv_send_wr *bad_wr;

    for (std::size_t i = 0; i < batch_size; i++) {
        auto &sge = sge_pool[i];
        auto &wr = wrs_pool[i];

        memset(&sge, 0, sizeof(struct ibv_sge));
        sge.addr   = (uintptr_t)sbuf + len * i;
        sge.length = len;
        sge.lkey   = mr->lkey;

        memset(&wr, 0, sizeof(struct ibv_send_wr));
        wr.wr_id               = id;
        wr.next                = (i == (batch_size - 1)) ? nullptr : &wrs_pool[i+1];
        wr.sg_list             = &sge;
        wr.num_sge             = 1;
        wr.opcode              = IBV_WR_SEND_WITH_IMM;
        wr.imm_data            = send_start_idx++;
    }
    wrs_pool[batch_size - 1].send_flags = IBV_SEND_SIGNALED;

    IBV_CHKERR(ibv_post_send(qp, wrs_pool, &bad_wr));
}

static inline void
ib_qp_rc_post_write_with_imm(struct ibv_qp *qp,
                             struct ibv_mr *smr, void *sbuf,
                             uint64_t rkey, uint64_t rbuf,
                             std::size_t len,
                             uint64_t id)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    memset(&sg, 0, sizeof(sg));
    sg.addr	  = (uintptr_t)sbuf;
    sg.length = len;
    sg.lkey	  = smr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id               = id;
    wr.sg_list             = &sg;
    wr.num_sge             = 1;
    wr.opcode              = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.imm_data            = htonl(42);
    wr.wr.rdma.rkey        = rkey;
    wr.wr.rdma.remote_addr = rbuf;
    wr.send_flags          = IBV_SEND_SIGNALED;

    IBV_CHKERR(ibv_post_send(qp, &wr, &bad_wr));
}

static inline void
ib_qp_rc_post_write_with_imm_batch(struct ibv_qp *qp,
                                   struct ibv_mr *smr, void *sbuf,
                                   uint64_t rkey, uint64_t rbuf,
                                   std::size_t len, std::size_t batch_size,
                                   struct ibv_sge *sge_pool,
                                   struct ibv_send_wr *wrs_pool,
                                   uint64_t id,
                                   uint32_t write_start_idx)
{
    struct ibv_send_wr *bad_wr;

    for (std::size_t i = 0; i < batch_size; i++) {
        auto &sge = sge_pool[i];
        auto &wr = wrs_pool[i];

        memset(&sge, 0, sizeof(struct ibv_sge));
        sge.addr   = (uintptr_t)sbuf + len * i;
        sge.length = len;
        sge.lkey   = smr->lkey;

        memset(&wr, 0, sizeof(struct ibv_send_wr));
        wr.wr_id               = id;
        wr.next                = (i == (batch_size - 1)) ? nullptr : &wrs_pool[i+1];
        wr.sg_list             = &sge;
        wr.num_sge             = 1;
        wr.opcode              = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.imm_data            = write_start_idx++;
        wr.wr.rdma.rkey        = rkey;
        wr.wr.rdma.remote_addr = rbuf + len * i;
    }
    wrs_pool[batch_size - 1].send_flags = IBV_SEND_SIGNALED;

    IBV_CHKERR(ibv_post_send(qp, wrs_pool, &bad_wr));
}

static inline void
ib_qp_ud_post_send(struct ibv_qp *qp, int qpn, struct ibv_ah *ah,
                   struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    struct ibv_sge sg;
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad_wr;

    memset(&sg, 0, sizeof(sg));
    sg.addr	  = (uintptr_t)buf;
    sg.length = len;
    sg.lkey	  = mr->lkey;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id      = id;
    wr.sg_list    = &sg;
    wr.num_sge    = 1;
    wr.opcode     = IBV_WR_SEND;

    wr.wr.ud.ah          = ah;
    wr.wr.ud.remote_qpn  = qpn;
    wr.wr.ud.remote_qkey = mlx5_default_qkey;

    IBV_CHKERR(ibv_post_send(qp, &wr, &bad_wr));
}

ib_dev_ctx::ib_dev_ctx(const std::string &dev_name)
{
    ib_dev_create(dev_name, &_ctx, &_pd);
}

ib_dev_ctx::~ib_dev_ctx()
{
    ib_dev_destroy(_ctx, _pd);
}

struct ibv_mr* ib_dev_ctx::reg_mr(void *buf, std::size_t len)
{
    return ib_mr_reg(_pd, buf, len);
}

void ib_dev_ctx::dereg_mr(struct ibv_mr* mr)
{
    ib_mr_dereg(mr);
}

ib_cq_pe_ctx::ib_cq_pe_ctx(struct dev_ctx *dev, std::size_t poll_batch_size,
                           std::size_t idx, std::size_t q_len)
    :
    pe_ctx(poll_batch_size, idx, q_len),
    _wcs(poll_batch_size)
{
    struct ib_dev_ctx *_dev = static_cast<struct ib_dev_ctx *>(dev);
    ib_cq_create(_dev->_ctx, q_len, &_cq);
}

ib_cq_pe_ctx::~ib_cq_pe_ctx()
{
    ib_cq_destroy(_cq);
}

int ib_cq_pe_ctx::poll()
{
    auto ncomp = ib_cq_poll(_cq, _poll_batch_size, _wcs.data());
    assert(ncomp > 0);
    return ncomp;
}

int ib_cq_pe_ctx::poll(struct ibv_wc **wcs)
{
    auto ncomp = ib_cq_poll(_cq, _poll_batch_size, _wcs.data());
    assert(ncomp > 0);
    *wcs = _wcs.data(); // this is horrible
    return ncomp;
}

void ib_cq_pe_ctx::poll_worker(std::function<bool(struct ibv_wc *wc, struct bmark_ctx &ctx)> cb,
                               struct bmark_ctx &ctx)
{
    while (true) {
        auto ncomp = ib_cq_poll(_cq, _poll_batch_size, _wcs.data());
        assert(ncomp > 0);
        for (auto comp_id = 0; comp_id < ncomp; comp_id++) {
            if (cb(&_wcs[comp_id], ctx)) {
                return;
            }
        }
    }
}

ud_ep_ctx::ud_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe, std::size_t idx, std::size_t q_len)
    :
    ep_ctx(pe, idx, q_len)
{
    _dev = static_cast<struct ib_dev_ctx *>(dev);
    _pe = static_cast<struct ib_cq_pe_ctx *>(pe);
    ib_qp_create(_dev->_ctx, _dev->_pd, _pe->_cq, IBV_QPT_UD, q_len, &_qp);
    _addr = ib_qp_pack_addr(_dev->_ctx, _qp);
}

ud_ep_ctx::~ud_ep_ctx()
{
    ib_ah_destroy(_ah);
    ib_qp_destroy(_qp);
}

void ud_ep_ctx::connect(struct ep_addr remote_addr)
{
    ep_ctx::connect(remote_addr);
    ib_ah_create(_dev->_pd, _remote_addr, &_ah);
    ib_qp_ud_transit_to_rts(_qp);
}

inline void ud_ep_ctx::post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled)
{
    (void)unsignaled;
    ib_qp_ud_post_send(_qp, _remote_addr._qpn, _ah, mr, buf, len, id);
}

inline void ud_ep_ctx::post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    ib_qp_post_recv(_qp, mr, buf, len, id);
}

rc_ep_ctx::rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe, std::size_t idx, std::size_t q_len,
                     bool rnr_retry)
    :
    ep_ctx(pe, idx, q_len),
    _wrs_pool(q_len),
    _sge_pool(q_len),
    _rnr_retry(rnr_retry)
{
    _dev = static_cast<struct ib_dev_ctx *>(dev);
    _pe = static_cast<struct ib_cq_pe_ctx *>(pe);
    ib_qp_create(_dev->_ctx, _dev->_pd, _pe->_cq, IBV_QPT_RC, q_len, &_qp);
    _addr = ib_qp_pack_addr(_dev->_ctx, _qp);
}

rc_ep_ctx::~rc_ep_ctx()
{
    ib_qp_destroy(_qp);
}

void rc_ep_ctx::connect(struct ep_addr remote_addr)
{
    ep_ctx::connect(remote_addr);
    ib_qp_rc_transit_to_rts(_qp, remote_addr, _rnr_retry);
}

inline void rc_ep_ctx::post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled)
{
    ib_qp_rc_post_send(_qp, mr, buf, len, id, unsignaled);
}

inline void rc_ep_ctx::post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    ib_qp_post_recv(_qp, mr, buf, len, id);
}

inline void
rc_ep_ctx::post_write_with_imm(struct ibv_mr *smr, void *sbuf, uint64_t rkey, uint64_t rbuf,
                               std::size_t len, uint64_t id)
{
    ib_qp_rc_post_write_with_imm(_qp, smr, sbuf, rkey, rbuf, len, id);
}

inline void
rc_ep_ctx::post_send_batch(struct ibv_mr *smr, void *sbuf,
                           std::size_t len, std::size_t batch_size, uint64_t id,
                           uint32_t send_start_idx)
{
    assert(batch_size <= _q_len);
    ib_qp_rc_post_send_batch(_qp, smr, sbuf, len, batch_size, _sge_pool.data(),
                             _wrs_pool.data(), id, send_start_idx);
}

inline void
rc_ep_ctx::post_write_with_imm_batch(struct ibv_mr *smr, void *sbuf,
                                     uint64_t rkey, uint64_t rbuf,
                                     std::size_t len, std::size_t batch_size, uint64_t id,
                                     uint32_t write_start_idx)
{
    assert(batch_size <= _q_len);
    ib_qp_rc_post_write_with_imm_batch(_qp, smr, sbuf, rkey, rbuf, len,
                                       batch_size, _sge_pool.data(), _wrs_pool.data(), id,
                                       write_start_idx);
}

dummy_rc_ep_ctx::dummy_rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe)
    : rc_ep_ctx(dev, pe, 0, 0, false)
{
}

void dummy_rc_ep_ctx::connect(struct ep_addr remote_addr)
{
    rc_ep_ctx::connect(remote_addr);
}
