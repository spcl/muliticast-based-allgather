#include <cassert>
#include <net/if.h>
#include <infiniband/mlx5dv.h>
#include <libflexio/flexio.h>

#include "dpa_transport.hpp"

extern "C" struct flexio_app *flexio_ag_bench_app;
extern "C" flexio_func_t dpa_bringup_test_rpc;
extern "C" flexio_func_t dpa_main_eh;

static void ibv_staging_memory_alloc(struct ibv_context *ctx, struct flexio_process *proc,
                                     struct ibv_pd *pd, std::size_t alloc_size,
                                     struct ibv_dm **dm, struct ibv_mr **mr,
                                     struct flexio_mkey **dpa_mkey,
                                     uint64_t *ptr,
                                     int staging_mem_type)
{
    flexio_uintptr_t *dev_ptr;
    struct flexio_mkey_attr dpa_mkey_attr;
    struct ibv_alloc_dm_attr dm_attr;
    struct ibv_device_attr_ex dev_attr;

    switch (staging_mem_type) {
 
        case dpa_rc_staging_ep_ctx::MEM_TYPE_HOST:
            *dm       = nullptr;
            *ptr      = reinterpret_cast<uint64_t>(new (std::align_val_t(64)) char[alloc_size]);
            *mr       = ib_mr_reg(pd, reinterpret_cast<void *>(*ptr), alloc_size);
            *dpa_mkey = nullptr;
            break;

        case dpa_rc_staging_ep_ctx::MEM_TYPE_DPA:
            dev_ptr = reinterpret_cast<flexio_uintptr_t *>(ptr);
            FLEXIO_CHKERR(flexio_buf_dev_alloc(proc, alloc_size, dev_ptr));
            memset(&dpa_mkey_attr, 0, sizeof(dpa_mkey_attr));
            dpa_mkey_attr.pd     = pd;
            dpa_mkey_attr.daddr  = *dev_ptr;
            dpa_mkey_attr.len    = alloc_size;
            dpa_mkey_attr.access = IBV_ACCESS_LOCAL_WRITE  |
                                   IBV_ACCESS_REMOTE_WRITE |
                                   IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_RELAXED_ORDERING;
            FLEXIO_CHKERR(flexio_device_mkey_create(proc, &dpa_mkey_attr, dpa_mkey));
            *mr         = new struct ibv_mr;
            memset(*mr, 0, sizeof(struct ibv_mr));
            (*mr)->lkey = flexio_mkey_get_id(*dpa_mkey);
            (*mr)->rkey = (*mr)->lkey;
            break;
 
        case dpa_rc_staging_ep_ctx::MEM_TYPE_MEMIC:
            FLEXIO_CHKERR(ibv_query_device_ex(ctx, nullptr, &dev_attr));
            memset(&dm_attr, 0, sizeof(dm_attr));
            dm_attr.length = alloc_size;
            FLEXIO_CHKERR_PTR(ibv_alloc_dm(ctx, &dm_attr), *dm);
            FLEXIO_CHKERR_PTR(ibv_reg_dm_mr(pd, *dm, 0 /*offset*/,
                                            alloc_size,
                                            IBV_ACCESS_LOCAL_WRITE  |
                                            IBV_ACCESS_REMOTE_WRITE |
                                            IBV_ACCESS_REMOTE_READ  |
                                            IBV_ACCESS_ZERO_BASED),
                             *mr);
            *ptr = 0;
            break;

        default:
            std::cerr << "Unknown staging memory type requested" << std::endl;
            exit(EXIT_FAILURE);
            break;
    }
}

static void ibv_staging_memory_free(struct flexio_process *proc, struct ibv_dm *dm, struct ibv_mr *mr,
                                    struct flexio_mkey *dpa_mkey, uint64_t ptr, int staging_mem_type)
{
    switch (staging_mem_type) {
 
        case dpa_rc_staging_ep_ctx::MEM_TYPE_HOST:
            (void)proc;
            assert(dpa_mkey == nullptr);
            ib_mr_dereg(mr);
            delete[] reinterpret_cast<char *>(ptr);
            break;

        case dpa_rc_staging_ep_ctx::MEM_TYPE_DPA:
            delete mr;
            FLEXIO_CHKERR(flexio_device_mkey_destroy(dpa_mkey));
            FLEXIO_CHKERR(flexio_buf_dev_free(proc, reinterpret_cast<flexio_uintptr_t>(ptr)));
            break;

        case dpa_rc_staging_ep_ctx::MEM_TYPE_MEMIC:
            ib_mr_dereg(mr);
            IBV_CHKERR(ibv_free_dm(dm));
            break;

        default:
            std::cerr << "Unknown staging memory type requested" << std::endl;
            exit(EXIT_FAILURE);
            break;
    }
}

static void dpa_process_destroy(struct flexio_process *proc, struct flexio_msg_stream *stream)
{
    FLEXIO_CHKERR(flexio_msg_stream_destroy(stream));
    FLEXIO_CHKERR(flexio_process_destroy(proc));
}

static void
dpa_process_create(struct ibv_context *ctx, struct flexio_process **proc, struct flexio_uar **uar,
                   struct flexio_msg_stream **stream)
{
    FLEXIO_CHKERR(flexio_process_create(ctx, flexio_ag_bench_app, nullptr, proc));
    FLEXIO_CHKERR_PTR(flexio_process_get_uar(*proc), *uar);
    flexio_msg_stream_attr_t stream_fattr;
    memset(&stream_fattr, 0, sizeof(stream_fattr));
    stream_fattr.data_bsize = flexio_stream_buff_size;
    stream_fattr.sync_mode = FLEXIO_LOG_DEV_SYNC_MODE_SYNC;
    FLEXIO_CHKERR(flexio_msg_stream_create(*proc, &stream_fattr, stdout, nullptr, stream));
}

static void dpa_rpc_call(struct flexio_process *proc,
                         flexio_func_t *rpc,
                         uint64_t *ret,
                         flexio_uintptr_t app_ctx)
{
    FLEXIO_CHKERR(flexio_process_call(proc, rpc, ret, app_ctx));
}

flexio_uintptr_t dpa_alloc_qp_wq_buff(struct flexio_process *process, 
                                      int log_rq_depth, flexio_uintptr_t *rq_daddr,
				                      int log_sq_depth, flexio_uintptr_t *sq_daddr)
{
	flexio_uintptr_t buff_daddr;
	size_t buff_bsize = 0;
	size_t rq_bsize = 0;
	size_t sq_bsize = 0;

	if (rq_daddr) {
		*rq_daddr = 0;
		rq_bsize = L2V(log_rq_depth) * sizeof(struct mlx5_wqe_data_seg);
		buff_bsize += rq_bsize;
	}

	if (sq_daddr) {
		*sq_daddr = 0;
		sq_bsize = L2V(log_sq_depth + LOG_FLEXIO_SWQE_BSIZE);
		buff_bsize += sq_bsize;
	}

	if (flexio_buf_dev_alloc(process, buff_bsize, &buff_daddr))
		return 0;

	/* buff starts from RQ (if exists) and after this - SQ (if exists) */
	if (rq_daddr)
		*rq_daddr = buff_daddr;

	if (sq_daddr)
		*sq_daddr = buff_daddr + rq_bsize;
	return buff_daddr;
}

flexio_uintptr_t dpa_alloc_dpa_dbr(struct flexio_process *process)
{
	flexio_uintptr_t dbr_daddr;
	__be32 dbr[2] = { 0, 0 };

	dbr_daddr = 0;
	FLEXIO_CHKERR(flexio_copy_from_host(process, dbr, sizeof(dbr), &dbr_daddr));

	return dbr_daddr;
}

flexio_uintptr_t dpa_alloc_dpa_cq_ring(struct flexio_process *process, int log_depth)
{
	struct mlx5_cqe64 *cq_ring_src, *cqe;
	size_t ring_bsize;
	flexio_uintptr_t ring_daddr;
	int i, err, num_of_cqes;

	num_of_cqes = L2V(log_depth);
	ring_bsize = num_of_cqes * L2V(LOG_FLEXIO_CQE_BSIZE);

	cq_ring_src = reinterpret_cast<struct mlx5_cqe64*>(calloc(num_of_cqes, L2V(LOG_FLEXIO_CQE_BSIZE)));
	assert(cq_ring_src);

	cqe = cq_ring_src;
	/* Init CQEs and set ownership bit */
	for (i = 0; i < num_of_cqes; i++)
		mlx5dv_set_cqe_owner(cqe++, 1);

	/* Copy CQEs from host to Flex IO CQ ring */
	err = flexio_copy_from_host(process, cq_ring_src, ring_bsize, &ring_daddr);
	if (err)
		ring_daddr = 0;

	free(cq_ring_src);
	return ring_daddr;
}

static void 
dpa_cq_create(struct flexio_process *proc, struct flexio_uar *uar,
              flexio_uintptr_t *cq_dbr_daddr, flexio_uintptr_t *cq_ring_daddr,
              struct flexio_event_handler *eh, struct flexio_cq **cq,
              std::size_t log_depth)
{
    FLEXIO_CHKERR_PTR(dpa_alloc_dpa_dbr(proc),
                      *cq_dbr_daddr);
    FLEXIO_CHKERR_PTR(dpa_alloc_dpa_cq_ring(proc, log_depth),
                      *cq_ring_daddr);

    struct flexio_cq_attr cq_fattr;
    memset(&cq_fattr, 0, sizeof(cq_fattr));
    if (eh) {
        //cq_fattr.cqe_comp_type  = FLEXIO_CQE_COMP_ENH; // idk why do we need this for now?
        cq_fattr.element_type   = FLEXIO_CQ_ELEMENT_TYPE_DPA_THREAD;
        cq_fattr.thread         = flexio_event_handler_get_thread(eh);
    } else {
        cq_fattr.element_type   = FLEXIO_CQ_ELEMENT_TYPE_NON_DPA_CQ;
    }
    cq_fattr.cq_dbr_daddr       = *cq_dbr_daddr;
    cq_fattr.cq_ring_qmem.daddr = *cq_ring_daddr;
    cq_fattr.log_cq_depth       = log_depth;
    cq_fattr.uar_id             = flexio_uar_get_id(uar);
    FLEXIO_CHKERR(flexio_cq_create(proc, nullptr, &cq_fattr, cq));
}

static void
dpa_cq_destroy(struct flexio_process *proc, struct flexio_cq *cq,
               flexio_uintptr_t cq_dbr_daddr, flexio_uintptr_t cq_ring_daddr)
{
    FLEXIO_CHKERR(flexio_cq_destroy(cq));
    FLEXIO_CHKERR(flexio_buf_dev_free(proc, cq_dbr_daddr));
    FLEXIO_CHKERR(flexio_buf_dev_free(proc, cq_ring_daddr));
}

static void
dpa_qp_create(struct ibv_pd *pd, struct flexio_uar *uar,
              struct flexio_process *proc, struct flexio_cq *cq,
              flexio_uintptr_t *qp_dbr_daddr,
              flexio_uintptr_t *wq_daddr,
              flexio_uintptr_t *qp_rq_daddr,
              flexio_uintptr_t *qp_sq_daddr,
              std::size_t log_depth,
              struct flexio_qp **qp,
              uint8_t qp_type)
{
    FLEXIO_CHKERR_PTR(dpa_alloc_dpa_dbr(proc),
                      *qp_dbr_daddr);
    FLEXIO_CHKERR_PTR(dpa_alloc_qp_wq_buff(proc,
                                            log_depth, qp_rq_daddr,
                                            log_depth, qp_sq_daddr),
                      *wq_daddr);

    struct flexio_qp_attr flexio_qp_fattr;
    memset(&flexio_qp_fattr, 0, sizeof(flexio_qp_fattr));
    flexio_qp_fattr.transport_type         = qp_type;
	flexio_qp_fattr.log_sq_depth           = log_depth;
    flexio_qp_fattr.log_rq_depth           = log_depth;
	flexio_qp_fattr.qp_wq_buff_qmem.daddr  = *wq_daddr;
    flexio_qp_fattr.qp_wq_dbr_qmem.memtype = FLEXIO_MEMTYPE_DPA,
	flexio_qp_fattr.qp_wq_dbr_qmem.daddr   = *qp_dbr_daddr;
	flexio_qp_fattr.uar_id                 = flexio_uar_get_id(uar);
	flexio_qp_fattr.rq_cqn                 = flexio_cq_get_cq_num(cq);
	flexio_qp_fattr.sq_cqn                 = flexio_qp_fattr.rq_cqn;
	flexio_qp_fattr.rq_type                = FLEXIO_QP_QPC_RQ_TYPE_REGULAR;
	flexio_qp_fattr.pd                     = pd;
	flexio_qp_fattr.qp_access_mask         = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
	flexio_qp_fattr.ops_flag               = FLEXIO_QP_WR_RDMA_WRITE | FLEXIO_QP_WR_RDMA_READ |
        FLEXIO_QP_WR_ATOMIC_CMP_AND_SWAP;
	FLEXIO_CHKERR(flexio_qp_create(proc, nullptr, &flexio_qp_fattr, qp));
}

static void
dpa_qp_destroy(struct flexio_process *proc, struct flexio_qp *qp,
               flexio_uintptr_t qp_dbr_daddr,
               flexio_uintptr_t wq_daddr)
{
    FLEXIO_CHKERR(flexio_qp_destroy(qp));
    FLEXIO_CHKERR(flexio_buf_dev_free(proc, wq_daddr));
    FLEXIO_CHKERR(flexio_buf_dev_free(proc, qp_dbr_daddr));
}

static void
dpa_rq_dbr_ring(struct flexio_process *proc, flexio_uintptr_t dbr_daddr,
                uint32_t rcv_counter, uint32_t send_counter)
{
    (void)send_counter;
    __be32 dbr;
	dbr = htobe32(rcv_counter & 0xffff);
	FLEXIO_CHKERR(flexio_host2dev_memcpy(proc, &dbr, sizeof(dbr), dbr_daddr));
}

uint32_t get_wqe_idx(uint32_t xq_log_depth, uint32_t pi_idx)
{
    int mask = L2M(xq_log_depth);

    if ((pi_idx & mask) == 0) {
        return 0;
    } else {
        return pi_idx % L2V(xq_log_depth);
    }
}

static void dpa_rq_post(struct flexio_process *proc,
                        flexio_uintptr_t rq_ring_daddr,
                        flexio_uintptr_t rq_dbr_daddr,
                        uint32_t rq_log_depth,
                        uint32_t *pi_idx,
                        uint32_t rq_data_lkey,
                        std::size_t rq_data_size,
                        uint64_t rq_data_ptr)
{
    auto wqe_idx = get_wqe_idx(rq_log_depth, *pi_idx);
    auto rq_daddr = rq_ring_daddr + sizeof(struct mlx5_wqe_data_seg) * wqe_idx;
	struct mlx5_wqe_data_seg new_segm;
    mlx5dv_set_data_seg(&new_segm, rq_data_size, rq_data_lkey, rq_data_ptr);
	FLEXIO_CHKERR(flexio_host2dev_memcpy(proc, &new_segm, sizeof(new_segm), rq_daddr));
    dpa_rq_dbr_ring(proc, rq_dbr_daddr, ++(*pi_idx), 0);
}

static struct ep_addr
dpa_qp_pack_addr(struct ibv_context *ctx, struct flexio_qp *qp)
{
    struct ep_addr addr;

    memset(&addr, 0, sizeof(addr));
    assert(ib_ctx_query_params(ctx, addr));
    addr._qpn = flexio_qp_get_qp_num(qp);    

    return addr;
}

static void
dpa_qp_rc_transit_to_rts(struct flexio_qp *qp, struct ep_addr &remote_addr)
{
	struct flexio_qp_attr_opt_param_mask qp_fattr_opt_param_mask;
	struct flexio_qp_attr qp_fattr;

	memset(&qp_fattr, 0, sizeof(qp_fattr));
	memset(&qp_fattr_opt_param_mask, 0, sizeof(qp_fattr_opt_param_mask));
	qp_fattr.remote_qp_num     = remote_addr._qpn;
	qp_fattr.dest_mac          = remote_addr._mac_addr;
	qp_fattr.rgid_or_rip       = remote_addr._gid;
	qp_fattr.gid_table_index   = remote_addr._gid_table_index;
	qp_fattr.rlid              = remote_addr._lid;
	qp_fattr.fl                = 0;
	qp_fattr.min_rnr_nak_timer = 1;
	qp_fattr.path_mtu          = FLEXIO_QP_QPC_MTU_BYTES_4K;
	qp_fattr.retry_count       = 0;
	qp_fattr.vhca_port_num     = 0x1;
	qp_fattr.udp_sport         = 0xc000;
	qp_fattr.grh               = 0x1;
    qp_fattr.qp_access_mask    = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
	qp_fattr.next_state = FLEXIO_QP_STATE_INIT;
    FLEXIO_CHKERR(flexio_qp_modify(qp, &qp_fattr, &qp_fattr_opt_param_mask));
	qp_fattr.next_state = FLEXIO_QP_STATE_RTR;
	FLEXIO_CHKERR(flexio_qp_modify(qp, &qp_fattr, &qp_fattr_opt_param_mask));
    qp_fattr.next_state = FLEXIO_QP_STATE_RTS;
    FLEXIO_CHKERR(flexio_qp_modify(qp, &qp_fattr, &qp_fattr_opt_param_mask));
}

dpa_dev_ctx::dpa_dev_ctx(std::string device_name)
    :
    ib_dev_ctx(device_name)
{
    dpa_process_create(_ctx, &_process, &_dpa_uar, &_msg_stream);
    FLEXIO_CHKERR(flexio_window_create(_process, _pd, &_window));

    FLEXIO_CHKERR(flexio_buf_dev_alloc(_process, sizeof(_export_data), &_export_data_daddr));
    flexio_uintptr_t verb_daddr;
    FLEXIO_CHKERR(flexio_buf_dev_alloc(_process, sizeof(uint64_t), &verb_daddr));

    memset(&_export_data, 0, sizeof(_export_data));
    _export_data.verb = verb_daddr;
    _export_data.window_id = flexio_window_get_id(_window);

    uint64_t test_ret;
    dpa_rpc_call(_process, &dpa_bringup_test_rpc, &test_ret, 0);
    assert(test_ret == DPA_BRINGUP_TEST_MAGICNUM);
}

dpa_dev_ctx::~dpa_dev_ctx()
{
    FLEXIO_CHKERR(flexio_buf_dev_free(_process, _export_data.verb));
    FLEXIO_CHKERR(flexio_buf_dev_free(_process, _export_data_daddr));
    FLEXIO_CHKERR(flexio_window_destroy(_window));
    dpa_process_destroy(_process, _msg_stream);
}

void dpa_dev_ctx::stream_flush()
{
    flexio_msg_stream_flush(_msg_stream);
}
  
void dpa_dev_ctx::dev_ctx_flush()
{
    FLEXIO_CHKERR(flexio_host2dev_memcpy(_process, &_export_data, sizeof(_export_data),
                                         _export_data_daddr));
}

void dpa_dev_ctx::app_ctx_alloc(std::size_t max_app_ctx_size)
{
    flexio_uintptr_t app_ctx_daddr;
    FLEXIO_CHKERR(flexio_buf_dev_alloc(_process, max_app_ctx_size, &app_ctx_daddr));
    _export_data.app_ctx = app_ctx_daddr;
    dev_ctx_flush();
}

void dpa_dev_ctx::app_ctx_free()
{
    FLEXIO_CHKERR(flexio_buf_dev_free(_process, _export_data.app_ctx));
}

void dpa_dev_ctx::app_ctx_export(uint64_t app_id, void *app_ctx, std::size_t app_ctx_size)
{
    FLEXIO_CHKERR(flexio_host2dev_memcpy(_process, &app_id, sizeof(app_id), _export_data.verb));
    FLEXIO_CHKERR(flexio_host2dev_memcpy(_process, app_ctx, app_ctx_size, _export_data.app_ctx));
}

uint64_t dpa_dev_ctx::rpc_call(flexio_func_t &rpc)
{
    uint64_t ret;
    dpa_rpc_call(_process, &rpc, &ret, _export_data_daddr);
    return ret;
}

dpa_pe_ctx::dpa_pe_ctx(struct dev_ctx *dev, std::size_t poll_batch_size,
                       std::size_t idx, std::size_t q_len, bool active)
    :
    pe_ctx(poll_batch_size, idx, q_len),
    _eh(nullptr)
{
    assert(idx < MAX_FLEXIO_DEV_CQS);
    _dev = static_cast<struct dpa_dev_ctx *>(dev);

    if (active) {
        struct flexio_event_handler_attr eh_attr;
        memset(&eh_attr, 0, sizeof(eh_attr));
        eh_attr.host_stub_func = dpa_main_eh;
        eh_attr.affinity.type  = FLEXIO_AFFINITY_STRICT;
        eh_attr.affinity.id    = idx >= LOOPBACK_CQQP_ARRAY_BASE_IDX ? // check datapath layout in macro definition
            (idx - LOOPBACK_CQQP_ARRAY_BASE_IDX) : idx;
        FLEXIO_CHKERR(flexio_event_handler_create(_dev->_process, &eh_attr, &_eh));
    }

    _cq_transfer.log_cq_depth = FAST_LOG2_UP(q_len);
    _cq_transfer.ci_idx = 0;
    _cq_transfer.hw_owner_bit = 0x1;
    _cq_transfer.poll_batch_size = poll_batch_size;

    dpa_cq_create(_dev->_process, _dev->_dpa_uar,
                  &_cq_transfer.cq_dbr_daddr, &_cq_transfer.cq_ring_daddr,
                  active ? _eh : nullptr,
                  &_cq, _cq_transfer.log_cq_depth);

    _cq_transfer.cq_num = flexio_cq_get_cq_num(_cq);

    _dev->_export_data.cq_transfer[idx] = _cq_transfer;
    _dev->_export_data.n_cqs++;

    _dev->dev_ctx_flush();

    if (active) {
        FLEXIO_CHKERR(flexio_event_handler_run(_eh, _dev->_export_data_daddr));
    }
}

dpa_pe_ctx::~dpa_pe_ctx()
{
    dpa_cq_destroy(_dev->_process, _cq,
                   _cq_transfer.cq_dbr_daddr,
                   _cq_transfer.cq_ring_daddr);
    if (_eh) {
        flexio_event_handler_destroy(_eh);
    }
}

int dpa_pe_ctx::poll()
{
    return 0;
}

void dpa_pe_ctx::poll_worker(std::function<bool(struct ibv_wc *wc, struct bmark_ctx &ctx)> cb,
                             struct bmark_ctx &ctx)
{
    (void)cb;
    (void)ctx;
    FLEXIO_CHKERR(1); // fatal
    return;
}

// TODO: make it internal EP type
dpa_rc_ep_ctx::dpa_rc_ep_ctx(struct dev_ctx *dev, struct pe_ctx *pe,
                             std::size_t idx, std::size_t q_len,
                             bool rnr_retry,
                             bool root,                // only root creates loopback/host cq/qp
                             bool active_loopback,     // activate DPA thread on loopback ops
                             bool remote_loopback_end) // we will connect it to the remote side
    :
    ep_ctx(pe, idx, q_len),
    _host_cq(nullptr),
    _host_qp(nullptr),
    _loopback_dpa_cq(nullptr),
    _loopback_dpa_qp(nullptr)
{
    assert(idx < MAX_FLEXIO_QPS);

    if (root) {
        _loopback_dpa_cq = new struct dpa_pe_ctx(dev, _q_len,
                                                 LOOPBACK_CQQP_ARRAY_BASE_IDX + idx,
                                                 _q_len,
                                                 active_loopback);
        _loopback_dpa_qp = new struct dpa_rc_ep_ctx(dev,
                                                    static_cast<struct pe_ctx *>(_loopback_dpa_cq),
                                                    LOOPBACK_CQQP_ARRAY_BASE_IDX + idx,
                                                    _q_len,
                                                    false, // no rnr retry
                                                    false); // not root
        ep_ctx::_loopback_ep_addr = _loopback_dpa_qp->_addr;
        if (!remote_loopback_end) {
            _host_cq = new struct ib_cq_pe_ctx(dev,
                                               _q_len,
                                               HOST_CQQP_ARRAY_BASE_IDX + idx,
                                               _q_len);
            _host_qp = new struct rc_ep_ctx(dev,
                                            static_cast<struct pe_ctx *>(_host_cq),
                                            HOST_CQQP_ARRAY_BASE_IDX + idx,
                                            _q_len,
                                            rnr_retry);
            _loopback_dpa_qp->connect(_host_qp->_addr);
            _host_qp->connect(_loopback_dpa_qp->_addr);
        }
    }

    _dev = static_cast<struct dpa_dev_ctx *>(dev);
    _pe = static_cast<struct dpa_pe_ctx *>(pe);

    dpa_qp_create(_dev->_pd, _dev->_dpa_uar, _dev->_process,
                  _pe->_cq,
                  &_qp_transfer.qp_dbr_daddr,
                  &_wq_daddr,
                  &_qp_transfer.qp_rq_daddr,
                  &_qp_transfer.qp_sq_daddr,
                  FAST_LOG2_UP(_q_len),
                  &_qp, FLEXIO_QPC_ST_RC);

    _qp_transfer.log_qp_sq_depth = FAST_LOG2_UP(_q_len);
    _qp_transfer.log_qp_rq_depth = FAST_LOG2_UP(_q_len);

    _qp_transfer.qp_num = flexio_qp_get_qp_num(_qp);

    _addr = dpa_qp_pack_addr(_dev->_ctx, _qp);

    _qp_transfer.rq_pi_idx = 0;
    _qp_transfer.sq_pi_idx = 0;
    _dev->_export_data.n_qps++;
    _dev->_export_data.qp_transfer[idx] = _qp_transfer;

    _dev->dev_ctx_flush();
}

dpa_rc_ep_ctx::~dpa_rc_ep_ctx()
{
    if (_loopback_dpa_qp)
        delete _loopback_dpa_qp;
    if (_loopback_dpa_cq)
        delete _loopback_dpa_cq;
    if (_host_qp)
        delete _host_qp;
    if (_host_cq)
        delete _host_cq;

    dpa_qp_destroy(_dev->_process, _qp, _qp_transfer.qp_dbr_daddr, _wq_daddr);
}

void dpa_rc_ep_ctx::connect(struct ep_addr remote_addr)
{
    ep_ctx::connect(remote_addr);
    dpa_qp_rc_transit_to_rts(_qp, remote_addr);
}

void dpa_rc_ep_ctx::post_send(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id, bool unsignaled)
{
    (void) mr;
    (void) buf;
    (void) len;
    (void) id;
    (void) unsignaled;
}

void dpa_rc_ep_ctx::post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    (void)id; // TODO: set WR ID

    dpa_rq_post(_dev->_process,
                _qp_transfer.qp_rq_daddr,
                _qp_transfer.qp_dbr_daddr,
                FAST_LOG2_UP(_q_len),
                &_qp_transfer.rq_pi_idx,
                mr ? mr->lkey : 0,
                mr ? len : 0,
                reinterpret_cast<uint64_t>(buf));

    _dev->_export_data.qp_transfer[_idx] = _qp_transfer;
}

void dpa_rc_ep_ctx::post_send_host2dpa(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    _host_qp->post_send(mr, buf, len, id, false); // FIXME: unsignaled send fails with bad wr
    _host_cq->poll();
}

void dpa_rc_ep_ctx::post_recv_host2dpa(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    _loopback_dpa_qp->post_recv(mr, buf, len, id);
}

int dpa_rc_staging_ep_ctx::str2memtype(const std::string &str_mem_type) {
    if (!str_mem_type.compare("host"))
        return dpa_rc_staging_ep_ctx::MEM_TYPE_HOST;
    else if (!str_mem_type.compare("dpa"))
        return dpa_rc_staging_ep_ctx::MEM_TYPE_DPA;
    else if (!str_mem_type.compare("memic"))
        return dpa_rc_staging_ep_ctx::MEM_TYPE_MEMIC;
    return -1;
}

dpa_rc_staging_ep_ctx::dpa_rc_staging_ep_ctx(struct dev_ctx *dev,
                                             struct pe_ctx *pe,
                                             std::size_t idx,
                                             std::size_t q_len,
                                             bool rnr_retry,
                                             int staging_mem_type,
                                             bool remote_loopback_end)
    :
    dpa_rc_ep_ctx(dev, pe, idx, q_len, rnr_retry,
                  true,  // create root EP (e.g. with host-FlexIO loopback)
                  false, // passive loopback
                  remote_loopback_end), 
    _max_pkt_size(mlx5_max_mtu),
    _staging_rq_pi(0),
    _staging_rq_size(_q_len),
    _staging_mem_type(staging_mem_type)
{
    ibv_staging_memory_alloc(static_cast<struct ib_dev_ctx *>(_dev)->_ctx,
                             _dev->_process,
                             static_cast<struct ib_dev_ctx *>(_dev)->_pd,
                             _staging_rq_size * _max_pkt_size,
                             &_staging_dm, &_staging_mr, &_staging_dpa_mkey, &_staging_addr,
                             _staging_mem_type);
    _loopback_dpa_qp->_qp_transfer.rqd_lkey  = _staging_mr->lkey;
    _loopback_dpa_qp->_qp_transfer.rqd_daddr = _staging_addr;
    _dev->_export_data.qp_transfer[LOOPBACK_CQQP_ARRAY_BASE_IDX + idx] = _loopback_dpa_qp->_qp_transfer;
    _dev->dev_ctx_flush();
}

dpa_rc_staging_ep_ctx::~dpa_rc_staging_ep_ctx()
{
    ibv_staging_memory_free(_dev->_process, _staging_dm, _staging_mr, _staging_dpa_mkey, _staging_addr, _staging_mem_type);
}

void
dpa_rc_staging_ep_ctx::post_recv(struct ibv_mr *mr, void *buf, std::size_t len, uint64_t id)
{
    assert(!mr);
    assert(!buf);
    assert(_staging_rq_pi <= _staging_rq_size);
    auto ptr = _staging_addr + _staging_rq_pi * len;
    dpa_rc_ep_ctx::post_recv(_staging_mr, reinterpret_cast<void *>(ptr), len, id);
    _staging_rq_pi = (_staging_rq_pi + 1) % _staging_rq_size;
}

void dpa_rc_staging_ep_ctx::connect_to_hmem_daemon(struct ep_addr hmem_daemon_addr)
{
    _loopback_dpa_qp->connect(hmem_daemon_addr);
}