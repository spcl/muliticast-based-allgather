#include "tl_spin.h"
#include "tl_spin_rcache.h"
#include "tl_spin_coll.h"

#define UCC_TL_SPIN_MCAST_MAX_MTU_COUNT 5
int mtu_lookup[UCC_TL_SPIN_MCAST_MAX_MTU_COUNT][2] = {
    {256,  IBV_MTU_256},
    {512,  IBV_MTU_512},
    {1024, IBV_MTU_1024},
    {2048, IBV_MTU_2048},
    {4096, IBV_MTU_4096}
};

static ucc_status_t ucc_tl_spin_init_p2p_context(ucc_tl_spin_context_t *ctx)
{
    ucc_status_t               status          = UCC_OK;
    ucc_base_lib_t            *lib             = ctx->super.super.lib;
    ucc_tl_spin_p2p_context_t *p2p_ctx         = &ctx->p2p;
    struct ibv_device        **device_list     = NULL;
    struct ibv_device         *dev             = NULL;
    char                      *devname         = NULL;
    char                      *ib              = NULL;
    char                      *ib_name         = NULL;
    char                      *port            = NULL;
    //struct ibv_gid_entry      *gid_tbl_entries = NULL;
    struct ibv_port_attr       port_attr;
    int                        num_devices;//, num_gid_tbl_entries;
    int                        i;
    int                        ib_valid;
    //int                        gid_found;

    device_list = ibv_get_device_list(&num_devices);
    if (!device_list || !num_devices) {
        tl_error(lib, "no ib devices available");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }

    if (!strcmp(ctx->cfg.ib_dev_name, "")) {
        dev          = device_list[0];
        devname      = (char *)ibv_get_device_name(dev);
        ctx->devname = ucc_malloc(strlen(devname)+3, "devname");
        if (!ctx->devname) {
            status = UCC_ERR_NO_MEMORY;
            goto free_dev_list;
        }
        memset(ctx->devname, 0, strlen(devname)+3);
        memcpy(ctx->devname, devname, strlen(devname));
        strncat(ctx->devname, ":1", 3);
    } else {
        ib_valid = 0;
        /* user has provided the devname now make sure it is valid */
        for (i = 0; device_list[i]; ++i) {
            if (!strcmp(ibv_get_device_name(device_list[i]), ctx->cfg.ib_dev_name)) {
                ib_valid = 1;
                break;
            }
        }
        if (!ib_valid) {
            tl_error(lib, "ib device %s not found", ctx->cfg.ib_dev_name);
            status = UCC_ERR_NOT_FOUND;
            goto free_dev_list;
        }
        ctx->devname = ctx->cfg.ib_dev_name;
    }

    ib = strdup(ctx->devname);
    ucc_string_split(ib, ":", 2, &ib_name, &port);
    ctx->ib_port = atoi(port);
    ucc_free(ib);

    UCC_TL_SPIN_CHK_PTR(lib, ibv_open_device(dev), p2p_ctx->dev, 
                        status, UCC_ERR_NO_RESOURCE, free_dev_list);

    UCC_TL_SPIN_CHK_ERR(lib, ibv_query_port(p2p_ctx->dev, 
                                            ctx->ib_port,
                                            &port_attr),
                        status, UCC_ERR_NO_RESOURCE, free_dev_list);
    if (port_attr.state != IBV_PORT_ACTIVE) {
        tl_error(lib, "ib device %s port %d is not in the active state", 
                 ctx->cfg.ib_dev_name, ctx->ib_port);
        status = UCC_ERR_NO_RESOURCE;
        goto close_dev;
    }

    p2p_ctx->dev_addr.port_num = ctx->ib_port;
    p2p_ctx->dev_addr.mtu      = port_attr.active_mtu;
    p2p_ctx->dev_addr.lid      = port_attr.lid;

    UCC_TL_SPIN_CHK_PTR(lib,
                        ibv_alloc_pd(p2p_ctx->dev), p2p_ctx->pd, 
                        status, UCC_ERR_NO_RESOURCE, close_dev);

    status = tl_spin_rcache_create(p2p_ctx->pd, &p2p_ctx->rcache);
    if (UCC_OK != status) {
        tl_debug(lib, "failed to create p2p rcache");
        goto close_dev;
    }

    tl_debug(lib, "p2p context setup complete: ctx %p", p2p_ctx);

close_dev:
    if (status != UCC_OK) {
        ibv_close_device(p2p_ctx->dev);
    }
free_dev_list:
    if ((status != UCC_OK) && !strcmp(ctx->cfg.ib_dev_name, "")) {
        ucc_free(ctx->devname);
    }
    ibv_free_device_list(device_list);
out:
    return status;
}

static ucc_status_t ucc_tl_spin_init_mcast_context(ucc_tl_spin_context_t *ctx)
{
    ucc_status_t                 status      = UCC_OK;
    int                          is_ipv4     = 0;
    struct sockaddr_in          *in_src_addr = NULL;
    struct rdma_cm_event        *revent      = NULL;
    int                          active_mtu  = 4096;
    int                          max_mtu     = 4096;
    ucc_tl_spin_mcast_context_t *mcast_ctx   = &ctx->mcast;
    ucc_base_lib_t              *lib         = ctx->super.super.lib;
    struct ibv_port_attr         port_attr;
    struct ibv_device_attr       device_attr;
    struct sockaddr_storage      ip_oib_addr;
    struct sockaddr_storage      dst_addr;
    char                         addrstr[128];
    int                          i;
    const char                  *dst;

    status = ucc_tl_mlx5_probe_ip_over_ib(ctx->devname, &ip_oib_addr);
    if (UCC_OK != status) {
        tl_debug(lib, "failed to get ipoib interface for devname %s", ctx->devname);
        goto error;
    }

    is_ipv4     = (ip_oib_addr.ss_family == AF_INET) ? 1 : 0;
    in_src_addr = (struct sockaddr_in*)&ip_oib_addr;

    dst = inet_ntop((is_ipv4) ? AF_INET : AF_INET6,
                    &in_src_addr->sin_addr, addrstr, sizeof(addrstr) - 1);
    if (NULL == dst) {
        tl_error(lib, "inet_ntop failed");
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    tl_debug(lib, "devname %s, ipoib %s", ctx->devname, addrstr);

    mcast_ctx->channel = rdma_create_event_channel();
    if (!mcast_ctx->channel) {
        tl_debug(lib, "rdma_create_event_channel failed, errno %d", errno);
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    memset(&dst_addr, 0, sizeof(struct sockaddr_storage));
    dst_addr.ss_family = is_ipv4 ? AF_INET : AF_INET6;
    if (rdma_create_id(mcast_ctx->channel, &mcast_ctx->id, NULL, RDMA_PS_UDP)) {
        tl_debug(lib, "failed to create rdma id, errno %d", errno);
        status = UCC_ERR_NOT_SUPPORTED;
        goto error;
    }

    if (0 != rdma_resolve_addr(mcast_ctx->id, (struct sockaddr *)&ip_oib_addr,
                               (struct sockaddr *) &dst_addr, 1000)) {
        tl_debug(lib, "failed to resolve rdma addr, errno %d", errno);
        status = UCC_ERR_NOT_SUPPORTED;
        goto error;
    }

    if (rdma_get_cm_event(mcast_ctx->channel, &revent) < 0) {
        tl_error(lib, "failed to get cm event, errno %d", errno);
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    } else if (revent->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        tl_error(lib, "cm event is not resolved");
        if (rdma_ack_cm_event(revent) < 0) {
            tl_error(lib, "rdma_ack_cm_event failed");
        }
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    if (rdma_ack_cm_event(revent) < 0) {
        tl_error(lib, "rdma_ack_cm_event failed");
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    mcast_ctx->dev = mcast_ctx->id->verbs;
    mcast_ctx->pd  = ibv_alloc_pd(mcast_ctx->dev);
    if (!mcast_ctx->pd) {
        tl_error(lib, "failed to allocate pd");
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    /* Determine MTU */
    if (ibv_query_port(mcast_ctx->dev, ctx->ib_port, &port_attr)) {
        tl_error(lib, "couldn't query port in ctx create, errno %d", errno);
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    for (i = 0; i < UCC_TL_SPIN_MCAST_MAX_MTU_COUNT; i++) {
        if (mtu_lookup[i][1] == port_attr.max_mtu) {
            max_mtu = mtu_lookup[i][0];
        }
        if (mtu_lookup[i][1] == port_attr.active_mtu) {
            active_mtu = mtu_lookup[i][0];
        }
    }

    mcast_ctx->mtu = active_mtu;
    mcast_ctx->gid = 0;

    tl_debug(lib, "port active MTU is %d and port max MTU is %d",
             active_mtu, max_mtu);

    if (port_attr.max_mtu < port_attr.active_mtu) {
        tl_debug(lib, "port active MTU (%d) is smaller than port max MTU (%d)",
                 active_mtu, max_mtu);
    }

    if (ibv_query_device(mcast_ctx->dev, &device_attr)) {
        tl_error(lib, "failed to query device in ctx create, errno %d", errno);
        status = UCC_ERR_NO_RESOURCE;
        goto error;
    }

    tl_debug(lib, "MTU %d, MAX QP WR: %d, max srq_wr: %d, max cq: %d, max cqe: %d, "
                  "max mcast grp: %d, max mcast qp attach: %d, max total mcast qp attach: %d",
             mcast_ctx->mtu, device_attr.max_qp_wr, device_attr.max_srq_wr,
             device_attr.max_cq, device_attr.max_cqe,
             device_attr.max_mcast_grp, device_attr.max_mcast_qp_attach, device_attr.max_total_mcast_qp_attach);

    mcast_ctx->max_qp_wr = device_attr.max_qp_wr;

    status = tl_spin_rcache_create(mcast_ctx->pd, &mcast_ctx->rcache);
    if (UCC_OK != status) {
        tl_debug(lib, "failed to create mcast rcache");
        goto error;
    }

    tl_debug(lib, "multicast context setup complete: ctx %p", mcast_ctx);

    return UCC_OK;

error:
    if (mcast_ctx->pd) {
        ibv_dealloc_pd(mcast_ctx->pd);
    }
    if (mcast_ctx->id) {
        rdma_destroy_id(mcast_ctx->id);
    }
    if (mcast_ctx->channel) {
        rdma_destroy_event_channel(mcast_ctx->channel);
    }

    return status;
}

void ucc_tl_spin_finalize_p2p_context(ucc_tl_spin_context_t *ctx)
{
    ucc_tl_spin_p2p_context_t *p2p_ctx = &ctx->p2p;
    ucc_base_lib_t            *lib     = ctx->super.super.lib;

    if (p2p_ctx->rcache) {
        ucc_rcache_destroy(p2p_ctx->rcache);
    }

    if (p2p_ctx->pd) {
        if (ibv_dealloc_pd(p2p_ctx->pd)) {
            tl_error(lib, "ibv_dealloc_pd failed errno %d", errno);
        }
    }

    if (p2p_ctx->dev) {
        if (ibv_close_device(p2p_ctx->dev)) {
            tl_error(lib, "ibv_close_dev failed errno %d", errno);
        }
    }

    if (!strcmp(ctx->cfg.ib_dev_name, "")) {
        ucc_free(ctx->devname);
    }
}

void ucc_tl_spin_finalize_mcast_context(ucc_tl_spin_context_t *ctx)
{
    ucc_tl_spin_mcast_context_t *mcast_ctx = &ctx->mcast;
    ucc_base_lib_t              *lib       = ctx->super.super.lib;

    if (mcast_ctx->rcache) {
        ucc_rcache_destroy(mcast_ctx->rcache);
    }

    if (mcast_ctx->pd) {
        if (ibv_dealloc_pd(mcast_ctx->pd)) {
            tl_error(lib, "ibv_dealloc_pd failed errno %d", errno);
        }
    }

    if (rdma_destroy_id(mcast_ctx->id)) {
        tl_error(lib, "rdma_destroy_id failed errno %d", errno);
    }

    rdma_destroy_event_channel(mcast_ctx->channel);
}

UCC_CLASS_INIT_FUNC(ucc_tl_spin_context_t,
                    const ucc_base_context_params_t *params,
                    const ucc_base_config_t *config)
{
    ucc_status_t status = UCC_OK;
    ucc_tl_spin_context_config_t *tl_spin_config =
        ucc_derived_of(config, ucc_tl_spin_context_config_t);
    
    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_context_t, &tl_spin_config->super,
                              params->context);
    memcpy(&self->cfg, tl_spin_config, sizeof(*tl_spin_config));
    
    ucc_assert_always(self->cfg.n_tx_workers == 1);
    self->cur_core_id = self->cfg.start_core_id;

    status = ucc_mpool_init(&self->req_mp, 0, sizeof(ucc_tl_spin_task_t), 0,
                            UCC_CACHE_LINE_SIZE, 8, UINT_MAX,
                            &ucc_coll_task_mpool_ops, params->thread_mode,
                            "tl_sharp_req_mp");
    if (status != UCC_OK) {
        tl_error(self->super.super.lib, "failed to initialize request mpool");
        return UCC_ERR_NO_MEMORY;
    }

    status = ucc_tl_spin_init_p2p_context(self);
    if (status != UCC_OK) {
        tl_error(self->super.super.lib, "failed to initialize p2p context");
        goto err_p2p;
    }

    status = ucc_tl_spin_init_mcast_context(self);
    if (status != UCC_OK) {
        tl_error(self->super.super.lib, "failed to initialize mcast context");
        goto err_mcast;
    }

    tl_info(self->super.super.lib, "initialized tl context: %p", self);
    return UCC_OK;

err_mcast:
    ucc_tl_spin_finalize_p2p_context(self);
err_p2p:
    return status;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_context_t)
{
    tl_info(self->super.super.lib, "finalizing tl context: %p", self);
    ucc_tl_spin_finalize_p2p_context(self);
    ucc_tl_spin_finalize_mcast_context(self);
    ucc_mpool_cleanup(&self->req_mp, 1);
}

UCC_CLASS_DEFINE(ucc_tl_spin_context_t, ucc_tl_context_t);

ucc_status_t
ucc_tl_spin_get_context_attr(const ucc_base_context_t *context, /* NOLINT */
                             ucc_base_ctx_attr_t *     attr)
{
    if (attr->attr.mask & UCC_CONTEXT_ATTR_FIELD_CTX_ADDR_LEN) {
        attr->attr.ctx_addr_len = 0;
    }
    attr->topo_required = 1;
    return UCC_OK;
}