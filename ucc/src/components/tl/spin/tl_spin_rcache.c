/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Copyright (c) 2024, ETH Zurich.
 *
 * See file LICENSE for terms.
 */

#include "tl_spin_rcache.h"

static ucs_status_t
rcache_reg_mr(void *context, ucc_rcache_t *rcache, //NOLINT: rcache is unused
              void *arg, ucc_rcache_region_t *rregion,
              uint16_t flags) //NOLINT: flags is unused
{
    struct ibv_pd               *pd           = (struct ibv_pd *)context;
    void                        *addr         = (void *)rregion->super.start;
    size_t                       length       = (size_t)(rregion->super.end
                                                       - rregion->super.start);
    int                         *change_flag  = (int *)arg;
    ucc_tl_spin_rcache_region_t *spin_rregion = ucc_derived_of(rregion,
                                                  ucc_tl_spin_rcache_region_t);

    *change_flag     = 1;
    spin_rregion->mr = ibv_reg_mr(pd, addr, length,
                                  IBV_ACCESS_LOCAL_WRITE |
                                  IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ);
    if (!spin_rregion->mr) {
        ucc_error("failed to register memory");
        return UCS_ERR_NO_MESSAGE;
    }

    return UCS_OK;
}

static void rcache_dereg_mr(void *context, //NOLINT: context is unused
                            ucc_rcache_t *rcache, //NOLINT: rcache is unused
                            ucc_rcache_region_t *rregion)
{
    ucc_tl_spin_rcache_region_t *spin_rregion =
        ucc_derived_of(rregion, ucc_tl_spin_rcache_region_t);
    ibv_dereg_mr(spin_rregion->mr);
}

static void ucc_tl_spin_rcache_dump_region_cb(void *context, //NOLINT
                                              ucc_rcache_t *rcache, //NOLINT
                                              ucs_rcache_region_t *rregion,
                                              char *buf, size_t max)
{
    ucc_tl_spin_rcache_region_t *spin_rregion =
        ucc_derived_of(rregion, ucc_tl_spin_rcache_region_t);

    snprintf(buf, max, "bar ptr:%p", spin_rregion->mr);
}

static ucc_rcache_ops_t ucc_tl_spin_rcache_ops = {
    .mem_reg     = rcache_reg_mr,
    .mem_dereg   = rcache_dereg_mr,
    .dump_region = ucc_tl_spin_rcache_dump_region_cb
};

ucc_status_t tl_spin_rcache_create(struct ibv_pd *pd, ucc_rcache_t **rcache)
{
    ucc_rcache_params_t rcache_params;

    ucc_rcache_set_default_params(&rcache_params);
    rcache_params.region_struct_size = sizeof(ucc_tl_spin_rcache_region_t);
    rcache_params.context            = pd;
    rcache_params.ops                = &ucc_tl_spin_rcache_ops;
    rcache_params.ucm_events         = UCM_EVENT_VM_UNMAPPED |
                                       UCM_EVENT_MEM_TYPE_FREE;

    return ucc_rcache_create(&rcache_params, "SPIN", rcache);
}