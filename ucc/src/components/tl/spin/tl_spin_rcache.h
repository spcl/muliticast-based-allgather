#ifndef UCC_TL_SPIN_RCACHE_H_
#define UCC_TL_SPIN_RCACHE_H_

#include "tl_spin.h"

ucc_status_t tl_spin_rcache_create(struct ibv_pd *pd, ucc_rcache_t **rcache);

#endif