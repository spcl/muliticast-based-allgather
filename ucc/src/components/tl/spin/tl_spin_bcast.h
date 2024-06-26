#ifndef UCC_TL_SPIN_BCAST_H_
#define UCC_TL_SPIN_BCAST_H_

#include "tl_spin.h"

void ucc_tl_spin_bcast_init_task_tx_info(ucc_tl_spin_task_t    *task,
                                         ucc_tl_spin_context_t *ctx);
ucc_status_t ucc_tl_spin_bcast_init(ucc_tl_spin_task_t   *task,
                                    ucc_base_coll_args_t *coll_args,
                                    ucc_tl_spin_team_t   *team);
ucc_status_t
ucc_tl_spin_coll_worker_tx_bcast_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task);
ucc_status_t
ucc_tl_spin_coll_worker_rx_bcast_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task);
ucc_status_t 
ucc_tl_spin_coll_worker_tx_handler(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task);
ucc_status_t
ucc_tl_spin_coll_worker_rx_handler(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task);
ucc_status_t
ucc_tl_spin_coll_worker_rx_reliability_handler(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *cur_task);

#endif