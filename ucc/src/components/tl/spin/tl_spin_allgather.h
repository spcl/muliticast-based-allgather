#ifndef UCC_TL_SPIN_ALLGATHER_H_
#define UCC_TL_SPIN_ALLGATHER_H_

#include "tl_spin.h"

ucc_status_t ucc_tl_spin_allgather_init(ucc_tl_spin_task_t   *task,
                                        ucc_base_coll_args_t *coll_args,
                                        ucc_tl_spin_team_t   *team);
ucc_status_t
ucc_tl_spin_coll_worker_tx_allgather_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *task);
ucc_status_t
ucc_tl_spin_coll_worker_rx_allgather_start(ucc_tl_spin_worker_info_t *ctx, ucc_tl_spin_task_t *task);

#endif