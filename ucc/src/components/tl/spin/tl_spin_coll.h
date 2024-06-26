#ifndef UCC_TL_SPIN_COLL_H_
#define UCC_TL_SPIN_COLL_H_

#include "tl_spin.h"

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *team,
                                   ucc_coll_task_t **task);
ucc_status_t ucc_tl_spin_coll_activate_workers(ucc_tl_spin_task_t *task);
void ucc_tl_spin_coll_progress(ucc_tl_spin_task_t *task, ucc_status_t *coll_status);
ucc_status_t ucc_tl_spin_coll_finalize(ucc_tl_spin_task_t *task);
void ucc_tl_spin_coll_post_kill_task(ucc_tl_spin_team_t *team);
void ucc_tl_spin_coll_kill_task_finalize(ucc_tl_spin_team_t *team);
void * ucc_tl_spin_coll_worker_main(void *arg);
#endif