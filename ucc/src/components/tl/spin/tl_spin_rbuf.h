#ifndef UCC_TL_SPIN_RBUF_H_
#define UCC_TL_SPIN_RBUF_H_

#include <stddef.h>
#include <stdatomic.h>
#include <stdint.h>

#define RBUF_SIZE 16
typedef struct rbuf {
    atomic_uintptr_t buf[RBUF_SIZE];
    atomic_size_t    capacity;
    atomic_int       tail_idx;
    int              head_idx;
} rbuf_t;

void rbuf_init(rbuf_t *rbuf);
int rbuf_has_space(rbuf_t *rbuf);
void rbuf_push_head(rbuf_t *rbuf, uintptr_t elem);
void rbuf_pop_tail(rbuf_t *rbuf);
uintptr_t rbuf_get_tail_element(rbuf_t *rbuf, int *elem_idx);

#endif