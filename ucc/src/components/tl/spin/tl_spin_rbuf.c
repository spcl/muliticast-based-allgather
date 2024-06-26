#include "tl_spin_rbuf.h"
#include <assert.h>
#include <stdio.h>

void rbuf_init(rbuf_t *rbuf)
{
    int i;
    rbuf->capacity = RBUF_SIZE;
    for (i = 0; i < rbuf->capacity; i++) {
        rbuf->buf[i] = 0;
    }
    rbuf->tail_idx = 0;
    rbuf->head_idx = 0;
}

inline int rbuf_has_space(rbuf_t *rbuf)
{
    return rbuf->capacity > 0;
}

inline void rbuf_push_head(rbuf_t *rbuf, uintptr_t elem)
{
    assert(rbuf_has_space(rbuf));
    rbuf->buf[rbuf->head_idx] = elem;
    rbuf->head_idx = (rbuf->head_idx + 1) % RBUF_SIZE;
    rbuf->capacity--;
}

inline void rbuf_pop_tail(rbuf_t *rbuf)
{
    int old_tail_idx = rbuf->tail_idx;
    assert(rbuf->buf[old_tail_idx] != 0);
    rbuf->tail_idx = (old_tail_idx + 1) % RBUF_SIZE;
    rbuf->buf[old_tail_idx] = 0;
    rbuf->capacity++;
}

inline uintptr_t rbuf_get_tail_element(rbuf_t *rbuf, int *elem_idx)
{
    int tail_idx = rbuf->tail_idx;
    *elem_idx = tail_idx;
    return rbuf->buf[tail_idx];
}