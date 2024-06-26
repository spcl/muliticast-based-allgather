#ifndef UCC_TL_SPIN_BITMAP_H_
#define UCC_TL_SPIN_BITMAP_H_

#include "tl_spin.h"

#include "tl_spin_bitmap.h"

size_t ucc_tl_spin_get_bitmap_size(size_t buf_size, int mtu);
void ucc_tl_spin_bitmap_cleanup(ucc_tl_spin_bitmap_descr_t *bitmap);
void ucc_tl_spin_bitmap_set_bit(ucc_tl_spin_bitmap_descr_t *bitmap, uint32_t bit_id);
size_t ucc_tl_spin_bitmap_get_next_gap(ucc_tl_spin_bitmap_descr_t *bitmap,
                                       size_t block_size,
                                       size_t block_offset,
                                       size_t offset,
                                       size_t *gap_start_offset,
                                       size_t *new_offset);

#endif