#include "tl_spin_bitmap.h"

size_t ucc_tl_spin_get_bitmap_size(size_t buf_size, int mtu)
{
    size_t n_bits     = buf_size / mtu + (buf_size % mtu ? 1 : 0);
    size_t n_uint64ts = n_bits / 64 + (n_bits % 64 ? 1 : 0);
    return n_uint64ts * 8;
}

inline void ucc_tl_spin_bitmap_cleanup(ucc_tl_spin_bitmap_descr_t *bitmap)
{
    memset(bitmap->buf, 0, bitmap->size);
}

inline void ucc_tl_spin_bitmap_set_bit(ucc_tl_spin_bitmap_descr_t *bitmap, uint32_t bit_id)
{
    uint32_t arr_id     = bit_id / 64;
    uint32_t bit_offset = bit_id % 64;
    bitmap->buf[arr_id] |= ((uint64_t)1 << bit_offset);
}

inline size_t ucc_tl_spin_bitmap_get_next_gap(ucc_tl_spin_bitmap_descr_t *bitmap,
                                              size_t block_size,
                                              size_t block_offset,
                                              size_t offset,
                                              size_t *gap_start_offset,
                                              size_t *new_offset)
{
    size_t   start_offset = block_offset + offset;
    size_t   max_bit_id   = block_offset + block_size;
    int      gap_started  = 0;
    size_t   gap_size     = 0;
    uint32_t bit_id, arr_id, bit_offset;

    ucc_assert_always(start_offset <= max_bit_id);

    for (bit_id = start_offset; bit_id < max_bit_id; bit_id++) {
        arr_id     = bit_id / 64;
        bit_offset = bit_id % 64;
        if (!(bitmap->buf[arr_id] & ((uint64_t)1 << bit_offset))) {
            if (gap_started) {
                gap_size++;
                continue;
            }
            *gap_start_offset = bit_id - block_offset;
            gap_size          = 1;
            gap_started       = 1;
        } else if (gap_started) {
            break;
        }
    }
    (*new_offset) = bit_id - block_offset;

    return gap_size;
}