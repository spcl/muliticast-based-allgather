#
# Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#

if TL_SPIN_ENABLED

mcast =                                         \
	../mlx5/mcast/tl_mlx5_mcast.h               \
	../mlx5/mcast/tl_mlx5_mcast_rcache.h        \
	../mlx5/mcast/tl_mlx5_mcast_rcache.c        \
	../mlx5/mcast/p2p/ucc_tl_mlx5_mcast_p2p.h   \
	../mlx5/mcast/p2p/ucc_tl_mlx5_mcast_p2p.c   \
	../mlx5/mcast/tl_mlx5_mcast_progress.h      \
	../mlx5/mcast/tl_mlx5_mcast_helper.h        \
	../mlx5/mcast/tl_mlx5_mcast_helper.c

sources =               \
	tl_spin.h           \
	tl_spin.c           \
	tl_spin_lib.c       \
	tl_spin_context.c   \
	tl_spin_team.c      \
	tl_spin_coll.h      \
	tl_spin_coll.c      \
	tl_spin_p2p.h       \
	tl_spin_p2p.c       \
	tl_spin_mcast.h     \
	tl_spin_mcast.c     \
	tl_spin_rcache.h    \
	tl_spin_rcache.c    \
	tl_spin_bcast.h     \
	tl_spin_bcast.c     \
	tl_spin_allgather.h \
	tl_spin_allgather.c \
	tl_spin_bitmap.h    \
	tl_spin_bitmap.c    \
	tl_spin_rbuf.h      \
	tl_spin_rbuf.c      \
	tl_spin_tsc.h       \
	$(mcast)

module_LTLIBRARIES = libucc_tl_spin.la
libucc_tl_spin_la_SOURCES  = $(sources)
libucc_tl_spin_la_CPPFLAGS = $(AM_CPPFLAGS) $(BASE_CPPFLAGS)
libucc_tl_spin_la_CFLAGS   = $(BASE_CFLAGS)
libucc_tl_spin_la_LDFLAGS  = -version-info $(SOVERSION) --as-needed
libucc_tl_spin_la_LIBADD   = $(UCC_TOP_BUILDDIR)/src/libucc.la $(IBVERBS_LIBADD) $(RDMACM_LIBADD)

include $(top_srcdir)/config/module.am

endif
