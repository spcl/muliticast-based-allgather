AUTOMAKE_OPTIONS = subdir-objects

NVCC = nvcc
NVCFLAGS = -cuda -maxrregcount 32 -ccbin $(CXX) $(NVCCFLAGS)
SUFFIXES = .cu .cpp
.cu.cpp:
	$(NVCC) $(NVCFLAGS) $(INCLUDES) $(CPPFLAGS) --output-file $@.ii $<
	mv $@.ii $@

xcclcollectivedir = $(pkglibexecdir)/xccl/collective
xcclcollective_PROGRAMS = osu_xccl_allgather osu_xccl_allreduce osu_xccl_bcast osu_xccl_reduce osu_xccl_reduce_scatter osu_xccl_alltoall

AM_CFLAGS = -I${top_srcdir}/c/util -I${top_srcdir}/c/xccl/util

UTILITIES = ${top_srcdir}/c/util/osu_util.c ${top_srcdir}/c/util/osu_util.h \
    ${top_srcdir}/c/util/osu_util_mpi.c ${top_srcdir}/c/util/osu_util_mpi.h \
    ${top_srcdir}/c/util/osu_util_papi.c ${top_srcdir}/c/util/osu_util_papi.h \
    ${top_srcdir}/c/xccl/util/osu_util_xccl_interface.c ${top_srcdir}/c/xccl/util/osu_util_xccl_interface.h

if CUDA_KERNELS
    UTILITIES += ${top_srcdir}/c/util/kernel.cu
    CLEANFILES = ${top_srcdir}/c/util/kernel.cpp
if BUILD_USE_PGI
    AM_CXXFLAGS = --nvcchost --no_preincludes
endif
endif

if RCCL
    UTILITIES += ${top_srcdir}/c/xccl/util/rccl/osu_util_rccl_impl.c ${top_srcdir}/c/xccl/util/rccl/osu_util_rccl_impl.h
endif

if NCCL
    UTILITIES += ${top_srcdir}/c/xccl/util/nccl/osu_util_nccl_impl.c ${top_srcdir}/c/xccl/util/nccl/osu_util_nccl_impl.h
endif

osu_xccl_allgather_SOURCES = osu_xccl_allgather.c $(UTILITIES)
osu_xccl_allreduce_SOURCES = osu_xccl_allreduce.c $(UTILITIES)
osu_xccl_bcast_SOURCES = osu_xccl_bcast.c $(UTILITIES)
osu_xccl_reduce_SOURCES = osu_xccl_reduce.c $(UTILITIES)
osu_xccl_reduce_scatter_SOURCES = osu_xccl_reduce_scatter.c $(UTILITIES)
osu_xccl_alltoall_SOURCES = osu_xccl_alltoall.c $(UTILITIES)

AM_CPPFLAGS = -DNCCL_USE_ROCM

if EMBEDDED_BUILD
    AM_LDFLAGS =
    AM_CPPFLAGS += -I$(top_builddir)/../src/include \
          -I${top_srcdir}/util \
          -I${top_srcdir}/../src/include \
          -I${top_srcdir}/../modules/rccl/build/include
if RCCL
    AM_LDFLAGS += -I$(top_builddir)/../modules/rccl/build/lib -lrccl
    AM_CPPFLAGS += -I${top_srcdir}/../modules/rccl/build/include
endif
if NCCL
    AM_LDFLAGS += -I$(top_builddir)/../modules/nccl/build/lib -lnccl
    AM_CPPFLAGS += -I${top_srcdir}/../modules/nccl/build/include
endif
if BUILD_PROFILING_LIB
    AM_LDFLAGS += $(top_builddir)/../lib/lib@PMPILIBNAME@.la
endif
    AM_LDFLAGS += $(top_builddir)/../lib/lib@MPILIBNAME@.la
endif

if OPENACC
    AM_CFLAGS += -acc -ta=tesla:nordc
    AM_CXXFLAGS = -acc -ta=tesla:nordc
endif
