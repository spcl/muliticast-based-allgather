#
# Copyright (c) 2024, ETH ZURICH. All rights reserved.
#

tl_spin_enabled=n
CHECK_TLS_REQUIRED(["spin"])
AS_IF([test "$CHECKED_TL_REQUIRED" = "y"],
[
    CHECK_RDMACM
    AC_MSG_RESULT([RDMACM support: $rdmacm_happy])

    spin_happy=no
    if test "x$rdmacm_happy" = "xyes"; then
       spin_happy=yes
    fi
    AC_MSG_RESULT([SPIN support: $spin_happy])
    if test $spin_happy = "yes"; then
        tl_modules="${tl_modules}:spin"
        tl_spin_enabled=y
    fi
], [])

AM_CONDITIONAL([TL_SPIN_ENABLED], [test "$tl_spin_enabled" = "y"])
AC_CONFIG_FILES([src/components/tl/spin/Makefile])
