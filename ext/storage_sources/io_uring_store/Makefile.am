AM_CPPFLAGS = -I$(top_builddir) -I$(top_srcdir)/src/include

lib_LTLIBRARIES = libwiredtiger_io_uring.la
libwiredtiger_io_uring_la_LDFLAGS = -avoid-version -module

libwiredtiger_lz4_la_SOURCES = io_uring_store.c
libwiredtiger_lz4_la_LIBADD = -luring
