dnl
dnl   Copyright (c) 2012, Dmitry Kolesnikov
dnl   All Rights Reserved.
dnl
dnl  This library is free software; you can redistribute it and/or modify
dnl  it under the terms of the GNU Lesser General Public License, version 3.0
dnl  as published by the Free Software Foundation (the "License").
dnl
dnl  Software distributed under the License is distributed on an "AS IS"
dnl  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
dnl  the License for the specific language governing rights and limitations
dnl  under the License.
dnl 
dnl  You should have received a copy of the GNU Lesser General Public
dnl  License along with this library; if not, write to the Free Software
dnl  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
dnl  USA or retrieve online http://www.opensource.org/licenses/lgpl-3.0.html
dnl
AC_INIT([pts], [0.2.1], [dmkolesnikov@gmail.com])
AM_INIT_AUTOMAKE([silent-rules])
AM_SILENT_RULES([yes])
ACX_INIT_GNUWEB

ACX_ERLANG

AM_CONFIG_HEADER(config.h)
AC_OUTPUT(
   Makefile     \
   test/Makefile \
   examples/Makefile \
)
ACX_DONE

