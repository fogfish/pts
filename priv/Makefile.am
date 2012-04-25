#
#   Copyright (c) 2012, Dmitry Kolesnikov
#   All Rights Reserved.
#
#  This library is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License, version 3.0
#  as published by the Free Software Foundation (the "License").
#
#  Software distributed under the License is distributed on an "AS IS"
#  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
#  the License for the specific language governing rights and limitations
#  under the License.
# 
#  You should have received a copy of the GNU Lesser General Public
#  License along with this library; if not, write to the Free Software
#  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
#  USA or retrieve online http://www.opensource.org/licenses/lgpl-3.0.html
#
cache_USE = pts
cache_SRC = \
   cache/cache.erl \
   cache/cache_app.erl \
   cache/cache_sup.erl \
   cache/cache_entry.erl \
   cache/cache_entry_sup.erl

nobase_pkgliberl_SCRIPTS = \
   ebin/cache.app

CLEANFILES = $(nobase_pkgliberl_SCRIPTS)
EXTRA_DIST = $(cache_SRC)

@MAKE_GNUWEB@
