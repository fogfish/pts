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
SUBDIRS = test examples

pts_SRC = \
   src/pts.erl \
   src/pts_ns.erl \
   src/pts_supervisor.erl \
   src/pts_supervisor_sup.erl \
   src/pts_sup.erl \
   src/pts_app.erl

nobase_pkgliberl_SCRIPTS = \
   ebin/pts.app \
   $(pts_BEAM)

CLEANFILES = $(nobase_pkgliberl_SCRIPTS)
EXTRA_DIST = $(pts_SRC)


run-example:
	$(ERL) -pa ./ebin -pa ./examples/ebin

@MAKE_GNUWEB@

