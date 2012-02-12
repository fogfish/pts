%%
%%   Copyright (c) 2012, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%  This library is free software; you can redistribute it and/or modify
%%  it under the terms of the GNU Lesser General Public License, version 3.0
%%  as published by the Free Software Foundation (the "License").
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and limitations
%%  under the License.
%% 
%%  You should have received a copy of the GNU Lesser General Public
%%  License along with this library; if not, write to the Free Software
%%  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%%  USA or retrieve online http://www.opensource.org/licenses/lgpl-3.0.html
%%
-module(pts_ns_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
   ok = pts_ns:start().
   
register_test() ->
   ?assert(
      ok =:= pts_ns:register("mykey", self())
   ).
   
whereis_test() ->
   ?assert(
      self() =:= pts_ns:whereis("mykey")
   ).
   
map_test() ->
   ?assert(
      ["mykey"] =:= pts_ns:map(fun({Uid, _}) -> Uid end)
   ).

fold_test() ->
   ?assert(
      ["mykey"] =:= pts_ns:fold([], fun({Uid, _}, A) -> [Uid | A] end)
   ).
   
unregister_test() ->
   ?assert(
      ok =:= pts_ns:unregister("mykey")
   ).
   