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

-define(KEY1, key1).
-define(KEY2, {key, 1}).
-define(KEY3, key3).

-define(NS1,  ns1).
-define(NS2,  ns2).


start_test() ->
   ok = pts_ns:start().
   
register_test() ->
   ?assert(
      ok =:= pts_ns:register(?KEY1, self())
   ),
   ?assert(
      ok =:= pts_ns:register(?KEY2, self())
   ),
   ?assert(
      ok =:= pts_ns:register(?NS1, ?KEY2, self())
   ).
   
whereis_test() ->
   ?assert(
      self() =:= pts_ns:whereis(?KEY1)
   ),
   ?assert(
      self() =:= pts_ns:whereis(?KEY2)
   ),
   ?assert(
      self() =:= pts_ns:whereis(?NS1, ?KEY2)
   ),
   ?assert(
      self() =:= pts_ns:whereis({?NS1, ?KEY2})
   ),
   ?assert(
      undefined =:= pts_ns:whereis(?KEY3)
   ),
   ?assert(
      undefined =:= pts_ns:whereis(?NS2, ?KEY2)
   ),
   ?assert(
      undefined =:= pts_ns:whereis({?NS2, ?KEY2})
   ).
   
whatis_test() ->
   Keys = pts_ns:whatis(self()),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)).
   
map_test() ->
   Keys = pts_ns:map(fun({Uid, _}) -> Uid end),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)),
   ?assert(
      [{?NS1, ?KEY2}] =:= pts_ns:map(?NS1, fun({Uid, _}) -> Uid end)
   ).
   
fold_test() ->   
   Keys = pts_ns:fold([], fun({Uid, _}, A) -> [Uid | A] end),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)),
   ?assert(
      [{?NS1, ?KEY2}] =:= pts_ns:fold(?NS1, [], fun({Uid, _}, A) -> [Uid | A] end)
   ).
   
unregister_test() ->
   ?assert(
      ok =:= pts_ns:unregister(?KEY1)
   ),
   ?assert(
      ok =:= pts_ns:unregister(?KEY2)
   ),
   ?assert(
      ok =:= pts_ns:unregister({?NS1, ?KEY2})
   ).
   