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
-module(pns_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").

-define(KEY1, key1).
-define(KEY2, {key, 1}).
-define(KEY3, key3).

-define(NS1,  ns1).
-define(NS2,  ns2).


start_test() ->
   ok = pns:start().
   
register_test() ->
   ?assert(
      ok =:= pns:register(?KEY1, self())
   ),
   ?assert(
      ok =:= pns:register(?KEY2, self())
   ),
   ?assert(
      ok =:= pns:register(?NS1, ?KEY2, self())
   ).
   
whereis_test() ->
   ?assert(
      self() =:= pns:whereis(?KEY1)
   ),
   ?assert(
      self() =:= pns:whereis(?KEY2)
   ),
   ?assert(
      self() =:= pns:whereis(?NS1, ?KEY2)
   ),
   ?assert(
      self() =:= pns:whereis({?NS1, ?KEY2})
   ),
   ?assert(
      undefined =:= pns:whereis(?KEY3)
   ),
   ?assert(
      undefined =:= pns:whereis(?NS2, ?KEY2)
   ),
   ?assert(
      undefined =:= pns:whereis({?NS2, ?KEY2})
   ).
   
whatis_test() ->
   Keys = pns:whatis(self()),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)).
   
map_test() ->
   Keys = pns:map(fun({Uid, _}) -> Uid end),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)),
   ?assert(
      [{?NS1, ?KEY2}] =:= pns:map(?NS1, fun({Uid, _}) -> Uid end)
   ).
   
fold_test() ->   
   Keys = pns:fold([], fun({Uid, _}, A) -> [Uid | A] end),
   ?assert(lists:member(?KEY1, Keys)),
   ?assert(lists:member(?KEY2, Keys)),
   ?assert(lists:member({?NS1, ?KEY2}, Keys)),
   ?assert(not lists:member(?KEY3, Keys)),
   ?assert(not lists:member({?NS2, ?KEY2}, Keys)),
   ?assert(
      [{?NS1, ?KEY2}] =:= pns:fold(?NS1, [], fun({Uid, _}, A) -> [Uid | A] end)
   ).
   
unregister_test() ->
   ?assert(
      ok =:= pns:unregister(?KEY1)
   ),
   ?assert(
      ok =:= pns:unregister(?KEY2)
   ),
   ?assert(
      ok =:= pns:unregister({?NS1, ?KEY2})
   ).
   