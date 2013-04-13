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
-define(KEY4, key4).

-define(NS1,  ns1).
-define(NS2,  ns2).


init_test() ->
   {ok, _} = pns:start_link().

register_test() ->
   ok = pns:register(?NS1, ?KEY1),
   ok = pns:register(?NS2, ?KEY2, self()),

   {badarg, _} = (catch pns:register(?NS1, ?KEY1, self())),

   spawn(fun() -> pns:register(?NS1, ?KEY3) end),
   timer:sleep(10),
   ok = pns:register(?NS1, ?KEY3, self()).

   
whereis_test() ->
   ?assert(
      self() =:= pns:whereis(?NS1, ?KEY1)
   ),
   ?assert(
      self() =:= pns:whereis(?NS2, ?KEY2)
   ),
   ?assert(
      undefined =:= pns:whereis(?NS1, ?KEY4)
   ).
   
whatis_test() ->
   [?KEY1, ?KEY3] = pns:whatis(?NS1, self()),
   [?KEY2] = pns:whatis(?NS2, self()).

lookup_test() ->
   [{?KEY1, _}, {?KEY3, _}] = pns:lookup(?NS1, '_'),
   [{?KEY2, _}] = pns:lookup(?NS2, {key, '_'}).


unregister_test() ->
   ok = pns:unregister(?NS1, ?KEY1),
   ok = pns:unregister(?NS2, ?KEY2).
   