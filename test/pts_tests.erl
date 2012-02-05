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
-module(pts_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").


%%-----------------------------------------------------------------------------
%%
%% table management
%%
%%-----------------------------------------------------------------------------
pts_tbl_mgmt_test_() ->
   {
      setup,
      fun() -> application:start(pts) end,
      [
         {"Create table", fun create_tbl/0},
         {"Drop table",  fun drop_tbl/0}
      ]
   }.

create_tbl() ->
   ok = pts:new(test, []),
   Hash = fun erlang:phash2/1,
   [{pts, test, ordered_set, 1, sync, 5000, Hash, undefined}] = pts:i().
   
drop_tbl() ->
   ok = pts:delete(test),
   [] = pts:i().
   

%%-----------------------------------------------------------------------------
%%
%% data management
%%
%%-----------------------------------------------------------------------------
pts_dat_mgmt_test_() ->
   {
      setup,
      fun() -> 
         application:start(pts),
         Loop = fun(L, S) ->
            receive
               {pts_req_put, Pid, Key, Val} -> 
                  Pid ! {pts_rsp_put, ok},
                  L(L, {Key, Val});
               {pts_req_get, Pid, Key} ->
                  {_, Val} = S,
                  Pid ! {pts_rsp_get, {ok, Val}},
                  L(L, S);
               {pts_req_remove, Pid, Key} ->
                  Pid ! {pts_rsp_remove, ok},
                  L(L, {nil,nil})
            end
         end,
         pts:new(test, [
            {factory, 
               fun(_,_)-> 
                  {ok, spawn(
                     fun() -> 
                        Loop(Loop, {nil, nil})
                     end
                  )}
               end
            }
         ])
      end,
      [
         {"Put", fun put/0},
         {"Has", fun has/0},
         {"Get", fun get/0},
         {"Remove", fun remove/0},
         {"Map", fun map/0},
         {"Fold", fun fold/0}
      ]
   }.   
   
put() ->
   ok = pts:put(test, key1, value).
   
has() ->
   true  = pts:has(test, key1),
   false = pts:has(test, key2).
   
get() ->
   {ok, value} = pts:get(test, key1),
   {error, not_found} = pts:get(test, key2).
   
remove() ->
   ok = pts:remove(test, key1),
   false = pts:has(test, key1),
   {error, not_found} = pts:get(test, key1).

map() ->
   lists:foreach(
      fun(X) -> ok = pts:put(test, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:map(test, fun({K,V}) -> K*V end),
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
fold() ->
   lists:foreach(
      fun(X) -> ok = pts:put(test, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold(test, 0, fun({K,V}, A) -> A + K*V end),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).   