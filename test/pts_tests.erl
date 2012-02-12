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
         {"Create table", fun create_tbl1/0},
         {"Drop table",  fun drop_tbl1/0},
         {"Create table (tuple)", fun create_tbl2/0},
         {"Drop table (tuple)",  fun drop_tbl2/0}
      ]
   }.

create_tbl1() ->
   ok = pts:new(test, []),
   [_] = pts:i().
   
drop_tbl1() ->
   ok = pts:drop(test),
   [] = pts:i().
   
create_tbl2() ->
   ok = pts:new({test, a}, []),
   [_] = pts:i().
   
drop_tbl2() ->
   ok = pts:drop({test, a}),
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
         Loop = fun(L, {Tab, Key, Val0} = S) ->
            receive
               {pts_req, Pid, {put, Key, Val}} -> 
                  pts:attach(Tab, Key),
                  Pid ! {pts_rsp, ok},
                  L(L, {Tab, Key, Val});
               {pts_req, Pid, {get, Key}} ->
                  Pid ! {pts_rsp, {ok, Val0}},
                  L(L, {Tab, Key, Val0});
               {pts_req, Pid, {remove, Key}} ->
                  pts:detach(Tab, Key),
                  Pid ! {pts_rsp, ok}
            end
         end,
         pts:new(test, [
            {factory, 
               fun(Tab,Key)-> 
                  {ok, spawn(
                     fun() -> 
                        pts:attach(Tab, Key),
                        Loop(Loop, {Tab, Key, nil})
                     end
                  )}
               end
            }
         ]),
         pts:new({test, a}, [
            {factory, 
               fun(Tab,Key)-> 
                  {ok, spawn(
                     fun() -> 
                        pts:attach(Tab, Key),
                        Loop(Loop, {Tab, Key, nil})
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
         {"Fold", fun fold/0},
         {"Put (tuple)", fun tput/0},
         {"Has (tuple)", fun thas/0},
         {"Get (tuple)", fun tget/0},
         {"Remove (tuple)", fun tremove/0},
         {"Map (tuple)", fun tmap/0},
         {"Fold (tuple)", fun tfold/0}
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
   
   
tput() ->
   ok = pts:put({test, a}, key1, value).
   
thas() ->
   true  = pts:has({test, a}, key1),
   false = pts:has({test, a}, key2).
   
tget() ->
   {ok, value} = pts:get({test, a}, key1),
   {error, not_found} = pts:get({test, a}, key2).
   
tremove() ->
   ok = pts:remove({test, a}, key1),
   false = pts:has({test, a}, key1),
   {error, not_found} = pts:get({test, a}, key1).

tmap() ->
   lists:foreach(
      fun(X) -> ok = pts:put({test, a}, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:map({test, a}, fun({K,V}) -> K*V end),
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
tfold() ->
   lists:foreach(
      fun(X) -> ok = pts:put({test, a}, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold({test, a}, 0, fun({K,V}, A) -> A + K*V end),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).      