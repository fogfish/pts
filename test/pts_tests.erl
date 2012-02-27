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
%% test process
%%
%%-----------------------------------------------------------------------------
proc(Key0, Val0) ->
   receive
      {pts, Tx, {put, Key, Val}} ->
         pts:notify(Tx, ok),
         proc(Key, Val);
      {pts, Tx, {get, Key}} ->
         pts:notify(Tx, {ok, Val0}),
         proc(Key0, Val0);
      {pts, Tx, {remove, Key}} ->
         pts:detach(Key),
         pts:notify(Tx,  ok)
   end.

factory({create, Key}) ->
   {ok, spawn(
      fun() ->
         pts:attach(Key),
         proc(Key, nil)
      end
   )}.   

%%-----------------------------------------------------------------------------
%%
%% table management
%%
%%-----------------------------------------------------------------------------
pts_tbl_mgmt_test_() ->
   {
      setup,
      fun()  -> application:start(pts) end,
      fun(_) -> application:stop(pts) end,
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
-define(KEY, {key, 1}).
-define(NS1,  test).
-define(NS2,  {test, 1}).

pts_dat_mgmt_test_() ->
   {
      setup,
      fun() -> 
         application:start(pts),
         pts:new(?NS1, [
            {factory, fun factory/1}
         ]),
         pts:new(?NS2, [
            {factory, fun factory/1}
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
   ok = pts:put({?NS1, ?KEY}, value).
   
has() ->
   true  = pts:has({?NS1, ?KEY}),
   false = pts:has({?NS1, key}).
   
get() ->
   {ok, value} = pts:get({?NS1, ?KEY}),
   {error, not_found} = pts:get({?NS1, key}).
   
remove() ->
   ok = pts:remove({?NS1, ?KEY}),
   false = pts:has({?NS1, ?KEY}),
   {error, not_found} = pts:get({?NS1, ?KEY}).

map() ->
   lists:foreach(
      fun(X) -> ok = pts:put({?NS1, X}, X) end,
      lists:seq(1,5)
   ),
   M = pts:map(?NS1, fun({{_,K},V}) -> K*V() end), 
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
fold() ->
   lists:foreach(
      fun(X) -> ok = pts:put({?NS1, X}, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold(?NS1, 0, fun({{_,K},V}, A) -> A + K*V() end),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).   
   
   
tput() ->
   ok = pts:put({?NS2, ?KEY}, value).
   
thas() ->
   true  = pts:has({?NS2, ?KEY}),
   false = pts:has({?NS2,  key}).
   
tget() ->
   {ok, value} = pts:get({?NS2, ?KEY}),
   {error, not_found} = pts:get({?NS2, key}).
   
tremove() ->
   ok = pts:remove({?NS2, ?KEY}),
   false = pts:has({?NS2, ?KEY}),
   {error, not_found} = pts:get({?NS2, ?KEY}).

tmap() ->
   lists:foreach(
      fun(X) -> ok = pts:put({?NS2, X}, X) end,
      lists:seq(1,5)
   ),
   M = pts:map(?NS2, fun({{_,K},V}) -> K*V() end),
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
tfold() ->
   lists:foreach(
      fun(X) -> ok = pts:put({?NS2, X}, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold(?NS2, 0, fun({{_,K},V}, A) -> A + K*V() end),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).      