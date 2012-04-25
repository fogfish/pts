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
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").


-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%-----------------------------------------------------------------------------
%%
%% test process
%%
%%-----------------------------------------------------------------------------
init([pts, _, Key]) ->
   pts:attach(Key),
   {ok, {Key, nil}}.

handle_call({put, Val}, _, {Key, _}) ->
   {reply, ok, {Key, Val}};
handle_call({get, Key}, _, {_, Val} = S) ->
   {reply, {ok, Val}, S};
handle_call({remove, Key}, _, S) ->
   {stop, normal, ok, S};   
handle_call(_Req, _From, State) ->
   {reply, undefined, State}.
   
handle_cast(_Req, State) ->
   {noreply, State}.

handle_info(_Req, State) ->
   {noreply, State}.

terminate(_Reason, {Key, _}) ->
   pts:detach(Key),
   ok.
   
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     



% proc(Key0, Val0) ->
%    receive
%       {pts, Tx, {put, Flags, Val}}  ->
%          case Flags of
%             nil -> pts:notify(Tx, ok);
%             val -> pts:notify(Tx, {ok, Val0})
%          end,
%          proc(Key0, Val);
%       {pts, Tx, {get, Key}} ->
%          pts:notify(Tx, {ok, Val0}),
%          proc(Key0, Val0);
%       {pts, Tx, {remove, Flags, Key}} ->
%          pts:detach(Key),
%          case Flags of
%             nil -> pts:notify(Tx,  ok);
%             val -> pts:notify(Tx,  {ok, Val0})
%          end
%    end.

factory(Req) ->
   gen_server:start_link(?MODULE, Req, []).


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
-define(NS1,  "pts:test").
-define(NS2,  {test, 1}).


-define(PTS, "kvs:test").
-define(KEY, key).
-define(VAL, {key, "value"}).
-define(NEW, {key,   "new"}).

pts_dat_mgmt_test_() ->
   {
      setup,
      fun() -> 
         application:start(pts),
         pts:new(?PTS, [
            {factory, fun factory/1}
         ])
      end,
      [
          {"Create", fun create/0}
         ,{"Read",   fun read/0}
         ,{"Update", fun update/0}
         ,{"Delete", fun delete/0}
         
         ,{"Put", fun put/0}
         ,{"Get", fun get/0}
         ,{"Remove", fun remove/0}
         
         ,{"Map", fun map/0}
         ,{"Fold", fun fold/0}
      ]
   }.   
   

create() ->
   ok   = pts:create(?PTS, ?VAL),
   true = is_process_alive(pns:whereis({?PTS, ?KEY})).
   
read() ->
   {ok, ?VAL} = pts:read(?PTS, ?KEY).
   
update() ->
   ok = pts:update(?PTS, ?NEW),
   {ok, ?NEW} = pts:read(?PTS, ?KEY).
   
delete() ->
   ok = pts:delete(?PTS, ?KEY),
   undefined = pns:whereis({?PTS, ?KEY}).
   
   
put() ->
   ok = pts:put(?PTS, ?VAL),
   ok = pts:put(?PTS, ?NEW).
   
get() ->
   {ok, ?NEW} = pts:get(?PTS, ?KEY).
   
remove() ->
   ok = pts:remove(?PTS, ?KEY).

   
   

map() ->
   lists:foreach(
      fun(X) -> ok = pts:create(?PTS, {X, X}) end,
      lists:seq(1,5)
   ),
   M = pts:map(?PTS, fun({K,V}) ->  {ok, {_,V0}} = V(), K*V0 end), 
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
fold() ->
   lists:foreach(
      fun(X) -> ok = pts:update(?PTS, {X, X}) end,
      lists:seq(1,5)
   ),
   M = pts:fold(?PTS, 0, fun({K,V}, A) -> {ok, {_,V0}} = V(), A + K*V0 end),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).   
