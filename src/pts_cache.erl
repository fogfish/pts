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
-module(pts_cache).
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).

-export([start_link/2]).
%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(srv, {
   ttl,       % time-to-live in seconds
   ns, 
   uid, 
   key,       % key
   val        % value
}).
-define(TTL_DEF, 30000).

%%
%%
start_link(Ns, Uid) ->
  gen_server:start_link(?MODULE, [Ns, Uid], []).
  
init([Ns, Uid]) ->
   process_flag(trap_exit, true),
   pns:register(Ns, Uid),
   %io:format("spawn: ~p@~p~n", [Uid, Ns]),
   {ok, #srv{ns=Ns, uid=Uid}}. 
   
%%
%%
handle_call(_, _Tx, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({put, Tx, Key, Val}, S) ->
   gen_server:reply(Tx, ok),
   %io:format("set ~p=~p~n", [Key, Val]),
   {noreply, S#srv{key=Key, val=Val}};

handle_info({get, Tx, _Key}, #srv{val=Val}=S) ->
   gen_server:reply(Tx, {ok, Val}),
   {noreply, S};

handle_info({remove, Tx, _Key}, #srv{val=Val}=S) ->
   gen_server:reply(Tx, ok),
   {stop, normal, S};

handle_info(timeout, S) ->
   {stop, normal, S}.

terminate(_Reason, #srv{ns=Ns, uid=Uid}) ->
   pns:unregister(Ns, Uid),
   %io:format("dies: ~p@~p~n", [Uid, Ns]),
   ok.
   
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     

