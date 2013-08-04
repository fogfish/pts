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
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-export([
   start_link/2, 
   start_link/3,
   %% gen_server
   init/1, 
   terminate/2,
   handle_call/3,
   handle_cast/2, 
   handle_info/2,  
   code_change/3
]).

-record(srv, {
   ns  = undefined :: atom(),        % name space
   uid = undefined :: any(),         % process identity
   ttl = undefined :: integer(),     % time-to-live in seconds
   key = undefined :: any(),         % key
   val = undefined :: any()          % value
}).

%%
%%
start_link(Ns, Uid) ->
  gen_server:start_link(?MODULE, [60000, Ns, Uid], []).

start_link(TTL, Ns, Uid) ->
  gen_server:start_link(?MODULE, [TTL, Ns, Uid], []).
  
init([TTL, Ns, Uid]) ->
   pns:register(Ns, Uid),
   {ok, #srv{ns=Ns, uid=Uid, ttl=TTL}}.
  
terminate(_Reason, #srv{ns=Ns, uid=Uid}) ->
   pns:unregister(Ns, Uid),
   ok.

%%
%%
handle_call({ttl, TTL}, _Tx, S) ->
   {reply, ok, S#srv{ttl = TTL}, TTL};

handle_call(ttl, _Tx, #srv{ttl=TTL}=S) ->
   {reply, {ok, TTL}, S, TTL};

handle_call(_, _Tx, #srv{ttl=TTL}=S) ->
   {noreply, S, TTL}.

%%
%%
handle_cast({ttl, TTL}, S) ->
   {noreply, S#srv{ttl = TTL}, TTL};

handle_cast(_, #srv{ttl=TTL}=S) ->
   {noreply, S, TTL}.

%%
%%
handle_info({put, Tx, {Key, Val}}, #srv{ttl=TTL}=S) ->
   plib:ack(Tx, ok),
   {noreply, S#srv{key=Key, val=Val}, TTL};

handle_info({get, Tx, _Key}, #srv{ttl=TTL, val=Val}=S) ->
   plib:ack(Tx, Val),
   {noreply, S, TTL};

handle_info({remove, Tx, _Key}, #srv{ns=Ns, uid=Uid}=S) ->
   plib:ack(Tx, ok),
   {stop, normal, S#srv{val=undefined}};

handle_info({ttl, TTL}, S) ->
   {noreply, S#srv{ttl = TTL}, TTL};

handle_info(timeout, S) ->
   {stop, normal, S}.

%%
%%
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     

