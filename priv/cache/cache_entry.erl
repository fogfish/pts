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
-module(cache_entry).
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).

-export([
   start_link/1,
   %% gen_server
   init/1, 
   handle_call/3,
   handle_cast/2, 
   handle_info/2, 
   terminate/2, 
   code_change/3 
]).

-record(srv, {
   ttl,       % time-to-live in seconds
   key,       % key
   val        % value
}).
-define(TTL_DEF, 30000).

%%
%%
start_link(Req) ->
  gen_server:start_link(?MODULE, Req, []).
  
init([pts, _, Key]) ->
   pts:attach(Key),
   {ok, 
      #srv{
         key=Key
      }
   }. 
   
handle_call({put, Val}, _, S) ->
   {reply, ok, S#srv{val = Val}};
handle_call({get, _Key}, _, S) ->
   {reply, {ok, S#srv.val}, S};
handle_call({remove, _Key}, _, S) ->
   {stop, normal, ok, S};

handle_call(_Req, _From, S) ->
   {reply, undefined, S}.
   
handle_cast(_Req, State) ->
   {noreply, State}.
 
%%
%%
handle_info(timeout, S) ->
   {stop, normal, S}.
   
   
terminate(_Reason, S) ->
   pts:detach(S#srv.key),
   ok.
   
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     
