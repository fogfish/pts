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
   start_link/2,
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
   tab,       % table
   key,       % key
   val        % value
}).
-define(TTL_DEF, 30000).

%%
%%
start_link(Tab, Key) ->
  gen_server:start_link(?MODULE, [Tab, Key], []).
  
init([Tab, Key]) ->
   {ok, 
      #srv{
         tab=Tab, 
         key=Key
      }
   }. 
   

handle_call(_Req, _From, State) ->
   {reply, undefined, State}.
   
handle_cast(_Req, State) ->
   {noreply, State}.
 
%%
%%
handle_info(timeout, S) ->
   {stop, normal, S};

handle_info({pts_req_put, Pid, _Key, Val}, S) ->  
   TTL = case is_list(Val) of
      true  -> proplists:get_value(ttl, Val, ?TTL_DEF);
      false -> ?TTL_DEF 
   end,
   Pid ! {pts_rsp_put, ok},
   {noreply, S#srv{ttl = TTL, val = Val}, TTL};

handle_info({pts_req_get, Pid, _Key}, S) ->   
   Pid ! {pts_rsp_get, {ok, S#srv.val}},
   {noreply, S, S#srv.ttl};

handle_info({pts_req_remove, Pid, _Key}, S) ->   
   Pid ! {pts_rsp_remove, ok},
   {stop, normal, S}.
   
terminate(_Reason, S) ->
   pts:deattach(S#srv.tab, S#srv.key),
   ok.
   
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     

