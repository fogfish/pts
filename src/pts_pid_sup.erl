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
%%  @description
%%     Monitors entity processes and re-spawns then on crash   
%%
-module(pts_pid_sup).
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).

-export([
   start_link/0,
   supervise/1,
   % gen_server
   init/1,
   handle_call/3,
   handle_cast/2,
   handle_info/2,
   terminate/2,
   code_change/3
]).

%%
%% debug macro
-ifdef(DEBUG).
-define(DEBUG(M), error_logger:info_report([{?MODULE, self()}] ++ M)).
-else.
-define(DEBUG(M), true).
-endif.

%%
%% start_link(Ns) - {ok, Pid} | {error, ...}
%%
%% start an entity supervisour 
start_link() ->
   gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
   
init([]) ->
   {ok, []}.

%%
%%  
%%
supervise(Entry) when is_pid(Entry) ->
   gen_server:call(?MODULE, {supervise, Entry}).
   

%% 
%%
handle_call({supervise, Entry}, _Pid, S) ->
   ?DEBUG([{supervise, Entry}]),
   erlang:monitor(process, Entry),
   {reply, ok, S};
handle_call(_Req, _Pid, S) ->
   {reply, {error, not_implemented}, S}.
   
handle_cast(_Req, S) ->
   {noreply, S}.
   
handle_info({'DOWN', _Ref, process, Pid, normal}, S) ->    
   % not a crash, end of life cycle
   ?DEBUG([{normal, Pid}]),
   {noreply, S}; 
handle_info({'DOWN', _Ref, process, Pid, shutdown}, S) -> 
   % not a crash, manual termination
   ?DEBUG([{shutdown, Pid}]),
   {noreply, S};
handle_info({'DOWN', _Ref, process, Pid, _}, S) ->
   % crash, re-spawn
   try 
      Keys = pts_ns:whatis(Pid),
      ?DEBUG([recovery, {pid, Pid}, {keys, Keys}]),
      [{Ns, _} | _ ] = Keys,
      F = pts:i(Ns, factory),
      {ok, Entry} = F({recovery, Keys}),
      erlang:monitor(process, Entry),
      {noreply, S}
   catch
      _ -> 
        {noreply, S}
   end; 
handle_info(_Msg, S) ->
   {noreply, S}.
   
terminate(_Reason, _S) ->
   ok.
   
code_change(_Vsn, S, _Extra) ->
   {ok, S}.
   