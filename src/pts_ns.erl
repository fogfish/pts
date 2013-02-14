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
%%     name space container
-module(pts_ns).
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).

-include("pts.hrl").
-export([start_link/1]).
%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%
%%
start_link(Bucket) ->
   gen_server:start_link(?MODULE, [Bucket], []).
  
init([#pts{owner=Owner, supervisor=Spec}=Bucket]) ->
   erlang:monitor(process, Owner),
   {ok, Sup} = init_ns_sup(Spec),
   ets:insert(pts, Bucket#pts{supervisor=Sup}),
   {ok, Bucket#pts{supervisor=Sup}}.

init_ns_sup({Mod, Opts}) ->
   erlang:apply(Mod, start_link, Opts);

init_ns_sup(Mod) ->
   erlang:apply(Mod, start_link, []).


terminate(_Reason, #pts{ns=Ns}) ->
   ets:delete(pts, Ns),
   supervisor:terminate_child(pts_sup, Ns),
   supervisor:delete_child(pts_sup, Ns),
   ok.


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
handle_info({'DOWN', _, _, _Pid, _}, S) ->
   {stop, normal, S};

handle_info(drop, S) ->
   {stop, normal, S};

handle_info(_, S) ->
   {noreply, S}.

%%
%%
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     


%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------
