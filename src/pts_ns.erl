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
%%     name space leader process
-module(pts_ns).
-behaviour(gen_server).
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-include("pts.hrl").

-export([
   start_link/3,
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,
   code_change/3
]).


%%%-----------------------------------------------------------------------------
%%%
%%% factory
%%%
%%%-----------------------------------------------------------------------------


%%
%%
start_link(Sup, Name, Opts)
 when is_atom(Name) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Name, Opts], []);

start_link(Sup, Name, Opts) ->
   gen_server:start_link(?MODULE, [Sup, Name, Opts], []).

init([Sup, Name, Opts]) ->
   self() ! {set_factory, Sup}, % message to itself, to avoid supervisor deadlock
   random:seed(os:timestamp()),
   erlang:send_after(?CONFIG_EVICT, self(), evict),
   {ok, lists:foldl(fun init/2, #pts{name=Name, lead=self()}, Opts)}.

init({keylen, X}, State) -> 
   State#pts{keylen=X}; 
init(readonly,    State) -> 
   State#pts{readonly=true};
init(immutable,   State) -> 
   State#pts{immutable=true};
init('read-through',   State) ->
   State#pts{rthrough=true};
init({protocol,   X},  State) ->
   State#pts{protocol=X};
init({entity,     X},  State) ->
   State#pts{entity=X};
init({supervisor, X},  State) ->
   State#pts{entity=X};
init({capacity,   X},  State) ->
   State#pts{capacity=X};
init(_, State) ->
   State.

%%
%%
terminate(_, _) ->
   ok.

%%%-----------------------------------------------------------------------------
%%%
%%% gen_server
%%%
%%%-----------------------------------------------------------------------------

%%
%%
handle_call(_, _Tx, State) ->
   {noreply, State}.

%%
%%
handle_cast(_, State) ->
   {noreply, State}.

%%
%%
handle_info({set_factory, Sup}, State) ->
   Childs = supervisor:which_children(Sup),
   case lists:keyfind(pts_entity_sup, 1, Childs) of
      false ->
         _ = ets:insert(pts, State),
         {noreply, State};
      {pts_entity_sup, Pid, _, _} ->
         _ = ets:insert(pts, State#pts{factory = Pid}),
         {noreply, State#pts{factory = Pid}}
   end;

handle_info(evict, #pts{capacity=inf}=State) ->
   {noreply, State};

handle_info(evict, #pts{name=Ns, capacity=Capacity}=State) ->
   case ets:lookup(pts, Ns) of
      [#pts{size = Size}] when Size > Capacity ->
         {_, N} = evict(Ns),
         ets:update_counter(pts, Ns, [{#pts.size, -1 * N}]),
         erlang:send_after(?CONFIG_EVICT, self(), evict),
         {noreply, State};
      [#pts{size = Size}]           -> 
         erlang:send_after(?CONFIG_EVICT, self(), evict),
         {noreply, State}
   end;

handle_info(_, State) ->
   {noreply, State}.

%%
%%
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.     

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% evicts random elements
%% see http://www.geeksforgeeks.org/select-a-random-number-from-stream-with-o1-space/
evict(Ns) ->
   pns:fold(
      fun({_, Pid}, {N, Acc}) ->
         case random:uniform(N + 1) of
            N -> 
               %% @todo: use free signal (shutdown damages the state) 
               erlang:exit(Pid, shutdown),
               {N + 1, Acc + 1};
            _ -> 
               {N + 1, Acc}
         end
      end,
      {0, 0},
      Ns
   ).

