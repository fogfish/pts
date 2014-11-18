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

%%
%% internal state
-record(srv, {
   storage   = undefined :: #pts{}        
  ,length    = 0         :: integer()        % queue length 
  ,queue     = undefined :: any()            % process eviction queue
}).


%%
%%
start_link(Sup, Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Name, Opts], []).

init([Sup, Name, Opts]) ->
   self() ! {set_factory, Sup}, % message to itself, to avoid supervisor deadlock
   {ok, init(Opts, #pts{name=Name})}.

init([{keylen, X} | Opts], S) ->
   init(Opts, S#pts{keylen=X}); 

init([readonly | Opts],  S) ->
   init(Opts, S#pts{readonly=true});

init([immutable | Opts], S) ->
   init(Opts, S#pts{immutable=true});

init(['read-through' | Opts], S) ->
   init(Opts, S#pts{rthrough=true});

init([{protocol, X} | Opts], S) ->
   init(Opts, S#pts{protocol=X});

init([{entity, X} | Opts], S) ->
   init(Opts, S#pts{entity=X});

init([{capacity, X} | Opts], S) ->
   init(Opts, S#pts{capacity=X});

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   #srv{
      storage = S
     ,queue   = queue:new()
   }.

%%
%%
terminate(_, _) ->
   ok.

%%
%%
handle_call({ensure, _Key}, _Tx, #srv{storage=#pts{factory=undefined}}=State) ->
   {reply, {error, readonly}, State};

handle_call({ensure, Key}, _Tx, #srv{storage=Storage}=State) ->
   case pts:whereis(Storage, Key) of
      undefined ->
         Result = supervisor:start_child(Storage#pts.factory, 
            [Storage#pts.name, key_to_uid(Key, Storage#pts.keylen)]
         ),
         {reply, Result, enq_entity(Result, State)};
      Pid ->
         {reply, {ok, Pid}, State}
   end;

handle_call(_, _Tx, State) ->
   {noreply, State}.

%%
%%
handle_cast(_, State) ->
   {noreply, State}.

%%
%%
handle_info({set_factory, Sup}, #srv{storage=Storage}=State) ->
   Childs = supervisor:which_children(Sup),
   case lists:keyfind(pts_entity_sup, 1, Childs) of
      false ->
         _ = ets:insert(pts, Storage),
         {noreply, State};
      {pts_entity_sup, Pid, _, _} ->
         _ = ets:insert(pts, Storage#pts{factory = Pid}),
         {noreply, State#srv{storage=Storage#pts{factory = Pid}}}
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
%% map key to process identity
key_to_uid(Key, inf) ->
   Key;

key_to_uid(Key,   1)
 when is_tuple(Key) ->   
   erlang:element(1, Key);

key_to_uid(Key, Len)
 when is_tuple(Key) ->
   list_to_tuple(
      lists:sublist(
         tuple_to_list(Key),
         Len
      )
   );

key_to_uid(Key, _) ->
   Key.

%%
%% 
enq_entity({error, _}, State) ->
   State;

enq_entity({ok, _}, #srv{storage=#pts{capacity=inf}}=State) ->
   State;

enq_entity({ok, Pid}, #srv{storage=#pts{capacity=C}, length=C}=State) ->
   {N, Queue} = drop_while(C, State#srv.queue),
   State#srv{
      length = N + 1,
      queue  = queue:in(Pid, Queue)
   };
   
enq_entity({ok, Pid}, #srv{length=Length}=State) ->
   State#srv{
      length = Length + 1,
      queue  = queue:in(Pid, State#srv.queue)
   }.

%%
%%
drop_while(N, Queue0) ->
   case queue:out(Queue0) of
      {empty, Queue} ->
         {0, Queue};
      {{value, Pid}, Queue} ->
         case erlang:is_process_alive(Pid) of
            true  ->
               %% @todo: use free signal (shutdown damages the state) 
               erlang:exit(Pid, shutdown),
               {N - 1, Queue};
            false ->
               drop_while(N - 1, Queue)
         end
   end.



