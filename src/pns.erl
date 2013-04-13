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
%%     process name space
%%
-module(pns).
-author(dmkolesnikov@gmail.com).
-include_lib("stdlib/include/qlc.hrl").

-export([
   start_link/0, 
   %% api
   register/2, register/3, unregister/2, whereis/2, whatis/2, lookup/2, map/2, fold/3,

   %% gen_server
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%%
%% create new pns
-spec(start_link/0 :: () -> {ok, pid()} | {error, any()}).

start_link() ->
   gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%
%% Associates Uid with a Pid of current process.
%% Fails with badarg if association exists and associated process is alive
-spec(register/2 :: (any(), any()) -> ok).
-spec(register/3 :: (any(), any(), pid()) -> ok).

register(Ns, Uid) ->
   ?MODULE:register(Ns, Uid, self()).

register(Ns, Uid, Pid) ->
   case ets:insert_new(pns, {{Ns, Uid}, Pid}) of
      true  -> 
         ok;
      false ->
         % existed key might be associated with dead processes
         % parallel register to same key are plausible
         % handle update via server
         case gen_server:call(?MODULE, {register, Ns, Uid, Pid}) of
            ok -> 
               ok;
            _  -> 
               throw({badarg, {Ns, Uid}})
         end
   end.

%%
%% Removes the registered Uid association with a Pid.
-spec(unregister/2 :: (any(), any()) -> ok).

unregister(Ns, Uid) ->
   ets:delete(pns, {Ns, Uid}),
   ok.

%%
%% Returns the Pid assotiated with Uid. 
%% Returns undefined if the name is not registered.   
-spec(whereis/2 :: (any(), any()) -> pid() | undefined).

whereis(Ns, Uid) ->
   case ets:lookup(pns, {Ns, Uid}) of
      [{_, Pid}] ->
         case is_process_alive(Pid) of
            true  -> Pid;
            false -> undefined
         end;
      _            -> 
         undefined
   end.   
 
%%
%% Returns all Uid associated with Pid
-spec(whatis/2 :: (any(), pid()) -> [pid()]).

whatis(Ns, Pid) ->
   ets:select(pns, 
      [
         {{{Ns, '$1'}, Pid}, [], ['$1']}
      ]
   ).

%%
%%
-spec(lookup/2 :: (any(), any()) -> [pid()]).

lookup(Ns, Uid) ->
   List = ets:select(pns, 
      [
         {{{Ns, Uid}, '_'}, [], ['$_']}
      ]
   ),
   [{Key, Pid} || {{_, Key}, Pid} <- List, is_process_alive(Pid)].


%%
%% map function over name space
%% Fun = fun({Uid, Pid}) 
-spec(map/2 :: (function(), atom()) -> list()).

map(Fun, Ns0) ->
   qlc:e(
      qlc:q([ 
         Fun({Uid, Pid}) 
         || {{Ns, Uid}, Pid} <- ets:table(pns), Ns =:= Ns0, is_process_alive(Pid)
      ])
   ).
   
%%
%% fold function
-spec(fold/3 :: (function(), any(), atom()) -> list()).

fold(Fun, Acc0, Ns0) ->
   qlc:fold(Fun, Acc0, 
      qlc:q([ 
         {Uid, Pid} || {{Ns, Uid}, Pid} <- ets:table(pns), Ns =:= Ns0, is_process_alive(Pid)
      ])
   ).      

%%-----------------------------------------------------------------------------
%%
%% gen_server
%%
%%-----------------------------------------------------------------------------

init(_) ->
   _ = ets:new(pns, [
      public,
      named_table,
      ordered_set,
      {read_concurrency, true}
   ]),
   {ok, undefined}.

terminate(_, _) ->
   ok.

%%
handle_call({register, Ns, Uid, Pid}, _, S) ->
   % ensure that no race-condition on registered key
   case ?MODULE:whereis(Ns, Uid) of
      undefined ->
         ets:insert(pns, {{Ns, Uid}, Pid}),
         {reply, ok, S};
      _         ->
         {reply, conflict, S}
   end;

handle_call(_, _, S) ->
   {noreply, S}.

%%
handle_cast(_, S) ->
   {noreply, S}.

%%
handle_info(_, S) ->
   {noreply, S}.

%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}. 

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

   