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

init([{entity, X} | Opts], S) ->
   init(Opts, S#pts{entity=X});

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   S.

terminate(_, _) ->
   ok.

%%
%%
handle_call({ensure, _Key}, _Tx, #pts{factory=undefined}=S) ->
   {reply, {error, readonly}, S};

handle_call({ensure, Key}, _Tx, #pts{}=S) ->
   case pts:whereis(S, Key) of
      undefined ->
         Result = supervisor:start_child(S#pts.factory, 
            [S#pts.name, key_to_uid(Key, S#pts.keylen)]
         ),
         {reply, Result, S};
      Pid ->
         {reply, {ok, Pid}, S}
   end;

handle_call(_, _Tx, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({set_factory, Sup}, S) ->
   Childs = supervisor:which_children(Sup),
   case lists:keyfind(pts_entity_sup, 1, Childs) of
      false ->
         _ = ets:insert(pts, S),
         {noreply, S};
      {pts_entity_sup, Pid, _, _} ->
         _ = ets:insert(pts, S#pts{factory = Pid}),
         {noreply, S#pts{factory = Pid}}
   end;

handle_info(_, S) ->
   {noreply, S}.

%%
%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}.     

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
