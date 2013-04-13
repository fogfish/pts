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
%%     In-Process Term Storage: the library provides hashtable-like interface 
%%     to manipulate data distributed through Erlang processes.
-module(pts).
-author(dmkolesnikov@gmail.com).
-include("pts.hrl").

-export([
   start_link/2, 
   i/1, i/2,

   % put key-val to bucket
   put/3, put/4, put_/3, put_/4,

   % get kev-val from bucket 
   get/2, get/3, get_/2, get_/3,

   % remove key-val from bucket
   remove/2, remove/3, remove_/2, remove_/3, 

   % communicate with process
   call/3, cast/3, send/3, ack/2,

   % map, fold bucket
   map/2,  fold/3
]).

-type(pts() :: atom() | tuple()).

%%
%% start new bucket
-spec(start_link/2 :: (pts(), list()) -> {ok, pid()} | {error, any()}).

start_link(Name, Opts) ->
   pts_ns_sup:start_link(Name, Opts).

%%
%% return meta data for given table
-spec(i/1 :: (atom()) -> list()).

i(Ns) ->
   gen_server:call(Ns, i).

%%
%% return meta data for given table
i(Prop, Ns) ->
   case lists:keyfind(Prop, 1, i(Ns)) of
      false    -> throw(badarg);
      {_, Val} -> Val
   end.

%%
%% put value
-spec(put/3  :: (atom(), any(), any()) -> ok | {error, any()}).
-spec(put/4  :: (atom(), any(), any(), timeout()) -> ok | {error, any()}).
-spec(put_/3 :: (atom(), any(), any()) -> {ok, reference()} | {error, any()}).
-spec(put_/4 :: (atom(), any(), any(), boolean()) -> ok | {ok, reference()} | {error, any()}).

put(Ns, Key, Val) ->
   pts:put(Ns, Key, Val, ?DEF_TIMEOUT).

put(Ns, Key, Val, Timeout) ->
   gen_server:call(pns:whereis(Ns), {put, Key, Val}, Timeout).

put_(Ns, Key, Val) ->
   pts:put_(Ns, Key, Val, true).

put_(Ns, Key, Val,  true) ->
   Tx = erlang:make_ref(),
   gen_server:cast(pns:whereis(Ns), {put, {tx, self(), Tx}, Key, Val}),
   {ok, Tx};

put_(Ns, Key, Val, false) ->
   erlang:send(pns:whereis(Ns), {put, Key, Val}),
   ok.

%%
%% get value
-spec(get/2  :: (atom(), any()) -> {ok, any()} | {error, any()}).
-spec(get/3  :: (atom(), any(), timeout()) -> {ok, any()} | {error, any()}).
-spec(get_/2 :: (atom(), any()) -> {ok, reference()} | {error, any()}).
-spec(get_/3 :: (atom(), any(), boolean()) -> ok | {ok, reference()} | {error, any()}).

get(Ns, Key) ->
   pts:get(Ns, Key, ?DEF_TIMEOUT).

get(Ns, Key, Timeout) ->
   gen_server:call(pns:whereis(Ns), {get, Key}, Timeout).

get_(Ns, Key) ->
   pts:get_(Ns, Key, true).

get_(Ns, Key, true) ->
   Tx = erlang:make_ref(),
   gen_server:cast(pns:whereis(Ns), {get, {tx, self(), Tx}, Key}),
   {ok, Tx};

get_(Ns, Key, false) ->
   erlang:send(pns:whereis(Ns), {get, Key}),
   ok.


%%
%% remove value
-spec(remove/2  :: (atom(), any()) -> ok | {error, any()}).
-spec(remove/3  :: (atom(), any(), timeout()) -> {ok, reference()} | {error, any()}).
-spec(remove_/2 :: (atom(), any()) -> ok | {error, any()}).
-spec(remove_/3 :: (atom(), any(),boolean()) -> ok | {ok, reference()} | {error, any()}).

remove(Ns, Key) ->
   pts:remove(Ns, Key, ?DEF_TIMEOUT).

remove(Ns, Key, Timeout) ->   
   gen_server:call(pns:whereis(Ns), {remove, Key}, Timeout).
  
remove_(Ns, Key) ->
   pts:remove_(Ns, Key, true).

remove_(Ns, Key, true) ->
   Tx = erlang:make_ref(),
   gen_server:cast(pns:whereis(Ns), {remove, {tx, self(), Tx}, Key}),
   {ok, Tx};

remove_(Ns, Key, false) ->
   erlang:send(pns:whereis(Ns), {remove, Key}),
   ok.

%%
%% 
-spec(call/3 :: (atom(), any(), any()) -> any()).

call(Ns, Key, Msg) ->
   gen_server:call(pns:whereis(Ns), {call, Key, Msg}).

%%
%%
-spec(cast/3 :: (atom(), any(), any()) -> ok).

cast(Ns, Key, Msg) ->
   gen_server:cast(pns:whereis(Ns), {cast, Key, Msg}).

%%
%% 
-spec(send/3 :: (atom(), any(), any()) -> any()).

send(Ns, Key, Msg) ->
   erlang:send(pns:whereis(Ns), {send, Key, Msg}),
   ok.

%%
%% acknowledge transaction
-spec(ack/2 :: (any(), any()) -> ok).

ack(undefined, _Msg) ->
   ok;

ack({tx, Pid, Ref}, Msg) ->
   Pid ! {Ref, Msg};

ack({_, _}=Tx, Msg) ->
   gen_server:reply(Tx, Msg).

%%
%% map(Tab, Fun) -> List
%%
map(Fun, Ns) ->
   pns:map( 
      fun({Key, Pid}) ->
         Getter = fun() ->
            try
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {get, {self(), Ref}, Key}),
               wait_for_reply(Ref, 5000)
            catch _:_ ->
               undefined
            end
         end,
         Fun({Key, Getter})
      end,
      Ns
   ).

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Fun, Acc, Ns) ->
   pns:fold( 
      fun({Key, Pid}, A) ->
         Getter = fun() ->
            try
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {get, {self(), Ref}, Key}),
               wait_for_reply(Ref, 5000)
            catch _:_ ->
               undefined
            end
         end,
         Fun({Key, Getter}, A)
      end,
      Acc, Ns
   ).   

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%%
wait_for_reply(Ref, Timeout) ->
   receive
      {Ref, {ok, Reply}} ->
         erlang:demonitor(Ref, [flush]),
         Reply;
      {Ref, Reply} ->
         erlang:demonitor(Ref, [flush]),
         Reply;
      {'DOWN', Ref, _, _, Reason} ->
         throw(Reason)
   after Timeout ->
      erlang:demonitor(Ref),
      receive
         {'DOWN', Ref, _, _, _} -> true
      after 0 -> true
      end,
      throw(timeout)
   end.

