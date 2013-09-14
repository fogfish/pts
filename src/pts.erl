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
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-include("pts.hrl").

-export([start/0]).
-export([
   start_link/2, 
   i/1, 
   i/2,
   whereis/2,
   % put key-val to bucket
   put/3, 
   put/4, 
   put_/3, 
   put_/4,
   % get kev-val from bucket 
   get/2, 
   get/3, 
   get_/2, 
   get_/3,
   % remove key-val from bucket
   remove/2, 
   remove/3, 
   remove_/2, 
   remove_/3, 
   % communicate with process
   call/3, 
   call/4, 
   cast/3,
   % map, fold bucket
   map/2,
   fold/3
]).

-type(pts() :: atom()).
-type(key() :: any()).
-type(val() :: any()).

%%
%% start application
start() ->
   _ = application:start(pns),
   application:start(pts).

%%
%% start new bucket
%% Options:
%%   {keylen, integer()} - length of key prefix 
%%   readonly
%%   immutable
%%   'read-through'
%%   {entity, mfa()}
%%   supervisor
-spec(start_link/2 :: (pts(), list()) -> {ok, pid()} | {error, any()}).

start_link(Name, Opts) ->
   pts_ns_sup:start_link(Name, Opts).

%%
%% return meta data for given table
-spec(i/1 :: (pts()) -> list()).
-spec(i/2 :: (atom(), pts()) -> list()).

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
%% return pid() of processes
-spec(whereis/2 :: (pts(), key()) -> pid() | undefined).

whereis(Ns, Key) ->
   gen_server:call(Ns, {whereis, Key}).


%%
%% synchronous put value
-spec(put/3  :: (pts()|pid(), key(), val()) -> ok | {error, any()}).
-spec(put/4  :: (pts()|pid(), key(), val(), timeout()) -> ok | {error, any()}).

put(Ns, Key, Val) ->
   pts:put(Ns, Key, Val, ?DEF_TIMEOUT).

put(Ns, Key, Val, Timeout) ->
   plib:call(Ns, put, {Key, Val}, Timeout).

%%
%% asynchronous put value
-spec(put_/3 :: (pts()|pid(), key(), val()) -> reference()).
-spec(put_/4 :: (pts()|pid(), key(), val(), boolean()) -> reference()).

put_(Ns, Key, Val) ->
   pts:put_(Ns, Key, Val, true).

put_(Ns, Key, Val,  true) ->
   plib:cast(Ns, put, {Key, Val});

put_(Ns, Key, Val, false) ->
   plib:send(Ns, put, {Key, Val}).

%%
%% synchronous get value
-spec(get/2  :: (atom(), any()) -> any() | {error, any()}).
-spec(get/3  :: (atom(), any(), timeout()) -> any() | {error, any()}).

get(Ns, Key) ->
   pts:get(Ns, Key, ?DEF_TIMEOUT).

get(Ns, Key, Timeout) ->
   plib:call(Ns, get, Key, Timeout).

%%
%% asynchronous get value
-spec(get_/2 :: (atom(), any()) -> reference() | {error, any()}).
-spec(get_/3 :: (atom(), any(), boolean()) -> ok | reference() | {error, any()}).

get_(Ns, Key) ->
   pts:get_(Ns, Key, true).

get_(Ns, Key, true) ->
   plib:cast(Ns, get, Key);

get_(Ns, Key, false) ->
   plib:send(Ns, get, Key).

%%
%% synchronous remove value
-spec(remove/2  :: (atom(), any()) -> ok | {error, any()}).
-spec(remove/3  :: (atom(), any(), timeout()) -> ok | {error, any()}).

remove(Ns, Key) ->
   pts:remove(Ns, Key, ?DEF_TIMEOUT).

remove(Ns, Key, Timeout) ->   
   plib:call(Ns, remove, Key, Timeout).

%%
%% asynchronous remove value
-spec(remove_/2 :: (atom(), any()) -> reference() | {error, any()}).
-spec(remove_/3 :: (atom(), any(),boolean()) -> ok | reference() | {error, any()}).

remove_(Ns, Key) ->
   pts:remove_(Ns, Key, true).

remove_(Ns, Key, true) ->
   plib:cast(Ns, remove, Key);

remove_(Ns, Key, false) ->
   plib:send(Ns, remove, Key).

%%
%% process call
-spec(call/3 :: (atom(), any(), any()) -> any()).
-spec(call/4 :: (atom(), any(), any(), timeout()) -> any()).

call(Ns, Key, Msg) ->
   call(Ns, Key, Msg, ?DEF_TIMEOUT).

call(Ns, Key, Msg, Timeout) ->
   % sync call uses plib protocol but otp gen_call is simulated
   % it allows plib relay to delivery otp complaint request to 
   % destination process
   plib:call(Ns, '$gen_call', {Key, Msg}, Timeout).

%%
%% process cast
-spec(cast/3 :: (atom(), any(), any()) -> ok).

cast(Ns, Key, Msg) ->
   % uses plib complaint cast notation
   plib:cast(Ns, '$gen_call', {Key, Msg}).

%%
%% map function Fun({Key, Pid}) over name space
-spec(map/2 :: (function(), pts()) -> list()).

map(Fun, Ns) ->
   pns:map(fun({_Key, _Pid}=X) -> Fun(X) end, Ns).

%%
%% fold function Fun({Key, Pid}, Acc) over name space
-spec(fold/3 :: (function(), any(), pts()) -> list()).

fold(Fun, Acc0, Ns) ->
   pns:fold(fun({_Key, _Pid}=X, Acc) -> Fun(X, Acc) end, Acc0, Ns).   

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

