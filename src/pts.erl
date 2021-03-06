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
%%
%%  @todo
%%     * put / get only if process exists 
-module(pts).
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-include("pts.hrl").

-export([start/0]).
-export([
   start_link/2, 
   i/1, 
   i/2,
   whereis/2,
   ensure/2,
   ensure/3,
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
   send/3,
   % fold bucket
   fold/3,
   foreach/2
]).

-type(pts() :: atom() | #pts{}).
-type(key() :: any()).
-type(val() :: any()).

%%
%% start application
start() ->
   application:start(pipe),
   application:start(pns),
   application:start(pts).

%%
%% start new bucket
%% Options:
%%   {keylen,  integer()} - length of key prefix 
%%   readonly             - only read operations are restricted (no writes)
%%   immutable            - any existed value cannot be replaced 
%%   'read-through'       - read operation creates process instance if it is not exists
%%   {entity,        ...} - defines entity (worker) container (used along with factory)
%%   {supervisor,    ...} - defines entity (supervisor) container (used along with factory)
%%   {factory,    atom()} - defines recovery strategy for entity processes, 
%%                          processes are created out-side of pts supervisor by application
%%                          if factory is not defined.
%%   {protocol, pipe | server} - define ipc protocol (default pipe)
-spec start_link(pts(), list()) -> {ok, pid()} | {error, any()}.

start_link(Name, Opts) ->
   pts_ns_sup:start_link(Name, Opts).

%%
%% return meta data for given table
-spec i(pts()) -> list().
-spec i(atom(), pts()) -> list().

i(Ns) ->
   case ns(Ns) of
      undefined -> 
         [];
      #pts{}=T  -> 
         lists:zip(
            record_info(fields, pts), 
            tl(tuple_to_list(T))
         )
   end.

%%
%% return meta data for given table
i(Prop, Ns) ->
   case lists:keyfind(Prop, 1, i(Ns)) of
      false    -> throw(badarg);
      {_, Val} -> Val
   end.

%%
%% return pid() of processes
-spec whereis(pts(), key()) -> pid() | undefined.

whereis(#pts{name=Name, keylen=Keylen}, Key) ->
   pns:whereis(Name, key_to_uid(Key, Keylen));
whereis(Ns, Key) ->
   whereis(ns(Ns), Key).

%%
%% ensure process is exists and returns its pid()
%% process creation is serialized to avoid key collisions
-spec ensure(pts(), key()) -> {ok, pid()} | {error, any()}.
-spec ensure(pts(), key(), list()) -> {ok, pid()} | {error, any()}.

ensure(Ns, Key) ->
   ensure(Ns, Key, []).

ensure(#pts{factory=undefined}, _Key, _Args) ->
   {error, readonly};

ensure(#pts{name=Ns, keylen=Keylen, factory=Sup}=Storage, Key, Args) ->
   case pts:whereis(Storage, Key) of
      undefined ->
         case 
            supervisor:start_child(Sup, 
               [Ns, key_to_uid(Key, Keylen)] ++ Args
            )
         of
            {ok, Pid} ->
               ets:update_counter(pts, Ns, [{#pts.size, 1}]),
               {ok, Pid};
            {error, {badarg, {_,_}}} ->
               ensure(Storage, Key, Args);
            {error, {shutdown, {failed_to_start_child, _, {badarg,{_,_}}}}} ->
               ensure(Storage, Key, Args);
            {error, Reason} ->
               {error, Reason}
         end;
      Pid ->
         {ok, Pid}
   end;
ensure(Ns, Key, Args) ->
   ensure(ns(Ns), Key, Args).

%%
%% synchronous put value
-spec put(pts()|pid(), key(), val()) -> ok | {error, any()}.
-spec put(pts()|pid(), key(), val(), timeout()) -> ok | {error, any()}.

put(Ns, Key, Val) ->
   pts:put(Ns, Key, Val, ?DEF_TIMEOUT).

put(#pts{}=Ns, Key, Val, Timeout) ->
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         case (catch pcall(Ns#pts.protocol, Pid, {put, Key, Val}, Timeout)) of
            {'EXIT', noproc} -> 
               {error, not_found};
            {'EXIT', normal} ->
               {error, not_found};
            {'EXIT', {error, _}=Reason} ->
               Reason;
            Result           ->
               Result            
         end;
      Error     ->
         Error
   end;
put(Ns, Key, Val, Timeout) ->
   put(ns(Ns), Key, Val, Timeout).

%%
%% asynchronous put value
-spec put_(pts()|pid(), key(), val()) -> reference().
-spec put_(pts()|pid(), key(), val(), boolean()) -> ok | reference().

put_(Ns, Key, Val) ->
   pts:put_(Ns, Key, Val, true).

put_(#pts{}=Ns, Key, Val,  true) ->
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         pcast(Ns#pts.protocol, Pid, {put, Key, Val});
      Error     ->
         Error
   end;
put_(#pts{}=Ns, Key, Val, false) ->
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         psend(Ns#pts.protocol, Pid, {put, Key, Val}), ok;
      Error     ->
         Error
   end;
put_(Ns, Key, Val, Flag) ->
   put_(ns(Ns), Key, Val, Flag).

%%
%% synchronous get value
-spec get(atom(), any()) -> any() | {error, any()}.
-spec get(atom(), any(), timeout()) -> any() | {error, any()}.

get(Ns, Key) ->
   pts:get(Ns, Key, ?DEF_TIMEOUT).

get(#pts{}=Ns, Key, Timeout) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         case (catch pcall(Ns#pts.protocol, Pid, {get, Key}, Timeout)) of
            {'EXIT', noproc} -> 
               {error, not_found};
            {'EXIT', normal} ->
               {error, not_found};
            {'EXIT', {error, _}=Reason} ->
               Reason;
            Result           ->
               Result            
         end;
      Error     ->
         Error
   end;
get(Ns, Key, Timeout) ->
   get(ns(Ns), Key, Timeout).

%%
%% asynchronous get value
-spec get_(atom(), any()) -> reference().
-spec get_(atom(), any(), boolean()) -> ok | reference().

get_(Ns, Key) ->
   pts:get_(Ns, Key, true).

get_(#pts{}=Ns, Key, true) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         pcast(Ns#pts.protocol, Pid, {get, Key});
      Error     ->
         Error
   end;
get_(#pts{}=Ns, Key, false) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         psend(Ns#pts.protocol, Pid, {get, Key}), ok;
      Error     ->
         Error
   end;
get_(Ns, Key, Flags) ->
   get_(ns(Ns), Key, Flags).

%%
%% synchronous remove value
-spec remove(atom(), any()) -> ok | {error, any()}.
-spec remove(atom(), any(), timeout()) -> ok | {error, any()}.

remove(Ns, Key) ->
   pts:remove(Ns, Key, ?DEF_TIMEOUT).

remove(#pts{}=Ns, Key, Timeout) ->   
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         case (catch pcall(Ns#pts.protocol, Pid, {remove, Key}, Timeout)) of
            {'EXIT', noproc} -> 
               {error, not_found};
            {'EXIT', normal} ->
               {error, not_found};
            {'EXIT', {error, _}=Reason} ->
               Reason;
            Result           ->
               Result            
         end;      
      Error     ->
         Error
   end;
remove(Ns, Key, Timeout) ->
   remove(ns(Ns), Key, Timeout).

%%
%% asynchronous remove value
-spec remove_(atom(), any()) -> reference().
-spec remove_(atom(), any(),boolean()) -> ok | reference().

remove_(Ns, Key) ->
   pts:remove_(Ns, Key, true).

remove_(#pts{}=Ns, Key, true) ->
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         pcast(Ns#pts.protocol, Pid, {remove, Key});
      Error     ->
         Error
   end;
remove_(#pts{}=Ns, Key, false) ->
   case where_to_write(Ns, Key) of
      {ok, Pid} ->
         psend(Ns#pts.protocol, Pid, {remove, Key}), ok;
      Error     ->
         Error
   end;
remove_(Ns, Key, Flags) ->
   remove_(ns(Ns), Key, Flags).

%%
%% call process
-spec call(atom(), any(), any()) -> any().
-spec call(atom(), any(), any(), timeout()) -> any().

call(Ns, Key, Msg) ->
   call(Ns, Key, Msg, ?DEF_TIMEOUT).

call(#pts{}=Ns, Key, Msg, Timeout) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         pcall(Ns#pts.protocol, Pid, Msg, Timeout);
      Error     ->
         Error
   end;
call(Ns, Key, Msg, Timeout) ->
   call(ns(Ns), Key, Msg, Timeout).

%%
%% cast message to process
-spec cast(atom(), any(), any()) -> ok.

cast(#pts{}=Ns, Key, Msg) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         pcast(Ns#pts.protocol, Pid, Msg);
      Error     ->
         Error
   end;
cast(Ns, Key, Msg) ->
   cast(ns(Ns), Key, Msg).

%%
%% send message to process
-spec send(atom(), any(), any()) -> ok.

send(#pts{}=Ns, Key, Msg) ->
   case where_to_read(Ns, Key) of
      {ok, Pid} ->
         psend(Ns#pts.protocol, Pid, Msg);
      Error     ->
         Error
   end;
send(Ns, Key, Msg) ->
   send(ns(Ns), Key, Msg).
   
%%
%% fold function Fun(Key, Acc) over name space
-spec fold(function(), any(), pts()) -> any().

fold(Fun, Acc0, #pts{name=Ns}) ->
   pns:fold(
      fun({Key, Pid}, Acc) -> 
         Fun(Key, Pid, Acc)
      end, 
      Acc0, 
      Ns
   );

fold(Fun, Acc0, Ns) ->
   fold(Fun, Acc0, ns(Ns)).

%%
%% applies a function to each element for its side-effects.
-spec foreach(function(), pts()) -> ok.

foreach(Fun, #pts{name=Ns}) ->
   pns:fold(
      fun({Key, Pid}, Acc) -> 
         Fun(Key, Pid), Acc
      end, 
      ok, 
      Ns
   );

foreach(Fun, Ns) ->
   foreach(Fun, ns(Ns)).


%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% lookup bucket
ns(Ns) ->
   case ets:lookup(pts, Ns) of
      [#pts{}=X] -> X;
      _          -> exit(noproc)
   end.

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
%% assert write operation
where_to_write(#pts{readonly=true}, _Key) ->
   {error, readonly};
where_to_write(#pts{immutable=true}=Ns, Key) ->
   case whereis(Ns, Key) of
      undefined -> ensure(Ns, Key);
      _Pid      -> {error, readonly}
   end;
where_to_write(#pts{} = Ns, Key) ->
   case whereis(Ns, Key) of
      undefined -> ensure(Ns, Key);
      Pid       -> {ok, Pid}
   end.

%%
%% assert read operation
where_to_read(#pts{rthrough=true}=Ns, Key) ->
   case whereis(Ns, Key) of
      undefined -> ensure(Ns, Key);
      Pid       -> {ok, Pid}
   end;
where_to_read(#pts{}=Ns, Key) ->
   case whereis(Ns, Key) of
      undefined -> {error, not_found};
      Pid       -> {ok, Pid}
   end.

%%
%%
pcall(pipe,   Pid, Msg, Timeout) -> 
   pipe:call(Pid, Msg, Timeout);
pcall(server, Pid, Msg, Timeout) -> 
   gen_server:call(Pid, Msg, Timeout).

pcast(pipe, Pid, Msg) ->
   pipe:cast(Pid, Msg);
pcast(server, Pid, Msg) ->
   gen_server:cast(Pid, Msg).

psend(pipe, Pid, Msg) ->
   pipe:send(Pid, Msg);
psend(server, Pid, Msg) ->
   erlang:send(Pid, Msg).

