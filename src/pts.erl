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
-module(pts).
-author(dmkolesnikov@gmail.com).

%%
%% In-Process Term Storage
%%
%% The library provides hashtable-like interface to manipulate data 
%% distributed through Erlang processes.
%%


%% Management
-export([new/1, new/2, drop/1, i/0, i/1, i/2]).
%% Hashtable
-export([put/3, get/2, remove/2]).
%% map/fold
-export([map/2, fold/3]).

%%
%% internal record, pts meta-data
-record(pts, {
   ns        :: atom(),               % table id / name-space
   kprefix   = inf   :: integer() | inf, % length of key 
   readonly  = false :: boolean(),    % write operations are disabled
   rthrough  = false :: boolean(),    % read-through
   immutable = false :: boolean(),    % written value cannot be changed
   factory   :: function()            % factory function Fun(Ns, Uid)
}).
-define(TIMEOUT, 10000).

%%-----------------------------------------------------------------------------
%%
%% table management
%%
%%-----------------------------------------------------------------------------

%%
%% new(Ns, Opts) -> ok
%%    Name = term()
%%    Opts = [Option]
%%       {factory, Fun(Ns, Uid)}
%%
%% Creates a new namespace
%%
new(Ns) ->
   new(Ns, []).
   
new(Ns, Opts) ->
   case ets:lookup(pts, Ns) of
      [] -> ets:insert(pts, init(Opts, #pts{ns=Ns})), ok;
      _  -> throw(badarg)
   end.

init([{kprefix, X} | T], P) ->
   init(T, P#pts{kprefix=X});
init([{factory, {Mod, Opts}} | T], P) ->
   {ok, _} = pts_ns_sup:append(Mod, Opts),
   init(T, P#pts{factory=Mod});
init([{factory, X} | T], P)
 when is_function(X) ->
   init(T, P#pts{factory=X});
init([readonly | T], P) ->
   init(T, P#pts{readonly=true});
init([immutable | T], P) ->
   init(T, P#pts{immutable=true});
init(['read-through' | T], P) ->
   init(T, P#pts{rthrough=true});
init([], P) ->
   P.

%%
%% delete(Ns) -> ok
%%
drop(Ns) ->
   case ets:lookup(pts, Ns) of
      [_] -> ets:delete(pts, Ns), ok;
      _   -> ok
   end.   
   
%%
%% i() -> [Meta]
%%
%% return metadata of defined tables
i() ->
   ets:tab2list(pts).
  
%%
%% i(Tab) -> {ok, Meta} | {error, Reason}
%%
%% return meta data for given table
i(Ns) ->
   case ets:lookup(pts, Ns) of
      [T] -> {ok, T};
      _   -> {error, no_table}
   end.

%%
%% i(Property, Tab) -> {ok, Meta} | {error, Reason}
%%
%% return meta data for given table
i(ns, #pts{ns = Val}) ->
   Val;
i(factory, #pts{factory = Val}) ->
   Val;
i(_, #pts{}) ->
   throw(badarg);
i(Prop, Ns) ->
   case ets:lookup(pts, Ns) of
      [T] -> i(Prop, T);
      _   -> {error, no_table}
   end.


%%-----------------------------------------------------------------------------
%%
%% put/get
%%
%%-----------------------------------------------------------------------------

%%
%% create(Ns, Key, Val) -> ok | {error, Reason}
%%
put(Ns, Key, Val) ->
   case ets:lookup(pts, Ns) of
      [] -> {error, no_namespace};
      [#pts{readonly=true}]     -> {error, readonly};
      [#pts{factory=undefined}] -> {error, readonly};
      [#pts{}=P]                -> do_put(Key, Val, P)
   end.   

do_put(Key, Val, #pts{ns=Ns, kprefix=Pfx}=P) ->
   Uid = key_to_uid(Key, Pfx),
   try 
      do_put(pns:whereis(Ns, Uid), Uid, Key, Val, P)
   catch
      error:{badmatch, {error, Reason}} -> {error, Reason}
   end.

do_put(undefined, Uid, Key, Val, #pts{ns=Ns, factory=Fun}) ->
   % create a new process
   {ok, Pid} = create(Fun, Ns, Uid),
   Tx = erlang:monitor(process, Pid),
   erlang:send(Pid, {put, {self(), Tx}, Key, Val}),
   wait_for_reply(Tx, ?TIMEOUT);

do_put(_Pid, _Uid, _Key, _Val, #pts{immutable=true}) ->
   {error, duplicate};

do_put(Pid, _Uid, Key, Val, #pts{}) ->
   % update process
   Tx = erlang:monitor(process, Pid),
   erlang:send(Pid, {put, {self(), Tx}, Key, Val}),
   wait_for_reply(Tx, ?TIMEOUT).


%%
%% get(Ns, Key) - {ok, Val} | {error, Error}
get(Ns, Key) ->
   case ets:lookup(pts, Ns) of
      []         -> {error, no_namespace};
      [#pts{}=P] -> do_get(Key, P)
   end.

do_get(Key, #pts{ns=Ns, kprefix=Pfx}=P) ->
   Uid = key_to_uid(Key, Pfx),
   try 
      do_get(pns:whereis(Ns, Uid), Uid, Key, P)
   catch
      error:{badmatch, {error, Reason}} -> {error, Reason}
   end.  

do_get(undefined, Uid, Key, #pts{rthrough=true, ns=Ns, factory=Fun}) ->
   {ok, Pid} = create(Fun, Ns, Uid),
   Tx = erlang:monitor(process, Pid),
   erlang:send(Pid, {get, {self(), Tx}, Key}),
   wait_for_reply(Tx, ?TIMEOUT);

do_get(undefined, _Uid, _Key, #pts{rthrough=false}) ->
   {error, not_found};

do_get(Pid, _Uid, Key, #pts{}) ->
   Tx = erlang:monitor(process, Pid),
   erlang:send(Pid, {get, {self(), Tx}, Key}),
   wait_for_reply(Tx, ?TIMEOUT).

%%
%%
remove(Ns, Key) ->
   case ets:lookup(pts, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly=true}] ->
         {error, readonly};
      [#pts{factory=undefined}] ->
         {error, readonly};
      [#pts{kprefix=Pfx}] ->
         case pns:whereis(Ns, key_to_uid(Key, Pfx)) of
            undefined -> 
               ok;
            Pid       -> 
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {remove, {self(), Ref}, Key}),
               wait_for_reply(Ref, ?TIMEOUT)
         end
   end.
      
%%
%% map(Tab, Fun) -> List
%%
map(Ns, Fun) ->   
   case ets:lookup(pts, Ns) of
      []   -> {error, no_namespace};
      [#pts{}] -> 
         pns:map(Ns, 
            fun({Key, Pid}) ->
               Getter = fun() ->
                  Ref = erlang:monitor(process, Pid),
                  erlang:send(Pid, {get, {self(), Ref}, Key}),
                  wait_for_reply(Ref, ?TIMEOUT)
               end,
               Fun({Key, Getter})
            end
         )
   end.  

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Ns, Acc, Fun) ->   
   case ets:lookup(pts, Ns) of
      []   -> {error, no_namespace};
      [#pts{}] -> 
         pns:fold(Ns, Acc, 
            fun({Key, Pid}, A) ->
               Getter = fun() ->
                  Ref = erlang:monitor(process, Pid),
                  erlang:send(Pid, {get, {self(), Ref}, Key}),
                  wait_for_reply(Ref, ?TIMEOUT)
               end,
               Fun({Key, Getter}, A)
            end
         )
   end.   
      
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% transforms key to unique process identifier   
key_to_uid(Key, inf) ->
   Key;
key_to_uid(Key,   1) when is_tuple(Key) ->   
   element(1, Key);
key_to_uid(Key, Len) when is_tuple(Key) ->
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
wait_for_reply(Ref, Timeout) ->
   receive
      {Ref, Reply} ->
         erlang:demonitor(Ref, [flush]),
         Reply;
      {'DOWN', Ref, _, _, Reason} ->
         {error, Reason}
   after Timeout ->
      erlang:demonitor(Ref),
      receive
         {'DOWN', Ref, _, _, _} -> true
      after 0 -> true
      end,
      {error, timeout}
   end.

%%
%%
create(Fun, Ns, Uid)
 when is_function(Fun) ->
   Fun(Ns, Uid);
create(Fun, Ns, Uid)
 when is_atom(Fun) ->
   pts_factory:create(Fun, Ns, Uid).

