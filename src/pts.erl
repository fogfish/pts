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
%% internal record, pts metadata
-record(pts, {
   ns        :: atom(),       % table id / namesppace
   kprefix   :: integer() | inf, % length of key 
   readonly  = false :: boolean(),    % write operations are disabled
   rthrough  = false :: boolean(),    % read-through
   immutable = false :: boolean(),    % written value cannont be changed
   factory   :: function()    % factory function  
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
%%    Opt  = {keypos,  integer()} | async | readonly | 
%%           {timeout, integer()} | {factory fun()}
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
init([{factory, X} | T], P) ->
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
      [] ->
         {error, no_namespace};
      [#pts{readonly=true}] ->
         {error, readonly};
      [#pts{factory=undefined}] ->
         {error, readonly};
      [#pts{factory=Fun, kprefix=Pfx, immutable=Imm}=S] ->
         Uid = key_to_uid(Key, Pfx),
         case pns:whereis(Ns, Uid) of
            undefined ->
               {ok, Pid} = Fun(Ns, Uid),
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {put, {self(), Ref}, Key, Val}),
               wait_for_reply(Ref, ?TIMEOUT);
            Pid ->
               case Imm of
                  true  ->
                     {error, duplicate};
                  false ->
                     Ref = erlang:monitor(process, Pid),
                     erlang:send(Pid, {put, {self(), Ref}, Key, Val}),
                     wait_for_reply(Ref, ?TIMEOUT)
               end
         end
   end.   

%%
%% get(Ns, Key) - {ok, Val} | {error, Error}
get(Ns, Key) ->
   case ets:lookup(pts, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{rthrough=true, factory=Fun, kprefix=Pfx}=S] ->
         Uid = key_to_uid(Key, Pfx),
         {ok, Pid} = case pns:whereis(Ns, Uid) of
            undefined -> Fun(Ns, Uid);
            Proc      -> {ok, Proc}
         end,
         Ref = erlang:monitor(process, Pid),
         erlang:send(Pid, {get, {self(), Ref}, Key}),
         wait_for_reply(Ref, ?TIMEOUT);
      [#pts{kprefix=Pfx}=S] ->
         Uid = key_to_uid(Key, Pfx),
         case pns:whereis(Ns, Uid) of
            undefined -> 
               {error, not_found};
            Pid       ->
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {get, {self(), Ref}, Key}),
               wait_for_reply(Ref, ?TIMEOUT)
         end
   end.
   

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
      [#pts{}=S] -> 
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
      [#pts{}=S] -> 
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


