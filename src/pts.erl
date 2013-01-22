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
-export([new/1, new/2, drop/1, i/1, i/2]).
%% Hashtable
-export([put/3, put/4, get/2, get/3, remove/2, remove/3]).
%% map/fold
-export([map/2, map/3, fold/3, fold/4]).

%%
%% internal record, pts meta-data
-record(pts, {
   ns        :: atom(),                % unique name-space id
   keylen    = inf   :: integer() | inf, % length of key (significant park of key used to distinguish a process)
   readonly  = false :: boolean(),     % write operations are disabled
   rthrough  = false :: boolean(),     % read-through
   immutable = false :: boolean(),     % write-once (written value cannot be changed)
   supervisor        :: atom() | pid() % element supervisor (simple_one_for_one)
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

init([{keylen, X} | T], P) ->
   init(T, P#pts{keylen=X});   
init([{factory, _} | _], _) ->
    % the factory function is deprecated
   throw(badarg);
init([{supervisor, Mod} | T], #pts{ns=Ns}=P) ->
   {ok, Pid} = supervisor:start_child(pts_sup, {
      Ns,
      {Mod, start_link, []},
      permanent, 1000, supervisor, dynamic
   }),
   init(T, P#pts{supervisor=Pid});
init([{supervisor, Mod, Opts} | T], #pts{ns=Ns}=P) ->
   {ok, Pid} = supervisor:start_child(pts_sup, {
      Ns,
      {Mod, start_link, Opts},
      permanent, 1000, supervisor, dynamic
   }),
   init(T, P#pts{supervisor=Pid});
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
      [_] -> 
         ets:delete(pts, Ns), 
         supervisor:terminate_child(pts_sup, Ns),
         supervisor:delete_child(pts_sup, Ns),
         ok;
      _   -> ok
   end.   
   
%%
%% i(Tab) -> {ok, Meta} | {error, Reason}
%%
%% return meta data for given table
i(Ns) ->
   case ets:lookup(pts, Ns) of
      [T] -> lists:zip(record_info(fields, pts), tl(tuple_to_list(T)));
      _   -> throw(badarg)
   end.

%%
%% i(Property, Tab) -> {ok, Meta} | {error, Reason}
%%
%% return meta data for given table
i(Prop, Ns) ->
   case lists:keyfind(Prop, 1, i(Ns)) of
      false    -> throw(badarg);
      {_, Val} -> Val
   end.


%%-----------------------------------------------------------------------------
%%
%% put/get
%%
%%-----------------------------------------------------------------------------

%%
%% put(Ns, Key, Val) -> ok | {error, Reason}
%%
put(Ns, Key, Val) ->
   put(Ns, Key, Val, []).

put(Ns, Key, Val, Opts) ->
   case ets:lookup(pts, Ns) of
      [] -> 
         throw({nonamespace, Ns});
      [#pts{readonly=true}] -> 
         throw({readonly, Ns});
      [#pts{supervisor=undefined}] -> 
         throw({readonly, Ns});
      [#pts{keylen=KLen}=P] -> 
         do_put(key_to_uid(Key, KLen), Key, Val, P, Opts)
   end.   

do_put(Uid, Key, Val, #pts{ns=Ns}=P, Opts) ->
   case pns:lock(Ns, Uid) of
      true   ->
         create(Uid, Key, Val, P, Opts);
      {locked, _} ->
         timer:sleep(100), 
         update(Uid, Key, Val, P, Opts);
      {active, Pid}    -> 
         update(Uid, Key, Val, P, Opts)
   end.

create(Uid, Key, Val, #pts{ns=Ns, supervisor=Sup}, Opts) ->
   Pid = create_process(Sup, Ns, Uid),
   case proplists:is_defined(async, Opts) of
      true  ->
         erlang:send(Pid, {put, Key, Val}),
         ok;
      false ->
         Tx = erlang:monitor(process, Pid),
         erlang:send(Pid, {put, {self(), Tx}, Key, Val}),
         wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT))
   end.

update(_Uid, Key, _Val, #pts{immutable=true}, _Opts) ->
   % process exists and cannot be changed
   throw({already_exists, Key});

update(Uid, Key, Val, #pts{ns=Ns, supervisor=Sup}=P, Opts) ->
   case pns:whereis(Ns, Uid) of
      undefined -> 
         do_put(Uid, Key, Val, P, Opts);
      Pid       ->
         case proplists:is_defined(async, Opts) of
            true  ->
               erlang:send(Pid, {put, Key, Val}),
               ok;
            false ->
               Tx = erlang:monitor(process, Pid),
               erlang:send(Pid, {put, {self(), Tx}, Key, Val}),
               wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT))
         end
   end.

%%
%% get(Ns, Key) - {ok, Val} | {error, Error}
get(Ns, Key) ->
   get(Ns, Key, []).

get(Ns, Key, Opts) ->
   case ets:lookup(pts, Ns) of
      []         -> throw({nonamespace, Ns});
      [#pts{keylen=KLen}=P] -> do_get(key_to_uid(Key, KLen), Key, P, Opts)
   end.

do_get(Uid, Key, #pts{ns=Ns}=P, Opts) ->
   case pns:whereis(Ns, Uid) of
      undefined ->
         get_through(Uid, Key, P, Opts);
      Pid       ->
         Tx = erlang:monitor(process, Pid),
         erlang:send(Pid, {get, {self(), Tx}, Key}),
         wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT))
   end.

get_through(_Uid, Key, #pts{rthrough=false}, _Opts) ->
   throw({not_found, Key});

get_through(Uid, Key, #pts{ns=Ns, supervisor=Sup}=P, Opts) ->
   case pns:lock(Ns, Uid) of
      true   ->
         Pid = create_process(Sup, Ns, Uid),
         Tx = erlang:monitor(process, Pid),
         erlang:send(Pid, {get, {self(), Tx}, Key}),
         wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT));
      {locked, _} ->
         timer:sleep(100), 
         do_get(Uid, Key, P, Opts);
      {active, Pid}    -> 
         Tx = erlang:monitor(process, Pid),
         erlang:send(Pid, {get, {self(), Tx}, Key}),
         wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT))
   end.

%%
%%
remove(Ns, Key) ->
   remove(Ns, Key, []).

remove(Ns, Key, Opts) ->
   case ets:lookup(pts, Ns) of
      [] -> throw({nonamespace, Ns});
      [#pts{readonly=true}]     -> throw({readonly, Ns});
      [#pts{supervisor=undefined}] -> throw({readonly, Ns});
      [#pts{keylen=KLen}=P] -> do_remove(key_to_uid(Key, KLen), Key, P, Opts)
   end.
      
do_remove(Uid, Key, #pts{ns=Ns}=P, Opts) ->
   case proplists:is_defined(async, Opts) of
      true  -> cast_remove(pns:whereis(Ns, Uid), Key, P, Opts);
      false -> call_remove(pns:whereis(Ns, Uid), Key, P, Opts)
   end.

call_remove(undefined, _Key, _P, _Opts) ->
   ok;

call_remove(Pid, Key, #pts{}, Opts) ->
   Tx = erlang:monitor(process, Pid),
   erlang:send(Pid, {remove, {self(), Tx}, Key}),
   wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT)).

cast_remove(undefined, _Key, _P, _Opts) ->
   ok;

cast_remove(Pid, Key, #pts{}, _Opts) ->
   erlang:send(Pid, {remove, Key}).

%%
%% map(Tab, Fun) -> List
%%
map(Ns, Fun) ->
   map(Ns, Fun, []).

map(Ns, Fun, Opts) ->   
   case ets:lookup(pts, Ns) of
      []   -> throw({nonamespace, Ns});
      [#pts{}=P] -> do_map(P, Fun, Opts)
   end.  

do_map(#pts{ns=Ns}, Fun, Opts) ->
   Tout = proplists:get_value(timeout, Opts, ?TIMEOUT),
   pns:map(Ns, 
      fun({Key, Pid}) ->
         Getter = fun() ->
            try
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {get, {self(), Ref}, Key}),
               wait_for_reply(Ref, Tout)
            catch _:_ ->
               undefined
            end
         end,
         Fun({Key, Getter})
      end
   ).

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Ns, Acc, Fun) ->
   fold(Ns, Acc, Fun, []).

fold(Ns, Acc, Fun, Opts) ->   
   case ets:lookup(pts, Ns) of
      []   -> throw({nonamespace, Ns});
      [#pts{}=P] -> do_fold(P, Acc, Fun, Opts)
   end.   

do_fold(#pts{ns=Ns}, Acc, Fun, Opts) ->
   Tout = proplists:get_value(timeout, Opts, ?TIMEOUT),
   pns:fold(Ns, Acc, 
      fun({Key, Pid}, A) ->
         Getter = fun() ->
            try
               Ref = erlang:monitor(process, Pid),
               erlang:send(Pid, {get, {self(), Ref}, Key}),
               wait_for_reply(Ref, Tout)
            catch _:_ ->
               undefined
            end
         end,
         Fun({Key, Getter}, A)
      end
   ).   

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
create_process(Sup, Ns, Uid) ->
   case supervisor:start_child(Sup, [Ns, Uid]) of
      {ok, Pid}       -> 
         Pid;
      {error, {Reason, Stack}} when is_list(Stack) ->
         pns:unlock(Ns, Uid),
         throw(Reason);
      {error, Reason} ->
         pns:unlock(Ns, Uid),
         throw(Reason)
   end.        

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

