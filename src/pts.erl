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
%% Management
-export([new/1, new/2, drop/1, i/1, i/2]).
%% Hashtable
-export([put/3, put/4, get/2, get/3, remove/2, remove/3, call/3]).
%% map/fold
-export([map/2, map/3, fold/3, fold/4]).



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
   {ok, _} = supervisor:start_child(pts_sup, {
      Ns,
      {pts_ns, start_link, [init(Opts, #pts{ns=Ns, owner=self()})]},
      transient, 5000, worker, dynamic
   }),
   ok.

init([{keylen, X} | T], P) ->
   init(T, P#pts{keylen=X}); 

init([{supervisor, Mod} | T], P) ->
   init(T, P#pts{supervisor=Mod});

init([{supervisor, Mod, Opts} | T], P) ->
   init(T, P#pts{supervisor={Mod, Opts}});

init([readonly | T], P) ->
   init(T, P#pts{readonly=true});

init([immutable | T], P) ->
   init(T, P#pts{immutable=true});

init(['read-through' | T], P) ->
   init(T, P#pts{rthrough=true});

init([{attempt, X} | T], P) ->
   init(T, P#pts{attempt=X});

init([], P) ->
   P.

%%
%% delete(Ns) -> ok
%%
drop(Ns) ->
   Pid = erlang:element(2,
      lists:keyfind(Ns, 1, supervisor:which_children(pts_sup))
   ),
   erlang:send(Pid, drop).

   
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

do_put(_Uid, Key, _Val, #pts{ns=Ns, attempt=0}, _Opts) ->
   throw({nolock, {Ns, Key}});

do_put(Uid, Key, Val, #pts{ns=Ns, attempt=A}=P, Opts) ->
   case pns:lock(Ns, Uid) of
      true   ->
         create(Uid, Key, Val, P, Opts);
      {locked, _} ->
         timer:sleep(100), 
         update(Uid, Key, Val, P#pts{attempt=A - 1}, Opts);
      {active, _Pid}    -> 
         update(Uid, Key, Val, P#pts{attempt=A - 1}, Opts)
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

update(Uid, Key, Val, #pts{ns=Ns}=P, Opts) ->
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

get_through(_Uid, Key, #pts{ns=Ns, attempt=0}, _Opts) ->
   throw({nolock, {Ns, Key}});

get_through(Uid, Key, #pts{ns=Ns, supervisor=Sup, attempt=A}=P, Opts) ->
   case pns:lock(Ns, Uid) of
      true   ->
         Pid = create_process(Sup, Ns, Uid),
         Tx = erlang:monitor(process, Pid),
         erlang:send(Pid, {get, {self(), Tx}, Key}),
         wait_for_reply(Tx, proplists:get_value(timeout, Opts, ?TIMEOUT));
      % key is locked
      {locked, _} ->
         timer:sleep(100), 
         do_get(Uid, Key, P#pts{attempt=A - 1}, Opts);
      % key process is exists
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
%% 
call(Ns, Key, Msg) ->
   case ets:lookup(pts, Ns) of
      [] -> throw({nonamespace, Ns});
      [#pts{keylen=KLen}=P] -> do_call(key_to_uid(Key, KLen), Key, P, Msg)
   end.

do_call(Uid, Key, #pts{ns=Ns}=P, Msg) ->
   case pns:whereis(Ns, Uid) of
      undefined ->
         call_through(Uid, Key, P, Msg);
      Pid       ->
         gen_server:call(Pid, Msg)
   end.

call_through(_Uid, Key, #pts{rthrough=false}, _Msg) ->
   throw({not_found, Key});

call_through(_Uid, Key, #pts{ns=Ns, attempt=0}, _Msg) ->
   throw({nolock, {Ns, Key}});

call_through(Uid, Key, #pts{ns=Ns, supervisor=Sup, attempt=A}=P, Msg) ->
   case pns:lock(Ns, Uid) of
      true   ->
         Pid = create_process(Sup, Ns, Uid),
         gen_server:call(Pid, Msg);
      % key is locked
      {locked, _} ->
         timer:sleep(100), 
         do_call(Uid, Key, P#pts{attempt=A - 1}, Msg);
      % key process is exists
      {active, Pid}    -> 
         gen_server:call(Pid, Msg)
   end.


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

