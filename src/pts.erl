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
%% The library provides hashtable-like interface to manipulate data distributed 
%% across Erlang processes.
%%

-export([
   % data management interface
   put/3,
   put/2,
   has/2,
   get/2,
   remove/2,
   map/2,
   fold/3,
   % table management interface
   new/1,
   new/2,
   delete/1,
   i/0,
   i/1,
   % process registry
   register/1,
   unregister/1,
   whereis/1,
   registered/0,
   % process interface 
   register/2,
   unregister/2
]).


%%
%% internal record, pts-table metadata
-record(pts, {
   id,      % table id
   ref,     % reference to keyspace
   type,    % type of the table
   keypos,  % 
   io,      % I/O operation (sync, async)
   timeout, % process timeout operation
   hash,    % hash function
   factory  % factory function
}).

-include_lib("stdlib/include/qlc.hrl").

%%-----------------------------------------------------------------------------
%%
%% data management
%%
%%-----------------------------------------------------------------------------

%%
%% put(Tab, Key, Val) -> ok | {error, Reason} 
%%
%% Inserts the value object into table and assotiates its with the key. 
%% If tables is set or ordered_set and the key of inserted value matches
%% any existed key, the old assotiation is replace.
%%
put(Tab, Key, Val) when is_record(Tab, pts) ->
   % resolve id of managing process
   {ok, Pid} = case Tab#pts.type of
      bag -> 
         key_spawn(Tab, Key);
      _   ->
         case key_to_pid(Tab, Key) of
            {error, not_found} -> key_spawn(Tab, Key);
            {ok, Kpid}         -> {ok, Kpid}
         end
   end,
   % store
   case prot_put(Tab, Pid, Key, Val) of
      ok -> pid_to_key(Tab, Key, Pid);
      R  -> R
   end;   

put(Tab, Key, Val) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> put(T, Key, Val);
      _   -> {error, no_table}
   end.

%%
%% put(Tab, Object) -> ok | {error, Reason}
%%
%% Insert the tuple object or list of tuples and assotiates with key
%%
put(Tab, Object) when is_list(Object)->
   case ets:lookup(pts_table, Tab) of
      [T] -> 
         lists:foreach(
            fun(Obj) ->
               put(T, erlang:element(Tab#pts.keypos, Obj), Obj)
            end,
            Object
         ),
         ok;
      _   -> 
         {error, no_table}
   end;

put(Tab, Object) when is_tuple(Object)->
   case ets:lookup(pts_table, Tab) of
      [T] -> put(T, erlang:element(Tab#pts.keypos, Object), Object);
      _   -> {error, no_table}
   end.
   
   
%%
%% has(Tab, Key) -> bool()
%%
%% Check if the key exists in the table
%%
has(Tab, Key) when is_record(Tab, pts) -> 
   case key_to_pid(Tab, Key) of
      {error, not_found} -> false;
      {ok, _Pid}         -> true   
   end;
   
has(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> has(T, Key);
      _   -> {error, no_table}
   end.


%%
%% get(Tab, Key) -> {ok, Val} | {error, Reason}
%%
%% return a value assotiated with key
%%
get(Tab, Key) when is_record(Tab, pts) ->
   case key_to_pid(Tab, Key) of
      {ok, Pid} -> prot_get(Tab, Pid, Key);
      Ret       -> Ret
   end;

get(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> get(T, Key);
      _   -> {error, no_table}
   end.
   
   
%%
%% remove(Tab, Key) -> ok | {error, _}
%%
%% removes the value 
%%
remove(Tab, Key) when is_record(Tab, pts) ->
   case key_to_pid(Tab, Key) of
      {ok, Pid} ->
         case prot_remove(Tab, Pid, Key) of
            ok -> pid_to_key(Tab, Key, undefined);
            R  -> R
         end;
      _        -> 
         ok
   end; 
   
remove(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> remove(T, Key);
      _   -> {error, no_table}
   end.
   
%%
%% map(Tab, Fun) -> List
%%
map(Tab, Fun) when is_record(Tab, pts) ->
   Map = fun({_, Key, Pid}) -> 
      case prot_get(Tab, Pid, Key) of
         {ok, Val} -> Fun({Key, Val});
         _         -> Fun({Key, undefined})
      end
   end,
   Q = qlc:q([ Map(X) || X <- ets:table(Tab#pts.ref)]),
   qlc:e(Q);
   
map(Tab, Fun) ->   
   case ets:lookup(pts_table, Tab) of
      [T] -> map(T, Fun);
      _   -> {error, no_table}
   end.  

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Tab, Acc, Fun) when is_record(Tab, pts) ->
   Fold = fun({_, Key, Pid}, A) ->
      case prot_get(Tab, Pid, Key) of
         {ok, Val} -> Fun({Key, Val}, A);
         _         -> Fun({Key, undefined}, A)
      end
   end,
   Q = qlc:q([ X || X <- ets:table(Tab#pts.ref)]),
   qlc:fold(Fold, Acc, Q);  
   
fold(Tab, Acc, Fun) ->   
   case ets:lookup(pts_table, Tab) of
      [T] -> fold(T, Acc, Fun);
      _   -> {error, no_table}
   end.   
   
%%-----------------------------------------------------------------------------
%%
%% table management
%%
%%-----------------------------------------------------------------------------

%%
%% new(Name, Opts) -> ok
%%    Name = atom()
%%    Opts = [Option]
%%    Opt  = Type | Access | Write | Factory | Hash
%%
%% Creates a new named pts table and return
%%
new(Tab) ->
   new(Tab, []).
   
new(Tab, Opts) ->
   case ets:lookup(pts_table, Tab) of
      [] -> 
         Access = is_opt(private, Opts, public),
         Type   = is_opt(set,     Opts, ordered_set),
         % define a keyspace
         Ref    = ets:new(undefined, 
            [Access, Type, {read_concurrency, true}]
         ),
         ets:insert(pts_table, #pts{
            id      = Tab,
            ref     = Ref,
            type    = Type,
            keypos  = proplists:get_value(keypos, Opts, 1),
            io      = is_opt(async, Opts, sync),
            timeout = proplists:get_value(timeout,Opts, 5000),
            hash    = proplists:get_value(hash, Opts, fun erlang:phash2/1),
            factory = proplists:get_value(factory, Opts)
         }),
         ok;
      _  -> 
         throw(badarg)
   end.

%%
%% delete(Name) -> ok
%%
delete(Tab) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> 
         Fold = fun({_, Key, Pid}, A) ->
            Pid ! {pts_req_remove, self(), Key},
            receive
               {pts_rsp_remove, _} -> A
            after 
               T#pts.timeout       -> A
            end
         end,
         Q = qlc:q([ X || X <- ets:table(T#pts.ref)]),
         qlc:fold(Fold, [], Q),
         ets:delete(T#pts.ref),
         ets:delete(pts_table, T#pts.id),
         ok;
      _   ->
         ok
   end.   
   
%%
%% i() -> [Meta]
%%
%% return metadata of defined tables
i() ->
   ets:tab2list(pts_table).
  
%%
%% i(Tab) -> {ok, Meta} | {error, Reason}
%%
%% return meta datafor given table
i(Tab) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> {ok, T};
      _   -> {error, no_table}
   end.
   
%%-----------------------------------------------------------------------------
%%
%% process registry
%%
%%-----------------------------------------------------------------------------   
%%
%% register(Uid, Pid) -> ok
%%   Uid = term()
%%   Pid = pid()
%%
%% Associates the name Uid with a valid Pid-only. Uid is any term
%% Failure: badarg if Pid is not an existing, if Uid is already in use
%%
register(Uid) ->
   case pts:whereis(Uid) of
      undefined -> ets:insert(pts_reg, {Uid, self()}), ok;
      _         -> throw(badarg)
   end.


%%
%% unredister(Uid) -> ok
%%   Uid = term()
%%
%% Removes the registered Uid associatation with a pid.
%% Failure: badarg if Uid is not a registered name, Uri is assotiated 
%% with invalid pid or if Uri is not complient http://tools.ietf.org/html/rfc3986.
unregister(Uid) ->
   case pts:whereis(Uid) of
      undefined -> throw(badarg);
      _         -> ets:delete(pts_reg, Uid), ok
   end.
   
%%
%% whereis(Uid) -> pid() | undefined
%%    Uid = list() | tuple()
%%
%% Returns the pid or port identifier with the registered with Uid. 
%% Returns undefined if the name is not registered. 
whereis(Uid) ->
   case ets:lookup(pts_reg, Uid) of
      [{Uid, Pid}] ->
         case is_process_alive(Pid) of
            true  -> 
               Pid;
            false -> 
               ets:delete(pts_reg, Uid),
               undefined
         end;
      _            -> 
         undefined
   end.
   
%%
%% registered() -> [Uid]
%%
%% Returns a list of names which have been registered using register/2.
registered() ->   
   lists:foldl(
      fun({Uid, Pid}, Acc) ->
         case is_process_alive(Pid) of
            true  -> [Uid | Acc];
            false -> ets:delete(pts_reg, Uid), Acc
         end
      end,
      [],
      ets:match_object(pts_reg, '_')
   ).

%%-----------------------------------------------------------------------------
%%
%% process interfaces
%%
%%-----------------------------------------------------------------------------

%%
%% register(Tab, Key) -> bool()
%%
register(Tab, Key) when is_record(Tab, pts) ->
   case key_to_pid(Tab, Key) of
      {error, not_found} -> 
         pid_to_key(Tab, Key, self()),
         true;
      {ok, _Pid}         ->
         false
   end;

register(Tab, Key) ->    
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:register(T, Key);
      _   -> {error, no_table}
   end.   

%%
%% unregister(Tab, Key) -> bool()
%%
unregister(Tab, Key) when is_record(Tab, pts) ->
   Self = self(),
   case key_to_pid(Tab, Key) of
      {error, not_found} -> 
         true;
      {ok, Self}         -> 
         pid_to_key(Tab, Key, undefined),
         true;
      {ok, _Pid}         ->
         false
   end; 

unregister(Tab, Key) ->    
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:unregister(T, Key);
      _   -> {error, no_table}
   end.   

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% 
prot_put(Tab, Pid, Key, Val) ->
   Pid ! {pts_req_put, self(), Key, Val},
   case Tab#pts.io of
      async -> 
         ok;
      _     ->
         receive
            {pts_rsp_put, ok} -> ok;
            {pts_rsp_put,  R} -> R
         after 
            Tab#pts.timeout   -> {error, timeout}
         end
   end.

%%
%%
prot_get(Tab, Pid, Key) ->
   Pid ! {pts_req_get, self(), Key},
   receive
      {pts_rsp_get, {ok, Val}} -> {ok, Val};
      {pts_rsp_get,         R} -> R
   after 
      Tab#pts.timeout   -> {error, timeout}
   end.
   
%%
%%
prot_remove(Tab, Pid, Key) ->
   Pid ! {pts_req_remove, self(), Key},
   case Tab#pts.io of
      async ->
         ok;
      _     ->
         receive
            {pts_rsp_remove, ok} -> ok;
            {pts_rsp_remove,  R} -> R
         after 
            Tab#pts.timeout   -> {error, timeout}
         end
   end.
   
   
%%
%% maps key into process
key_to_pid(T, Key) ->
   Hash = T#pts.hash,
   case ets:lookup(T#pts.ref, Hash(Key)) of
      [{_, Key, Pid}] -> 
         case is_process_alive(Pid) of
            true  -> 
               {ok, Pid};
            false -> 
               ets:delete(T#pts.ref, Hash(Key)),
               {error, not_found}
         end;
      _          -> {error, not_found}
   end.

%%
%% assotiates pid with key value
pid_to_key(T, Key, undefined) ->
   Hash = T#pts.hash,
   ets:delete(T#pts.ref, Hash(Key)),
   ok;

pid_to_key(T, Key, Pid) ->
   Hash = T#pts.hash,
   ets:insert(T#pts.ref, {Hash(Key), Key, Pid}),
   ok.
   
%%
%% spawn a new key process
key_spawn(#pts{factory = undefined}, _) ->
   throw(badarg);
key_spawn(#pts{id = Name, factory = Factory}, Key) ->
   Factory(Name, Key).
   
   
is_opt(Opt, Opts, Default) ->
   case proplists:is_defined(Opt, Opts) of 
      true  -> Opt;
      false -> Default
   end.