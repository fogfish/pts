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

%%
%% Management
-export([new/1, new/2, drop/1, i/0, i/1, i/2]).
%%
%% CRUD
-export([spawn/2, create/2, read/2, update/2, delete/2]).
%%
%% Hashtable
-export([put/2, get/2, remove/2]).
%%
%% map/fold
-export([map/2, fold/3]).
%%
%% OTP process interface
-export([attach/1, detach/1]).

%%
%% internal record, pts metadata
-record(pts, {
   ns        :: atom(),       % table id / namesppace
   keypos    :: integer(),    % position of key element within tuple (default 1)
   keylen    :: integer() | inf, % length of key 
   async     :: boolean(),    % asynchronous I/O (write are not blocked)
   readonly  :: boolean(),    % write operations are disabled
   timeout   :: integer(),    % process timeout operation
   iftype    :: server | fsm, % interface type
   rthrough  :: boolean(),    % read-through
   factory   :: function()    % factory function  
}).

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
   case ets:lookup(pts_table, Ns) of
      [] -> 
         ets:insert(pts_table, #pts{
            ns       = Ns,
            keypos   = proplists:get_value(keypos, Opts, 1),
            keylen   = proplists:get_value(keylen, Opts, inf),
            async    = proplists:is_defined(async, Opts),
            readonly = proplists:is_defined(readonly, Opts),
            timeout  = proplists:get_value(timeout, Opts, 5000),
            iftype   = proplists:get_value(iftype, Opts, server),
            rthrough = proplists:is_defined(rthrough, Opts),
            factory  = proplists:get_value(factory, Opts)
         }),
         ok;
      _  -> 
         throw(badarg)
   end.

%%
%% delete(Ns) -> ok
%%
drop(Ns) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> 
         ets:delete(pts_table, T#pts.ns),
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
%% return meta data for given table
i(Ns) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> {ok, T};
      _   -> {error, no_table}
   end.

%%
%% i(Tab, Property) -> {ok, Meta} | {error, Reason}
%%
%% return meta data for given table
i(#pts{ns = Val}, ns) ->
   Val;
i(#pts{factory = Val}, factory) ->
   Val;
i(#pts{}, _) ->
   throw(badarg);
i(Ns, Prop) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> i(T, Prop);
      _   -> {error, no_table}
   end.


%%-----------------------------------------------------------------------------
%%
%% CRUD
%%
%%-----------------------------------------------------------------------------

%%
%% spawn(Ns, Key) -> {ok, Pid} | {error, Reason}
spawn(Ns, Key) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{factory  = F, keylen = Len}] ->
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined ->
               F([{self(), {Ns, key(Key, Len)}}]);
            _ ->
               {error, duplicate}
         end
   end.

%%
%% create(Ns, Val) -> ok | {error, Reason}
%%
create(Ns, Val) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{factory  = F, timeout = T, keylen = Len} = S] ->
         Key = erlang:element(S#pts.keypos, Val),
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined ->
               {ok, Pid} = F([{self(), {Ns, key(Key, Len)}}]),
               tx({S#pts.iftype, S#pts.async}, Pid, {put, Val}, T);
            _ ->
               {error, duplicate}
         end
   end.   


read(Ns, Key) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{rthrough = true, factory  = F, timeout = T, keylen = Len} = S] ->
         {ok, Pid} = case pns:whereis({Ns, key(Key, Len)}) of
            undefined -> F([{self(), {Ns, key(Key, Len)}}]);
            Kpid       -> {ok, Kpid}
         end,
         tx({S#pts.iftype, false}, Pid, {get, {Ns,Key}}, T);
      [#pts{timeout = T, keylen = Len} = S] ->
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined -> {error, not_found};
            Pid       -> tx({S#pts.iftype, false}, Pid, {get, {Ns,Key}}, T)
         end
   end.
   
   
update(Ns, Val) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{timeout = T, keylen = Len} = S] ->
         Key = erlang:element(S#pts.keypos, Val),
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined -> {error, not_found};
            Pid       -> tx({S#pts.iftype, S#pts.async}, Pid, {put, Val}, T)
         end
   end.
   
   
delete(Ns, Key) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{timeout = T, keylen = Len} = S] ->
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined -> ok;
            Pid       -> tx({S#pts.iftype, S#pts.async}, Pid, {remove, {Ns,Key}}, T)
         end
   end.
   
   
%%
%% put(Key, Val) -> ok | {error, Reason} 
%%
%% Inserts the value object into table and assotiates its with the key. 
%% If tables is set or ordered_set and the key of inserted value matches
%% any existed key, the old assotiation is replace.
%%
put(Ns, Val) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{factory  = F, timeout = T, keylen = Len} = S] ->
         Key = erlang:element(S#pts.keypos, Val),
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined ->
               {ok, Pid} = F([{self(), {Ns, key(Key, Len)}}]),
               tx({S#pts.iftype, S#pts.async}, Pid, {put, Val}, T);
            Pid ->
               tx({S#pts.iftype, S#pts.async}, Pid, {put, Val}, T)
         end
   end.   
   

%%
%% get(Key) -> {ok, Val} | {error, Reason}
%%
%% return a value assotiated with key
%%
get(Ns, Key) ->
   read(Ns, Key).
   
   
%%
%% remove(Tab, Key) -> {ok, Val} | {error, _}
%%
%% removes the value 
%%
remove(Ns, Key) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{readonly = true}] ->
         {error, readonly};
      [#pts{factory  = undefined}] ->
         {error, readonly};
      [#pts{timeout = T, keylen = Len} = S] ->
         case pns:whereis({Ns, key(Key, Len)}) of
            undefined -> ok;
            Pid       -> tx({S#pts.iftype, S#pts.async}, Pid, {remove, {Ns,Key}}, T)
         end
   end.
   
%%
%% map(Tab, Fun) -> List
%%
map(Ns, Fun) ->   
   case ets:lookup(pts_table, Ns) of
      []   -> {error, no_namespace};
      [#pts{timeout = T} = S] -> 
         pns:map(Ns, 
            fun({{_, Key}, Pid}) ->
               Get = fun() -> tx({S#pts.iftype, false}, Pid, {get, {Ns, Key}}, T) end,
               Fun({Key, Get})
            end
         )
   end.  

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Ns, Acc, Fun) ->   
   case ets:lookup(pts_table, Ns) of
      []   -> {error, no_namespace};
      [#pts{timeout = T} = S] -> 
         pns:fold(Ns, Acc, 
            fun({{_, Key}, Pid}, A) ->
               Get = fun() -> tx({S#pts.iftype, false}, Pid, {get, {Ns, Key}}, T) end,
               Fun({Key, Get}, A)
            end
         )
   end.   
   

%%-----------------------------------------------------------------------------
%%
%% process interfaces
%%
%%-----------------------------------------------------------------------------

%%
%% attach(Tab, Key) -> ok
%%
attach({Ns, _} = Key) ->
   case ets:lookup(pts_table, Ns) of
      [_] -> 
         pns:register(Key, self());
         %case T#pts.supervise of
         %   false -> ok;
         %   true  -> pts_pid_sup:supervise(self())
         %end;
      _   -> {error, no_namespace}
   end.
   
%%
%% attach(Tab, Key) -> ok
%%
detach({Ns, _} = Key) ->
   case ets:lookup(pts_table, Ns) of
      [_] -> pns:unregister(Key);
      _   -> {error, no_table}
   end.
      
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

tx({server, false}, Pid, Tx, T) ->
   gen_server:call(Pid, Tx, T);

tx({server, true}, Pid, Tx, _) ->
   gen_server:cast(Pid, Tx);

tx({fsm, false}, Pid, Tx, T) ->
   gen_fsm:sync_send_event(Pid, Tx, T);

tx({fsm, true}, Pid, Tx, _) ->
   gen_fsm:send_event(Pid, Tx).   
     
%% transforms key         
key(Key, inf) ->
   Key;
key(Key,   1) when is_tuple(Key) ->   
   element(1, Key);
key(Key, Len) when is_tuple(Key) ->
   list_to_tuple(
      lists:sublist(
         tuple_to_list(Key),
         Len
      )
   );
key(Key, _) ->
   Key.

