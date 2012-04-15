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

%%
%% Management
-export([new/1, new/2, drop/1, i/0, i/1, i/2]).
%%
%% CRUD
-export([create/2, read/2, update/2, delete/2]).
%%
%% Hashtable
-export([put/2, get/2, remove/2]).
%%
%% Map/Fold
-export([map/2, fold/3]).
%%
%% Process interface
-export([attach/1, detach/1, notify/2]).

%%
%% internal record, pts-table metadata
-record(pts, {
   ns,       % table id / namesppace
   keypos,   % position of key element within tuple (default 1)
   async,    % asynchronous I/O
   readonly, % write operations are disabled
   supervise,% processes supervised
   timeout,  % process timeout operation
   factory,  % factory function  
   entry     % entry specific settings
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
%%    Opt  = {keypos,  integer()} | async | readonly | supervise |
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
            async    = proplists:is_defined(async, Opts),
            readonly = proplists:is_defined(readonly, Opts),
            %supervise= proplists:is_defined(supervise, Opts),
            timeout  = proplists:get_value(timeout, Opts, 5000),
            factory  = proplists:get_value(factory, Opts),
            entry    = proplists:get_value(entry, Opts, [])
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
%% return meta datafor given table
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
      [#pts{factory  = F, keypos = Pos, timeout = T1, entry = E}] ->
         Key = erlang:element(Pos, Val),
         case pns:whereis({Ns, Key}) of
            undefined ->
               %% Note
               {ok, Pid} = pts_prot:req(F, {create, E, {Ns, Key}}, T1),
               pts_prot:req(Pid, {put, nil, Val}, T1);
            _ ->
               {error, duplicate}
         end
   end.   


read(Ns, Key) ->
   case ets:lookup(pts_table, Ns) of
      [] ->
         {error, no_namespace};
      [#pts{timeout = T1}] ->
         case pns:whereis({Ns, Key}) of
            undefined -> {error, not_found};
            Pid       -> pts_prot:req(Pid, {get, {Ns,Key}}, T1)
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
      [#pts{keypos = Pos, timeout = T1}] ->
         Key = erlang:element(Pos, Val),
         case pns:whereis({Ns, Key}) of
            undefined -> {error, {not_found, {Ns, Key}}};
            Pid       -> pts_prot:req(Pid, {put, nil, Val}, T1)
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
      [#pts{timeout = T1}] ->
         case pns:whereis({Ns, Key}) of
            undefined -> ok;
            Pid       -> pts_prot:req(Pid, {remove, nil, {Ns,Key}}, T1)
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
      [#pts{factory  = F, keypos = Pos, timeout = T1, entry = E}] ->
         Key = erlang:element(Pos, Val),
         case pns:whereis({Ns, Key}) of
            undefined ->
               {ok, Pid} = pts_prot:req(F, {create, E, {Ns, Key}}, T1),
               pts_prot:req(Pid, {put, nil, Val}, T1);
            Pid ->
               pts_prot:req(Pid, {put, val,  Val}, T1)
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
      [#pts{timeout = T1}] ->
         case pns:whereis({Ns, Key}) of
            undefined -> ok;
            Pid       -> pts_prot:req(Pid, {remove, val, {Ns,Key}}, T1)
         end
   end.
   
%%
%% map(Tab, Fun) -> List
%%
map(Ns, Fun) ->   
   case ets:lookup(pts_table, Ns) of
      []   -> {error, no_namespace};
      [#pts{timeout = T1}] -> 
         pns:map(Ns, 
            fun({{_, Key}, Pid}) ->
               Get = fun() -> pts_prot:req(Pid, {get, {Ns, Key}}, T1) end,
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
      [#pts{timeout = T1}] -> 
         pns:fold(Ns, Acc, 
            fun({{_, Key}, Pid}, A) ->
               Get = fun() -> pts_prot:req(Pid, {get, {Ns, Key}}, T1) end,
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
   
%%
%% 
%% notify transaction
notify({Pid, _} = Tx, Rsp) ->
   erlang:send(Pid, {pts, Tx, Rsp}).   
   
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

         
