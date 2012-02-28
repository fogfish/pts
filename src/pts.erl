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
   % table management interface
   new/1,
   new/2,
   drop/1,
   i/0,
   i/1,
   i/2,
   % data management interface
   put/2,
   has/1,
   get/1,
   remove/1,
   map/2,
   fold/3,
   % process interface 
   attach/1,
   detach/1,
   notify/2
]).


%%
%% internal record, pts-table metadata
-record(pts, {
   ns,       % table id / namesppace
   keypos,   % position of key element within tuple (default 1)
   async,    % asynchronous I/O
   readonly, % write operations are disabled
   supervise,% processes supervised
   timeout,  % process timeout operation
   factory   % factory function  
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
%% data management
%%
%%-----------------------------------------------------------------------------

%%
%% put(Key, Val) -> ok | {error, Reason} 
%%
%% Inserts the value object into table and assotiates its with the key. 
%% If tables is set or ordered_set and the key of inserted value matches
%% any existed key, the old assotiation is replace.
%%
put({Ns, _} = Key, Val) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> do_put(T, Key, Val);
      _   -> {error, no_namespace}
   end.
   
%%
%% has(Key) -> bool()
%%
%% Check if the key exists in the table
%%
has({_,_} = Key) ->
   case pts_ns:whereis(Key) of
      undefined -> false;
      _         -> true
   end.

%%
%% get(Key) -> {ok, Val} | {error, Reason}
%%
%% return a value assotiated with key
%%
get({Ns, _} = Key) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> 
         case pts_ns:whereis(Key) of
            undefined -> {error, not_found};
            Pid       -> prot_get(T, Pid, Key)
         end;
      _   -> {error, no_table}
   end.
   
   
%%
%% remove(Tab, Key) -> ok | {error, _}
%%
%% removes the value 
%%
remove({Ns, _} = Key) ->
   case ets:lookup(pts_table, Ns) of
      [T] -> do_remove(T, Key);
      _   -> {error, no_table}
   end.
   
%%
%% map(Tab, Fun) -> List
%%
map(#pts{ns = Ns} = Tab, Fun) ->
   pts_ns:map(Ns, 
      fun({Key, Pid}) ->
         Get = fun() -> prot_get(Tab, Pid, Key) end,
         Fun({Key, Get})
      end
   );
   
map(Ns, Fun) ->   
   case ets:lookup(pts_table, Ns) of
      [T] -> map(T, Fun);
      _   -> {error, no_table}
   end.  

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(#pts{ns = Ns} = Tab, Acc, Fun) ->
   pts_ns:fold(Ns, Acc,
      fun({Key, Pid}, A) ->
         Get = fun() -> prot_get(Tab, Pid, Key) end,
         Fun({Key, Get}, A)
      end
   );
   
fold(Ns, Acc, Fun) ->   
   case ets:lookup(pts_table, Ns) of
      [T] -> fold(T, Acc, Fun);
      _   -> {error, no_table}
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
      [T] -> 
         pts_ns:register(Key, self());
         %case T#pts.supervise of
         %   false -> ok;
         %   true  -> pts_pid_sup:supervise(self())
         %end;
      _   -> {error, no_table}
   end.
   
%%
%% attach(Tab, Key) -> ok
%%
detach({Ns, _} = Key) ->
   case ets:lookup(pts_table, Ns) of
      [_] -> pts_ns:unregister(Key);
      _   -> {error, no_table}
   end.
   
%%
%% 
%% notify transaction
notify({Pid, Ref}, Rsp) ->
   erlang:send(Pid, {pts, Ref, Rsp}).   
   
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

do_put(#pts{readonly = true}, _Key, _Val) ->
   {error, readonly};
do_put(#pts{factory = undefined}, _Key, _Val) ->
   {error, readonly};
do_put(#pts{factory = F} = Tab, Key, Val) ->
   % resolve id of managing process
   Pid = case pts_ns:whereis(Key) of
      undefined ->
         {ok, Pid0} = F({create, Key}),
         Pid0;
      Pid0 ->
         Pid0
   end,
   prot_put(Tab, Pid, Key, Val).  

do_remove(#pts{readonly = true}, _Key) ->
   {error, readonly};
do_remove(#pts{factory = undefined}, _Key) ->
   {error, readonly};
do_remove(#pts{} = Tab, Key) ->
   case pts_ns:whereis(Key) of
      undefined -> ok;
      Pid       -> prot_remove(Tab, Pid, Key)
   end.

%%
%% 
prot_put(#pts{async = true}, Pid, Key, Val) ->
   erlang:send(Pid, {pts, self(), {put, Key, Val}}),
   ok;

prot_put(#pts{timeout = T}, Pid, Key, Val) ->
   prot_sync_call(Pid, {put, Key, Val}, T).

%%
%%
prot_get(#pts{timeout = T}, Pid, Key) ->
   prot_sync_call(Pid, {get, Key}, T).  
   
%%
%%
prot_remove(#pts{async = true}, Pid, Key) ->
   erlang:send(Pid, {pts, self(), {remove, Key}}),
   ok;

prot_remove(#pts{timeout = T}, Pid, Key) ->
   prot_sync_call(Pid, {remove, Key}, T).

%%
%% executes synchronous protocol operation
prot_sync_call(Pid, Req, Timeout) ->
   try erlang:monitor(process, Pid) of
      Ref ->
         catch erlang:send(Pid, {pts, {self(), Ref}, Req}, [noconnect]),
         receive
            {'DOWN', Ref, _, _, Reason} -> 
               {error, Reason};
            {pts, Ref, Response}  -> 
               erlang:demonitor(Ref, [flush]),
               Response
         after Timeout ->
            erlang:demonitor(Ref, [flush]),
		      {error, timeout}
         end
   catch
      error:_ -> {error, system}
   end.   
 
         
