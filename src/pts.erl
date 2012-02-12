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
   put/3,
   put/2,
   has/2,
   get/2,
   remove/2,
   map/2,
   fold/3,
   % process interface 
   attach/2,
   detach/2
]).


%%
%% internal record, pts-table metadata
-record(pts, {
   id,       % table id
   ns,       % reference to namespace
   keypos,   % position of key element within tuple (default 1)
   async,    % asynchronous I/O
   readonly, % write operations are disabled
   supervise,% processes supervised
   timeout,  % process timeout operation
   factory   % factory function (if factory is not defined 
}).

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
         ets:insert(pts_table, #pts{
            id       = Tab,
            ns       = pts_ns:new(Tab, Opts),
            keypos   = proplists:get_value(keypos, Opts, 1),
            async    = proplists:is_defined(async, Opts),
            readonly = proplists:is_defined(readonly, Opts),
            supervise= proplists:is_defined(supervise, Opts),
            timeout  = proplists:get_value(timeout, Opts, 5000),
            factory  = proplists:get_value(factory, Opts)
         }),
         ok;
      _  -> 
         throw(badarg)
   end.

%%
%% delete(Tab) -> ok
%%
drop(Tab) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> 
         pts_ns:drop(T#pts.ns),
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

%%
%% i(Tab, Property) -> {ok, Meta} | {error, Reason}
%%
%% return meta datafor given table
i(#pts{ns = Ns}, ns) ->
   Ns;
i(#pts{}, _) ->
   {error, not_supported};
i(Tab, Prop) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> i(T, Prop);
      _   -> {error, no_table}
   end.


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
put(#pts{readonly = true}, _Key, _Val) ->
   {error, readonly};
put(#pts{factory = undefined}, _Key, _Val) ->
   {error, readonly};
put(#pts{id = Id, ns = Ns, factory = F} = Tab, Key, Val) ->
   % resolve id of managing process
   Pid = case pts_ns:whereis(Ns, Key) of
      undefined ->
         {ok, Pid0} = F(Id, Key),
         Pid0;
      Pid0 ->
         Pid0
   end,
   prot_put(Tab, Pid, Key, Val);   

put(Tab, Key, Val) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:put(T, Key, Val);
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
               pts:put(T, erlang:element(Tab#pts.keypos, Obj), Obj)
            end,
            Object
         ),
         ok;
      _   -> 
         {error, no_table}
   end;

put(Tab, Object) when is_tuple(Object)->
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:put(T, erlang:element(Tab#pts.keypos, Object), Object);
      _   -> {error, no_table}
   end.
   
   
%%
%% has(Tab, Key) -> bool()
%%
%% Check if the key exists in the table
%%
has(#pts{ns = Ns}, Key) ->
   case pts_ns:whereis(Ns, Key) of
      undefined -> false;
      _         -> true
   end;
   
has(Tab, Key) when is_atom(Tab) ->
   case pts_ns:whereis(Tab, Key) of
      undefined -> false;
      _         -> true
   end;
   
has(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:has(T, Key);
      _   -> {error, no_table}
   end.


%%
%% get(Tab, Key) -> {ok, Val} | {error, Reason}
%%
%% return a value assotiated with key
%%
get(#pts{ns = Ns} = Tab, Key) ->
   case pts_ns:whereis(Ns, Key) of
      undefined -> {error, not_found};
      Pid       -> prot_get(Tab, Pid, Key)
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
remove(#pts{readonly = true}, _Key) ->
   {error, readonly};
remove(#pts{factory = undefined}, _Key) ->
   {error, readonly};
remove(#pts{ns = Ns} = Tab, Key) ->
   case pts_ns:whereis(Ns, Key) of
      undefined -> ok;
      Pid       -> prot_remove(Tab, Pid, Key)
   end;
   
remove(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> remove(T, Key);
      _   -> {error, no_table}
   end.
   
%%
%% map(Tab, Fun) -> List
%%
map(#pts{ns = Ns} = Tab, Fun) ->
   pts_ns:map(Ns, 
      fun({Key, Pid}) ->
         case prot_get(Tab, Pid, Key) of
            {ok, Val} -> Fun({Key, Val});
            _         -> Fun({Key, undefined})
         end
      end
   );
   
map(Tab, Fun) ->   
   case ets:lookup(pts_table, Tab) of
      [T] -> map(T, Fun);
      _   -> {error, no_table}
   end.  

%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(#pts{ns = Ns} = Tab, Acc, Fun) ->
   pts_ns:fold(Ns, Acc,
      fun({Key, Pid}, A) ->
         case prot_get(Tab, Pid, Key) of
            {ok, Val} -> Fun({Key, Val}, A);
            _         -> Fun({Key, undefined}, A)
         end
      end
   );
   
fold(Tab, Acc, Fun) ->   
   case ets:lookup(pts_table, Tab) of
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
attach(#pts{ns = Ns}, Key) ->
   pts_ns:register(Ns, Key, self());

attach(Tab, Key) when is_atom(Tab) ->
   pts_ns:register(Tab, Key, self());
   
attach(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:attach(T, Key);
      _   -> {error, no_table}
   end.
   
%%
%% attach(Tab, Key) -> ok
%%
detach(#pts{ns = Ns}, Key) ->
   pts_ns:unregister(Ns, Key);

detach(Tab, Key) when is_atom(Tab) ->
   pts_ns:unregister(Tab, Key);
   
detach(Tab, Key) ->
   case ets:lookup(pts_table, Tab) of
      [T] -> pts:detach(T, Key);
      _   -> {error, no_table}
   end.
   
   
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% 
prot_put(#pts{async = true}, Pid, Key, Val) ->
   erlang:send(Pid, {pts_req, self(), {put, Key, Val}}),
   ok;

prot_put(#pts{timeout = T}, Pid, Key, Val) ->
   prot_sync_call(Pid, {pts_req, self(), {put, Key, Val}}, T).

%%
%%
prot_get(#pts{timeout = T}, Pid, Key) ->
   prot_sync_call(Pid, {pts_req, self(), {get, Key}}, T).  
   
%%
%%
prot_remove(#pts{async = true}, Pid, Key) ->
   erlang:send(Pid, {pts_req, self(), {remove, Key}}),
   ok;

prot_remove(#pts{timeout = T}, Pid, Key) ->
   prot_sync_call(Pid, {pts_req, self(), {remove, Key}}, T).

%%
%% executes synchronous protocol operation
prot_sync_call(Pid, Req, Timeout) ->
   try erlang:monitor(process, Pid) of
      Ref ->
         catch erlang:send(Pid, Req, [noconnect]),
         receive
            {'DOWN', Ref, _, _, Reason} -> 
               {error, Reason};
            {pts_rsp, Response}  -> 
               erlang:demonitor(Ref, [flush]),
               Response
         after Timeout ->
            erlang:demonitor(Ref),
            % consume possible down msg
            receive
			   {'DOWN', Ref, _, _, _} -> true
		      after 0 -> true
		      end,
		      {error, timeout}
         end
   catch
      error:_ -> {error, system}
   end.   
