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
%%     Process registry, assotiates a process Uid with Pid
%%
-module(pts_ns).
-author(dmkolesnikov@gmail.com).
-include_lib("stdlib/include/qlc.hrl").

-export([
   start/0,
   new/1,
   new/2,
   drop/1,
   register/2,
   register/3,
   unregister/1,
   unregister/2,
   whereis/1,
   whereis/2,
   whatis/1,
   whatis/2,
   map/1,
   map/2,
   fold/2,
   fold/3,
   send/2
]).

%%
%% core opts for namespace
-define(CORE_OPTS, [
   ordered_set,
   {read_concurrency, true},
   {write_concurrency, true}
]).

%%
%% default Namespace
-define(DEFAULT_NS, pts_local_ns).

%%
%%
%%
start() ->
   pts_ns:new(?DEFAULT_NS),
   ok.


%%
%% new(Ns, Opts) -> Ref
%%
%% defines a new namespace, throws bagarg is namespace exists
new(Ns) ->
   new(Ns, []).
   
new(Ns, Opts) when is_atom(Ns) ->
   Access = case lists:member(private, Opts) of
      true  -> private;
      false -> public
   end,
   ets:new(Ns, [Access, named_table | ?CORE_OPTS]);
   
new(_Ns, Opts) ->
   Access = case lists:member(private, Opts) of
      true  -> private;
      false -> public
   end,
   ets:new(undefined, [Access | ?CORE_OPTS]).
   
%%
%%
drop(Ns) ->
   ets:delete(Ns),
   ok.


%%
%% register(Ns, Uid) -> ok
%%   Ns  = atom()
%%   Uid = term()
%%
%% Associates Uid with a Pid of current process.
%% Failes with badarg is assotiation exists and assotaited process is alive
%%
register(Uid, Pid) ->
   pts_ns:register(?DEFAULT_NS, Uid, Pid).

register(Ns, Uid, Pid) ->  
   case ets:insert_new(Ns, {Uid, Pid}) of
      true  -> 
         ok;
      false ->
         % insert is allowed if assotiated process is dead
         case pts_ns:whereis(Ns, Uid) of
            undefined -> 
               ets:insert(Ns, {Uid, Pid}),
               ok;
            Pid1 ->
               if
                  Pid  =:= Pid1 -> ok;
                  true          -> throw(badarg)
               end
         end
   end.
   
%%
%% unregister(Ns, Uid) -> ok
%%
%% Removes the registered Uid associatation with a Pid.
unregister(Uid) ->
   pts_ns:unregister(?DEFAULT_NS, Uid).
   
unregister(Ns, Uid) ->
   ets:delete(Ns, Uid), 
   ok.

%%
%% whereis(Ns, Uid) -> pid() | undefined
%%
%% Returns the Pid assotiated with Uid. 
%% Returns undefined if the name is not registered.   
whereis(Uid) ->
   pts_ns:whereis(?DEFAULT_NS, Uid).
   
whereis(Ns, Uid) ->
   case ets:lookup(Ns, Uid) of
      [{Uid, Pid}] ->
         case is_process_alive(Pid) of
            true  -> Pid;
            false -> undefined
         end;
      _            -> 
         undefined
   end.   
 
%%
%% whatis(Ns, Pid) -> [Uid]
%%
%% Returns the Uid assotiated with Pid, (reversive lookup)
%% Returns undefined if the Pid not found
whatis(Pid) ->
   pts_ns:whatis(?DEFAULT_NS, Pid).
   
whatis(Ns, Pid) ->
   ets:select(Ns, [{{'$1', Pid}, [], ['$1']}]).


   
%%
%% map(Ns, Fun) -> [...]
%%   Fun = fun({Uid, Pid}) -> ...
map(Fun) ->
   pts_ns:map(?DEFAULT_NS, Fun).
   
map(Ns, Fun) ->
   qlc:e(
      qlc:q([ Fun(X) || X <- ets:table(Ns)])
   ).
   
%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Acc, Fun) ->
   pts_ns:fold(?DEFAULT_NS, Acc, Fun).

fold(Ns, Acc0, Fun) ->
   qlc:fold(Fun, Acc0, 
      qlc:q([ X || X <- ets:table(Ns)])
   ).      
   
%%
%% send(Pid, Msg) -> ok | {error, ...}
%%
%% send async message
send(undefined, Msg) ->
   {error, no_process};
send(Pid, Msg) ->   
   case is_process_alive(Pid) of
      true  -> erlang:send(Pid, Msg);
      false -> {error, no_process}
   end.   
   
   
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

   