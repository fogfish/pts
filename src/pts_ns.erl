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

%%
%% Max Theoretical scalability demand at 32-bit arch is 268435456 entries
%%

-export([
   start/0,
   register/1,
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
   fold/3
]).

%%
%% core opts for namespace
-define(CORE_OPTS, [
   ordered_set,
   {write_concurrency, true}
]).

%%
%% default Namespace
-define(REGISTRY, pts_local).

%%
%%
%%
start() ->
   new(?REGISTRY),
   ok.



%%
%% register(Uid, Pid) -> ok
%%   Ns  = atom()
%%   Uid = term()
%%
%% Associates Uid with a Pid of current process.
%% Failes with badarg is assotiation exists and assotaited process is alive
%%
register(Uid) ->
   pts_ns:register(Uid, self()).
register(Ns,  Uid, Pid) ->
   pts_ns:register({Ns, Uid}, Pid).
register(Uid, Pid) ->
   case ets:insert_new(?REGISTRY, {Uid, Pid}) of
      true  -> 
         ok;
      false ->
         % entry exists but insert is allowed if assotiated process is dead
         case pts_ns:whereis(Uid) of
            undefined -> 
               ets:insert(?REGISTRY, {Uid, Pid}),
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
unregister(Ns, Uid) ->
   pts_ns:unregister({Ns, Uid}).
unregister(Uid) ->
   ets:delete(?REGISTRY, Uid),
   ok.

%%
%% whereis(Ns, Uid) -> pid() | undefined
%%
%% Returns the Pid assotiated with Uid. 
%% Returns undefined if the name is not registered.   
whereis(Ns, Uid) ->
   pts_ns:whereis({Ns, Uid}).
   
whereis(Uid) ->
   case ets:lookup(?REGISTRY, Uid) of
      [{Uid, Pid}] ->
         case is_uid_alive(Pid) of
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
   ets:select(?REGISTRY, [{{'$1', Pid}, [], ['$1']}]).
   
whatis(Ns, Pid) ->
   ets:select(?REGISTRY, [{{{Ns, '$1'}, Pid}, [], [{Ns, '$1'}]}]).
   
%%
%% map(Ns, Fun) -> [...]
%%   Fun = fun({Uid, Pid}) -> ...
map(Fun) ->
   qlc:e(
      qlc:q([ Fun(X) || X <- ets:table(?REGISTRY)])
   ).   
   
map(Ns0, Fun) ->
   qlc:e(
      qlc:q([ 
         Fun({{Ns, X}, Pid}) 
         || {{Ns, X}, Pid} <- ets:table(?REGISTRY), Ns =:= Ns0
      ])
   ).
   
%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Acc0, Fun) ->
   qlc:fold(Fun, Acc0, 
      qlc:q([ X || X <- ets:table(?REGISTRY)])
   ).

fold(Ns0, Acc0, Fun) ->
   qlc:fold(Fun, Acc0, 
      qlc:q([ 
         {{Ns, X}, Pid} || {{Ns, X}, Pid} <- ets:table(?REGISTRY), Ns =:= Ns0
      ])
   ).      
      
   
%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% check 
is_uid_alive(Uid) when is_pid(Uid) ->
   is_process_alive(Uid);
is_uid_alive(Uid) ->
   true.


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

   