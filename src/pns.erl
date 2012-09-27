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
%%     process namespace
%%
-module(pns).
-author(dmkolesnikov@gmail.com).
-include_lib("stdlib/include/qlc.hrl").

-export([register/2, register/3, unregister/2, whereis/2, whatis/2, map/2, fold/3]).

%%
%% register(Ns, Uid, Pid) -> ok
%% register(Ns, Uid)
%%   Ns  = atom()
%%   Uid = term()
%%
%% Associates Uid with a Pid of current process.
%% Failes with badarg is assotiation exists and assotaited process is alive
%%
register(Ns, Uid) ->
   ?MODULE:register(Ns, Uid, self()).
register(Ns, Uid, Pid) ->
   case ?MODULE:whereis(Ns, Uid) of
      undefined -> 
         ets:insert(pns, {{Ns, Uid}, Pid}),
         ok;
      Old when Old =:= Pid ->
         ok;
      _ ->
         throw(badarg)
   end. 

%%
%% unregister(Ns, Uid) -> ok
%%
%% Removes the registered Uid associatation with a Pid.
unregister(Ns, Uid) ->
   ets:delete(pns, {Ns, Uid}),
   ok.

%%
%% whereis(Ns, Uid) -> pid() | undefined
%%
%% Returns the Pid assotiated with Uid. 
%% Returns undefined if the name is not registered.   
whereis(Ns, Uid) ->
   case ets:lookup(pns, {Ns, Uid}) of
      [{_, Pid}] ->
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
whatis(Ns, Pid) ->
   ets:select(pns, [{{{Ns, '$1'}, Pid}, [], ['$1']}]).
   
%%
%% map(Ns, Fun) -> [...]
%%   Fun = fun({Uid, Pid}) -> ...
map(Ns0, Fun) ->
   qlc:e(
      qlc:q([ 
         Fun({Uid, Pid}) 
         || {{Ns, Uid}, Pid} <- ets:table(pns), Ns =:= Ns0, is_uid_alive(Pid)
      ])
   ).
   
%%
%% foldl(Tab, Acc0, Fun) -> Acc
%%
fold(Ns0, Acc0, Fun) ->
   qlc:fold(Fun, Acc0, 
      qlc:q([ 
         {Uid, Pid} || {{Ns, Uid}, Pid} <- ets:table(pns), Ns =:= Ns0, is_uid_alive(Pid)
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
is_uid_alive(_) ->
   true.

   