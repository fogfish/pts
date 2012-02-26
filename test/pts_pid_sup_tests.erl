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
-module(pts_pid_sup_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").


proc(Key0, Val0) ->
   receive
      {pts, Tx, {put, Key, Val}} ->
         pts:notify(Tx, ok),
         proc(Key, Val);
      {pts, Tx, {get, Key}} ->
         pts:notify(Tx, {ok, Val0}),
         proc(Key0, Val0);
      {pts, Tx, {remove, Key}} ->
         pts:notify(Tx, ok);
      _ ->
         throw(badarg)
   end.

factory({recovery, [Key]}) ->
   factory({create, Key});
factory({create, Key}) ->
   {ok, spawn(
      fun() ->
         pts:attach(Key),
         proc(Key, nil)
      end
   )}.
   
-define(NS1, test).
-define(KEY, {key, 1}).
   
pts_supervisor_test_() ->
   {
      setup,
      fun()  ->
         application:start(pts),
         pts:new(?NS1, [
            {supervise, true},
            {factory, fun factory/1}
         ])
      end,
      fun(_) ->
         application:stop(pts)
      end,
      [
         {"put", fun put/0},
         {"respawn", fun respawn/0}
      ]
   }.
   
   
put() ->
   ok = pts:put({?NS1, ?KEY}, value).
   
respawn() ->
   {ok, value} = pts:get({?NS1, ?KEY}),
   Pid = pts_ns:whereis({?NS1, ?KEY}),
   Pid ! error,
   timer:sleep(500),
   {ok, nil} = pts:get({?NS1, ?KEY}).
   