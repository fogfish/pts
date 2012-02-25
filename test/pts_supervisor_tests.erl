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
-module(pts_supervisor_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").


proc(Tab, Key0, Val0) ->
   receive
      {pts, Tx, {put, Key, Val}} ->
         pts:notify(Tx, ok),
         proc(Tab, Key, Val);
      {pts, Tx, {get, Key}} ->
         pts:notify(Tx, {ok, Val0}),
         proc(Tab, Key0, Val0);
      {pts, Tx, {remove, Key}} ->
         pts:notify(Tx, ok);
      _ ->
         throw(badarg)
   end.

factory({respawn, Tab, [Key]}) ->
   factory(Tab, Key).
factory(Tab, Key) ->
   {ok, spawn(
      fun() ->
         pts:attach(Tab, Key),
         proc(Tab, Key, nil)
      end
   )}.
   
pts_supervisor_test_() ->
   {
      setup,
      fun()  ->
         application:start(pts),
         pts:new(test, [
            {supervise, true},
            {respawn, fun factory/1},
            {factory, fun factory/2}
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
   ok = pts:put(test, key1, value).
   
respawn() ->
   Pid = pts_ns:whereis(test, key1),
   Pid ! error,
   R = pts:get(test, key1),
   error_logger:error_report([{got, R}]),
   timer:sleep(500),
   {ok, nil} = pts:get(test, key1).
   