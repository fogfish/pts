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
-module(pts_reg_tests).
-author(dmkolesnikov@gmail.com).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
   ok = application:start(pts).
   
register_test() ->
   ?assert(
      ok =:= pts:register("mykey")
   ).
   
register_twice_test() ->
   ?assert(
      badarg =:= (catch pts:register("mykey") )
   ).
   
whereis_test() ->
   ?assert(
      self() =:= pts:whereis("mykey")
   ).
   
registered_test() ->
   ?assert(
      ["mykey"] =:= pts:registered()
   ).
   
unregister_test() ->
   ?assert(
      ok =:= pts:unregister("mykey")
   ).
   
unregister_twice_test() ->
   ?assert(
      badarg =:= (catch pts:unregister("mykey"))
   ).