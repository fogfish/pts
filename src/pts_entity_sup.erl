%%
%%   Copyright (c) 2012 - 2013, Dmitry Kolesnikov
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
%%     entity supervisor
-module(pts_entity_sup).
-behaviour(supervisor).
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-export([
   start_link/3, 
   init/1
]).

%%
-define(CHILD(T, R, M),        {M, {M, start_link, []}, R, 30000, T, dynamic}).
-define(CHILD(T, R, M, A),     {M, {M, start_link,  A}, R, 30000, T, dynamic}).
-define(CHILD(T, R, M, F, A),  {M, {M,          F,  A}, R, 30000, T, dynamic}).

start_link(Type, Recovery, Spec) ->
   supervisor:start_link(?MODULE, [Type, Recovery, Spec]).

init([Type, Recovery, Spec]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         child(Type, Recovery, Spec)
      }
   }.

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

child(Type, Recovery, Mod)
 when is_atom(Mod) ->
   [?CHILD(Type, Recovery, Mod)];

child(Type, Recovery, {Mod, Args})
 when is_atom(Mod) ->
   [?CHILD(Type, Recovery, Mod, Args)];

child(Type, Recovery, {M, F, A}) ->
   [?CHILD(Type, Recovery, M, F, A)].



