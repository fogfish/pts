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
   start_link/2, 
   init/1
]).

%%
-define(CHILD(R, M),        {M, {M, start_link, []}, R, 30000, worker, dynamic}).
-define(CHILD(R, M, A),     {M, {M, start_link,  A}, R, 30000, worker, dynamic}).
-define(CHILD(R, M, F, A),  {M, {M,          F,  A}, R, 30000, worker, dynamic}).

start_link(Recovery, Spec) ->
   supervisor:start_link(?MODULE, [Recovery, Spec]).

init([Recovery, Spec]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         child(Recovery, Spec)
      }
   }.

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

child(Recovery, Mod)
 when is_atom(Mod) ->
   [?CHILD(Recovery, Mod)];

child(Recovery, {Mod, Args})
 when is_atom(Mod) ->
   [?CHILD(Recovery, Mod, Args)];

child(Recovery, {M, F, A}) ->
   [?CHILD(Recovery, M, F, A)].



