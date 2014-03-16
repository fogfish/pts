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
%%     name space supervisor, manages
%%      * leader process
%%      * entity factory
-module(pts_ns_sup).
-behaviour(supervisor).
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-export([
   start_link/2
  ,init/1
]).

%%
-define(CHILD(Type, I),            {I,  {I, start_link,   []}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, I, Args),      {I,  {I, start_link, Args}, permanent, 30000, Type, dynamic}).
-define(CHILD(Type, ID, I, Args),  {ID, {I, start_link, Args}, permanent, 30000, Type, dynamic}).

%%
%%
start_link(Name, Opts) ->
   supervisor:start_link(?MODULE, [Name, Opts]).
   
init([Name, Opts]) ->       
   init(Name, Opts).

init(Name, Opts) ->
   {ok,
      {
         {one_for_one, 4, 1800}, 
         child(lists:keyfind(factory, 1, Opts), Name, Opts)
      }
   }.

%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% create list of child
child({_, transient}, Name, Opts) ->
   {entity, Entity} = lists:keyfind(entity, 1, Opts),
   [
      ?CHILD(worker,     pts_ns,         [self(), Name, Opts])
     ,?CHILD(supervisor, pts_entity_sup, [transient, Entity])
   ];

child({_, temporary}, Name, Opts) ->
   {entity, Entity} = lists:keyfind(entity, 1, Opts),
   [
      ?CHILD(worker,     pts_ns,         [self(), Name, Opts])
     ,?CHILD(supervisor, pts_entity_sup, [temporary, Entity])
   ];

child({_, permanent}, Name, Opts) ->
   {entity, Entity} = lists:keyfind(entity, 1, Opts),
   [
      ?CHILD(worker,     pts_ns,         [self(), Name, Opts])
     ,?CHILD(supervisor, pts_entity_sup, [permanent, Entity])
   ];

child(_, Name, Opts) ->
   %% factory type is not defined 
   [
      ?CHILD(worker, pts_ns, [self(), Name, Opts])
   ].
   
