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
   start_link/2, 
   init/1,
   factory/1
]).

%%
%%
start_link(Name, Opts) ->
   supervisor:start_link(?MODULE, [Name, Opts]).
   
init([Name, Opts]) -> 
   {ok,
      {
         {one_for_all, 4, 1800}, 
         [ns_spec(Name, Opts), factory_spec(Name, Opts)]
      }
   }.

%% name space leader
ns_spec(Name, Opts) ->
   {
      ns,
      {pts_ns, start_link, [self(), Name, Opts]},
      permanent, 60000, worker, dynamic
   }.

%% name space factory
factory_spec(_, Opts) ->
   {entity, Entity} = lists:keyfind(entity, 1, Opts),
   {
      factory,
      {pts_entity_sup, start_link, [sup_type(Opts), Entity]},
      permanent, 30000, supervisor, dynamic
   }.

sup_type(Opts) ->
   case lists:keyfind(supervisor, 1, Opts) of
      false     -> transient;
      {_, Type} -> Type
   end.


%%
%%
factory(Sup) ->
   {_, Pid, _, _} = lists:keyfind(factory, 1, supervisor:which_children(Sup)),
   {ok, Pid}.

