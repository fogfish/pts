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
-module(cache_entry_sup).
-behaviour(supervisor).
-author(dmkolesnikov@gmail.com).

-export([
   start_link/0,
   init/1,
   create/1
]).

create(Req) ->
   supervisor:start_child(?MODULE, [Req]).

start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   

init([]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         [
         {
            cache_entry,
            {
               cache_entry,
               start_link,
               []
            },
            temporary, brutal_kill, worker, dynamic
         }
         ]
      }
   }.