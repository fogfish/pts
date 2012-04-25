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
-module(cache_app).
-author(dmkolesnikov@gmail.com).

-export([
   start/2,
   stop/1
]).


start(_Type, _Args) ->
   case cache_sup:start_link() of
      {ok, Pid} ->
         % meta data for pts tables
         ok = pts:new(cache, [
            {factory, fun cache_entry_sup:create/1}
         ]),
         {ok, Pid};
      Err ->
         Err
   end.
   
stop(_S) ->
   ok.