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
-module(pts_prot).
-author(dmkolesnikov@gmail.com).

-export([
   req/3
]).




%%
%% Request PTS operation from process
req(Fun, Req, _Timeout) when is_function(Fun) ->
   % instantiates a process 
   Fun({pts, self(), Req}); 

req(Pid, Req, Timeout) ->
   try erlang:monitor(process, Pid) of
      Ref ->
         Tx = {self(), Ref},
         catch erlang:send(Pid, {pts, Tx, Req}, [noconnect]),
         receive
            {'DOWN', Ref, _, _, Reason} -> 
               {error, Reason};
            {pts, Tx, Response}  -> 
               erlang:demonitor(Ref, [flush]),
               Response
         after Timeout ->
            erlang:demonitor(Ref, [flush]),
		      {error, timeout}
         end
   catch
      error:_ -> {error, system}
   end.    