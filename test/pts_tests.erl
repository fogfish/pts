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
-module(pts_tests).
-include_lib("eunit/include/eunit.hrl").

init_cache_test() ->
   application:start(pns),
   application:start(pts),
   {ok, _} = pts:start_link(test, [
      {keylen, 1},
      {entity, {pts_cache, start_link, [60000]}}
   ]).

put_cache_test() ->
   ok = pts:put(test, {a, 1}, <<"val">>),
   ok = pts:put(test, {a, 1}, <<"new">>),

   ok = '<<'(pts:put_(test, {a, 2}, <<"val">>)),
   ok = '<<'(pts:put_(test, {a, 2}, <<"new">>)),

   ok = pts:put_(test, {a, 3}, <<"val">>, false),
   ok = pts:put_(test, {a, 3}, <<"new">>, false).

get_cache_test() ->
   <<"new">> = pts:get(test, {a, 1}),
   <<"new">> = '<<'(pts:get_(test, {a, 2})),
   ok        = pts:get_(test, {a, 3}, false).

remove_cache_test() ->
   ok = pts:remove(test, {a, 1}),
   ok = '<<'(pts:remove_(test, {a, 2})),
   ok = pts:remove_(test, {a, 3}, false),

   {error, not_found} = pts:get(test, {a, 1}),
   {error, not_found} = '<<'(pts:get_(test, {a, 2})),
   {error, not_found} = '<<'(pts:get_(test, {a, 3})).

call_test() ->
   ok = pts:put(test, {a, 1}, <<"val">>),
   ok = pts:call(test, {a, 1}, {ttl, 30000}),
   {ok, 30000} = pts:call(test, {a, 1}, ttl),
   ok = pts:remove(test, {a, 1}).

cast_test() ->
   ok = pts:put(test, {a, 1}, <<"val">>),
   _  = pts:cast(test, {a, 1}, {ttl, 30000}),
   {ok, 30000} = pts:call(test, {a, 1}, ttl),
   ok = pts:remove(test, {a, 1}).

map_test() ->
   lists:foreach(
      fun(X) -> ok = pts:put(test, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:map(fun({Key, Pid}) -> io:format("--> ~p ~p~n", [Key, pts:get(Pid, Key)]), Key * pts:get(Pid, Key) end, test), 
   lists:foreach(
      fun(X) -> true = lists:member(X*X, M) end, 
      lists:seq(1,5)
   ).
   
fold_test() ->
   lists:foreach(
      fun(X) -> ok = pts:put(test, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold(fun({K, V}, A) -> A + K * pts:get(V, K) end, 0, test),
   M = lists:foldl(
      fun(X, A) -> A + X*X end,
      0,
      lists:seq(1,5)
   ).


%%
'<<'(Ref) ->
   receive {Ref, Msg} -> Msg end.

