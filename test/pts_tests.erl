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

%%-----------------------------------------------------------------------------
%%
%% interface
%%
%%-----------------------------------------------------------------------------

interface_test_() ->
   {setup,
      fun interface_init/0,
      fun interface_free/1,
      [
         {"interface create", fun interface_create/0}
        ,{"interface put",    fun interface_put/0}
        ,{"interface get",    fun interface_get/0}
        ,{"interface remove", fun interface_remove/0}
      ]
   }.

interface_init() ->
   pts:start().

interface_free(_) ->
   application:stop(pts).

interface_create() ->
   {ok, _} = pts:start_link(iface, []),
   pns:register(iface, key, self()).

interface_put() ->
   pts:put_(iface, key, value),
   {put, key, value} = pipe:recv().

interface_get() ->
   pts:get_(iface, key),
   {get, key} = pipe:recv().

interface_remove() ->
   pts:remove_(iface, key),
   {remove, key} = pipe:recv().

%%-----------------------------------------------------------------------------
%%
%% cache
%%
%%-----------------------------------------------------------------------------

bucket_test_() ->
   {setup,
      fun bucket_init/0,
      fun bucket_free/1,
      [
         {"bucket create", fun bucket_create/0}
        ,{"bucket put",    fun bucket_put/0}
        ,{"bucket get",    fun bucket_get/0}
        ,{"bucket remove", fun bucket_remove/0}
        ,{"bucket call",   fun bucket_call/0}
        ,{"bucket cast",   fun bucket_cast/0}
        ,{"bucket fold",   fun bucket_fold/0}
      ]
   }.

'<<'(Ref)
 when is_reference(Ref) ->
   receive {Ref, Msg} -> Msg end;
'<<'(Any) ->
   Any.

bucket_init() ->
   pts:start().

bucket_free(_) ->
   application:stop(pts).

bucket_create() ->
   {ok, _} = pts:start_link(bucket, [
      {keylen, 1}
     ,{factory, temporary}
     ,{entity,  {pts_cache, start_link, [60000]}}
   ]).

bucket_put() ->
   ok = pts:put(bucket, key, <<"val">>),
   ok = pts:put(bucket, key, <<"new">>),

   ok = '<<'(pts:put_(bucket, {yek, 1}, <<"val">>)),
   ok = '<<'(pts:put_(bucket, {yek, 2}, <<"new">>)),

   ok = pts:put_(bucket, {eky, 1}, <<"val">>, false),
   ok = pts:put_(bucket, {eky, 2}, <<"new">>, false).


bucket_get() ->
   <<"new">> = pts:get(bucket, key),
   <<"new">> = '<<'(pts:get_(bucket, {yek, 2})),
   ok        = pts:get_(bucket, {eky, 2}, false).

bucket_remove() ->
   ok = pts:remove(bucket, key),
   ok = '<<'(pts:remove_(bucket, {yek, 2})),
   ok = pts:remove_(bucket, {eky, 2}, false),

   timer:sleep(100),

   {error, not_found} = pts:get(bucket, key),
   {error, not_found} = pts:get(bucket, {yek, 2}),
   {error, not_found} = pts:get(bucket, {eky, 2}).


bucket_call() ->
   ok = pts:put(bucket, key, <<"val">>),
   ok = pts:call(bucket, key, {ttl, 30000}),
   {ok, 30000} = pts:call(bucket, key, ttl),
   ok = pts:remove(bucket, key).

bucket_cast() ->
   ok = pts:put(bucket, key, <<"val">>),
   _  = pts:cast(bucket, key, {ttl, 30000}),
   {ok, 30000} = pts:call(bucket, key, ttl),
   ok = pts:remove(bucket, key).

bucket_fold() ->
   lists:foreach(
      fun(X) -> ok = pts:put(bucket, X, X) end,
      lists:seq(1,5)
   ),
   M = pts:fold(
      fun(K, V, A) -> A + K * V() end, 0, bucket),
   M = lists:foldl(
      fun(X, A) -> A + X * X end,
      0,
      lists:seq(1,5)
   ).



