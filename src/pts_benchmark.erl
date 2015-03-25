%%
%%   Copyright (c) 2012, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%%   @description
%%      basho_bench driver
-module(pts_benchmark).

-export([new/1, run/4]).
-export([run/0]).

%%
%%
new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      init()
   catch _:Err ->
      lager:error("pts failed: ~p", [Err]),
      halt(1)
   end,
   Timeout = basho_bench_config:get(timeout, 30000),
   {ok, Timeout}.

%%
%%
run(put, KeyGen, ValGen, S) ->
   Key = KeyGen(),
   case (catch pts:put(kv, Key, ValGen(), S)) of
      ok -> {ok, S};
      E  -> {error, failure(p, Key, E), S}
   end;

run(get, KeyGen, _ValueGen, S) ->
   Key = KeyGen(),
   case (catch pts:get(kv, Key, S)) of
      Val when is_binary(Val) -> {ok, S};
      undefined               -> {ok, S};
      E -> {error, failure(g, Key, E), S}
   end;

run(remove, KeyGen, _ValueGen, S) ->
   Key = KeyGen(),
   case (catch pts:remove(kv, Key, S)) of
      ok -> {ok, S};
      E  -> {error, failure(r, Key, E), S}
   end.


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
init() ->
   case pts:start() of
      {error, {already_started, _}} ->
         ok;
      ok ->
         TTL     = basho_bench_config:get(ttl,      30000),
         Capacity= basho_bench_config:get(capacity,   inf),
         {ok, _} = pts:start_link(kv, [
            'read-through',
            {factory, temporary},
            {entity,  {pts_cache, start_link, [TTL]}},
            {capacity, Capacity}
         ])
   end.

%%
%%
failure(Tag, Key, E) ->
   % io:format("-> ~p ~p ~p ~n", [Tag, Key, E]),
   failed.


%%%----------------------------------------------------------------------------   
%%%
%%% stress
%%%
%%%----------------------------------------------------------------------------   

-define(N,        8).
-define(LOOP,    10 * 1000).
-define(TIMEOUT, 60 * 1000).

run() ->
   pts:start(),
   {ok, _} = pts:start_link(kv, [
      'read-through',
      {factory, temporary},
      {entity,  {pts_cache, start_link, [60000]}}
   ]),
   case timer:tc(fun() -> exec(?N) end) of
      {T, ok} ->
         TPU = ?N * ?LOOP / T,
         TPS = ?N * ?LOOP / (T / 1000000),
         {TPU, TPS};
      {_, Error} ->
         Error
   end.

exec(N) ->
   Self = self(),
   Pids = [spawn_link(fun() -> loop(Self, Id, ?LOOP) end) || Id <- lists:seq(1, N)],
   fold(Pids).

fold([]) -> ok;
fold([Pid | Pids]) ->
   receive
      {ok, Pid} -> fold(Pids)
   after ?TIMEOUT ->
      {error, timeout}
   end.

loop(Pid, _Id, 0) ->
   Pid ! {ok, self()};
loop(Pid,  Id, N) ->
   pts:ensure(kv, {Id, N}),
   loop(Pid, Id, N - 1).



