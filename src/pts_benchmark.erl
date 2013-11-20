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
   case application:start(pts) of
      {error, {already_started, _}} ->
         ok;
      ok ->
         TTL     = basho_bench_config:get(ttl, 30000),
         {ok, _} = pts:start_link(kv, [
            'read-through',
            {entity, {pts_cache, start_link, [TTL]}}
         ])
   end.

%%
%%
failure(Tag, _Key, E) ->
   %io:format("----> ~p~n", [process_info(pns:whereis(kv, Key))]),
   io:format("~s -> ~p~n", [Tag, E]),
   failed.


