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
-module(pts_bench_drv).

-export([new/1, run/4]).

%%
%%
new(_Id) ->
   try
      lager:set_loglevel(lager_console_backend, basho_bench_config:get(log_level, info)),
      init()
   catch _:Err ->
      lager:error("mets failed: ~p", [Err]),
      halt(1)
   end,
   {ok, undefined}.

%%
%%
run(put, KeyGen, ValGen, S) ->
   case (catch pts:put(kv, KeyGen(), ValGen())) of
      ok -> {ok, S};
      %E  -> io:format("p -> ~p~n", [E]), {error, failed, S}
      _  -> {error, failed, S}
   end;

run(get, KeyGen, _ValueGen, S) ->
   case (catch pts:get(kv, KeyGen())) of
      Val when is_binary(Val) -> {ok, S};
      undefined               -> {ok, S};
      %E -> io:format("g -> ~p~n", [E]), {error, failed, S}
      _ -> {error, failed, S}
   end;

run(remove, KeyGen, _ValueGen, S) ->
   case (catch pts:remove(kv, KeyGen())) of
      ok -> {ok, S};
      %E  -> io:format("r -> ~p~n", [E]), {error, failed, S}
      _  -> {error, failed, S}
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
         TTL = basho_bench_config:get(ttl, 30000),
         pts:new(kv, ['read-through', {supervisor, pts_cache_sup, [TTL]}])
   end.

