-module(kv8_pts).

-export([start/1, stop/1, init/1, free/1, put/3, get/2]).

%%
%%
start(Opts) ->
   pts:new(kv8, [{factory, fun pts_cache_sup:spawn/2} | Opts]),
   kv8.

%%
%%
stop(Ref) ->
   pts:drop(Ref).

%%
%%
init(Ref) ->
   Ref. 

%%
%%
free(Ref) ->
   Ref.

%%
%%
put(Key, Val, Ref) ->
   pts:put(Ref, Key, Val).

%%
%%
get(Key, Ref) ->
   pts:get(Ref, Key).




   