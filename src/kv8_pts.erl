-module(kv8_pts).

-export([init/1, free/1, put/3, get/2]).

%%
%%
init(_) ->
   application:start(pts),
   pts:new(kv8, [
      async,      % async interface 
      rthrough,   % read through
      {iftype,   raw},
      {factory,  fun factory/1}
   ]),
   kv8.

%%
%%
free(Ref) ->
   pts:drop(Ref).

%%
%%
put(Ref, Key, Val) ->
   pts:create(Ref, {Key, Val}),
   ok.

%%
%%
get(Ref, Key) ->
   case pts:read(Ref, Key) of
      {ok, Val} -> {ok, Val};
      _         -> {error, not_found}
   end.



factory([{_, Key}]) ->
   Pid = spawn(
      fun() ->
      	pts:attach(Key),
      	loop(undefined)
      end
   ),
   {ok, Pid}.

loop(S) ->
   receive
      {put, {_, Val}} ->
   	     loop(Val);
      {pts, Tx, {get, _}} ->
         gen:reply(Tx, {ok, S}),
         loop(S);
      M -> 
         io:format('got: ~p~n', [M]),
         loop(S)
   end.


   