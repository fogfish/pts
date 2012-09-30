-module(pts_ns_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([append/2]).

%%
%%
start_link() ->
   supervisor:start_link({local, ?MODULE}, ?MODULE, []).
   
init(_) -> 
   {ok,
      {
         {one_for_one, 2, 3600},  % 2 failure in hour
         []
      }
   }.

append(Mod, Opts) ->
   supervisor:start_child(?MODULE, {
      Mod,
      {pts_factory, start_link, [Mod, Opts]},
      permanent, brutal_kill, supervisor, dynamic
   }).