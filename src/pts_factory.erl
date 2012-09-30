-module(pts_factory).
-behaviour(supervisor).

-export([start_link/2, init/1, create/3]).

create(Mod, Ns, Uid) ->
   supervisor:start_child(Mod, [Ns, Uid]).

%%-----------------------------------------------------------------------------
%%
%% supervisor
%%
%%-----------------------------------------------------------------------------
start_link(Mod, Opts) ->
   supervisor:start_link({local, Mod}, ?MODULE, [Mod, Opts]).
   
init([Mod, Opts]) ->
   {ok,
      {
         {simple_one_for_one, 10, 60},
         [factory(Mod, Opts)]
      }
   }.

factory(Mod, Opts) ->
   {factory, 
      {Mod, start_link, Opts},
      transient, brutal_kill, worker, dynamic
   }.