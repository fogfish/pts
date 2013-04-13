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
%%  @description
%%     name space container
-module(pts_ns).
-behaviour(gen_server).
-author(dmkolesnikov@gmail.com).

-include("pts.hrl").

-export([
   start_link/3,
   init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3
]).

%% internal state
-record(pts, {
   name              :: atom(),          % unique name-space id
   factory           :: pid(),           % process factory
   keylen    = inf   :: integer() | inf, % length of key (key prefix used to distinguish a process)
   readonly  = false :: boolean(),       % write operations are disabled
   rthrough  = false :: boolean(),       % read-through
   immutable = false :: boolean(),       % write-once (written value cannot be changed)
   entity            :: any()            % entity specification (for accounting only)
}).

%%
%%
start_link(Sup, Name, Opts) ->
   gen_server:start_link({local, Name}, ?MODULE, [Sup, Name, Opts], []).

init([Sup, Name, Opts]) ->
   self() ! {set_factory, Sup}, % message to itself, avoid supervisor deadlock
   {ok, init(Opts, #pts{name=Name})}.

init([{keylen, X} | Opts], S) ->
   init(Opts, S#pts{keylen=X}); 

init([readonly | Opts],  S) ->
   init(Opts, S#pts{readonly=true});

init([immutable | Opts], S) ->
   init(S, S#pts{immutable=true});

init(['read-through' | Opts], S) ->
   init(Opts, S#pts{rthrough=true});

init([{entity, X} | Opts], S) ->
   init(Opts, S#pts{entity=X});

init([_ | Opts], S) ->
   init(Opts, S);

init([], S) ->
   S.

terminate(_, _) ->
   ok.


% %%
% %%
% start_link(Bucket) ->
%    gen_server:start_link(?MODULE, [Bucket], []).
  
% init([#pts{owner=Owner, supervisor=undefined}=Bucket]) ->
%    % readonly namespace
%    erlang:monitor(process, Owner),
%    ets:insert(pts, Bucket),
%    {ok, Bucket};

% init([#pts{owner=Owner, supervisor=Spec}=Bucket]) ->
%    erlang:monitor(process, Owner),
%    {ok, Sup} = init_ns_sup(Spec),
%    ets:insert(pts, Bucket#pts{supervisor=Sup}),
%    {ok, Bucket#pts{supervisor=Sup}}.

% init_ns_sup({Mod, Opts}) ->
%    erlang:apply(Mod, start_link, Opts);

% init_ns_sup(Mod) ->
%    erlang:apply(Mod, start_link, []).


% terminate(_Reason, #pts{ns=Ns}) ->
%    ets:delete(pts, Ns),
%    % TODO: fix pure hack
%    spawn(
%       fun() ->
%          supervisor:terminate_child(pts_sup, Ns),
%          supervisor:delete_child(pts_sup, Ns)
%       end
%    ),
%    ok.


%%
%%
handle_call(i, _, S) ->
   {reply, lists:zip(record_info(fields, pts), tl(tuple_to_list(S))), S};

handle_call({put, Key, Val}, Tx, S) ->
   case proc_put({put, Tx, Key, Val}, S) of
      {error, Reason} ->
         {reply, {error, Reason}, S};
      _ ->
         {noreply, S}
   end;

handle_call({get, Key}, Tx, S) ->
   case proc_get({get, Tx, Key}, S) of
      {error, Reason} ->
         {reply, {error, Reason}, S};
      _ ->
         {noreply, S}
   end;

handle_call({remove, Key}, Tx, S) ->
   case proc_remove({remove, Tx, Key}, S) of
      {error, Reason} ->
         {reply, {error, Reason}, S};
      _ ->
         {noreply, S}
   end;

handle_call({call, Key, Req}, Tx, S) ->
   case proc_send({send, Key, {'$gen_call', Tx, Req}}, S) of
      {error, Reason} ->
         {reply, {error, Reason}, S};
      _ ->
         {noreply, S}
   end;

handle_call(_, _Tx, S) ->
   {noreply, S}.

%%
%%
handle_cast({put, Tx, Key, Val}, S) ->
   case proc_put({put, Tx, Key, Val}, S) of
      {error, Reason} ->
         pts:ack(Tx, {error, Reason}),
         {noreply, S};
      _ ->
         {noreply, S}
   end;

handle_cast({get, Tx, Key}, S) ->
   case proc_get({get, Tx, Key}, S) of
      {error, Reason} ->
         pts:ack(Tx, {error, Reason}),
         {noreply, S};
      _ ->
         {noreply, S}
   end;

handle_cast({remove, Tx, Key}, S) ->
   case proc_remove({remove, Tx, Key}, S) of
      {error, Reason} ->
         pts:ack(Tx, {error, Reason}),
         {noreply, S};
      _ ->
         {noreply, S}
   end;

handle_cast({cast, Key, Req}, S) ->
   proc_send({send, Key, {'$gen_cast', Req}}, S),
   {noreply, S};

handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({set_factory, Sup}, S) ->
   {ok, Pid} = pts_ns_sup:factory(Sup),
   {noreply, S#pts{factory = Pid}};

handle_info({put, Key, Val}, S) ->
   proc_put({put, undefined, Key, Val}, S),
   {noreply, S};

handle_info({get, Key}, S) ->
   proc_get({get, undefined, Key}, S),
   {noreply, S};

handle_info({remove, Key}, S) ->
   proc_remove({remove, undefined, Key}, S),
   {noreply, S};

handle_info({send, Key, Req}, S) ->
   proc_send({send, Key, Req}, S),
   {noreply, S};

handle_info(_, S) ->
   {noreply, S}.

%%
%%
code_change(_OldVsn, S, _Extra) ->
   {ok, S}.     


%%-----------------------------------------------------------------------------
%%
%% private
%%
%%-----------------------------------------------------------------------------

%%
%% put value
proc_put({put, _Tx, _Key, _Val}, #pts{readonly=true}) ->
   {error, readonly};

proc_put({put, Tx, Key, Val}, #pts{immutable=true}=S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen),
      undefined = pns:whereis(S#pts.name, Uid),
      {ok, Entity} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
      erlang:send(Entity, {put, Tx, Key, Val}),
      ok
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, immutable}
   end;

proc_put({put, Tx, Key, Val}, S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen), 
      {ok, Entity} = case pns:whereis(S#pts.name, Uid) of
         undefined -> supervisor:start_child(S#pts.factory, [S#pts.name, Uid]);
         Pid       -> {ok, Pid}
      end,
      erlang:send(Entity, {put, Tx, Key, Val}),
      ok
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end.

%%
%%
proc_get({get, Tx, Key}, #pts{rthrough=true}=S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen), 
      {ok, Entity} = case pns:whereis(S#pts.name, Uid) of
         undefined -> supervisor:start_child(S#pts.factory, [S#pts.name, Uid]);
         Pid       -> {ok, Pid}
      end,
      erlang:send(Entity, {get, Tx, Key}),
      ok
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end;

proc_get({get, Tx, Key}, S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen),
      case pns:whereis(S#pts.name, Uid) of
         undefined -> 
            {error, not_found};
         Entity    -> 
            erlang:send(Entity, {get, Tx, Key}),
            ok
      end
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end.

%%
%%
proc_remove({remove, _Tx, _Key}, #pts{readonly=true}) ->
   {error, readonly};

proc_remove({remove, Tx, Key}, S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen),
      case pns:whereis(S#pts.name, Uid) of
         undefined -> pts:ack(Tx, ok);
         Entity    -> erlang:send(Entity, {remove, Tx, Key})
      end,
      {noreply, S}
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end.

%%
%%
proc_send({send, Key, Msg}, #pts{rthrough=true}=S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen), 
      {ok, Entity} = case pns:whereis(S#pts.name, Uid) of
         undefined -> supervisor:start_child(S#pts.factory, [S#pts.name, Uid]);
         Pid       -> {ok, Pid}
      end,
      erlang:send(Entity, Msg),
      ok
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end;

proc_send({send, Key, Msg}, S) ->
   try
      Uid = key_to_uid(Key, S#pts.keylen),
      case pns:whereis(S#pts.name, Uid) of
         undefined -> 
            {error, not_found};
         Entity    -> 
            erlang:send(Entity, Msg),
            ok
      end
   catch _:Reason ->
      error_logger:error_report([{pts, S#pts.name}, {error, Reason} | erlang:get_stacktrace()]),
      {error, Reason}
   end.


%% transforms key to unique process identifier   
key_to_uid(Key, inf) ->
   Key;
key_to_uid(Key,   1)
 when is_tuple(Key) ->   
   erlang:element(1, Key);
key_to_uid(Key, Len)
 when is_tuple(Key) ->
   list_to_tuple(
      lists:sublist(
         tuple_to_list(Key),
         Len
      )
   );
key_to_uid(Key, _) ->
   Key.
