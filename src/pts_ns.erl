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
%%     name space leader process
-module(pts_ns).
-behaviour(gen_server).
-author('Dmitry Kolesnikov <dmkolesnikov@gmail.com>').

-include("pts.hrl").

-export([
   start_link/3,
   init/1, 
   terminate/2,
   handle_call/3, 
   handle_cast/2, 
   handle_info/2,
   code_change/3
]).

%% internal state
-record(pts, {
   name              :: atom(),          % unique name-space id
   factory           :: pid(),           % process factory
   keylen    = inf   :: integer() | inf, % length of key (key prefix used to distinguish a process)
   readonly  = false :: boolean(),       % write operations are disabled
   rthrough  = false :: boolean(),       % read-through
   immutable = false :: boolean(),       % write-once (written value cannot be changed)
   nofactory = false :: boolean(),       % objects are created outside of pts (e.g. by application)
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
   init(Opts, S#pts{immutable=true});

init([nofactory | Opts], S) ->
   init(Opts, S#pts{nofactory=true});

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

%%
%%
handle_call(i, _Tx, S) ->
   {reply, lists:zip(record_info(fields, pts), tl(tuple_to_list(S))), S};

handle_call({whereis, Key}, _Tx, S) ->
   Uid = key_to_uid(Key, S#pts.keylen),
   {reply, pns:whereis(S#pts.name, Uid), S};

handle_call({ensure, Key}, _Tx, S) ->
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined ->
         {ok, Pid} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
         {reply, Pid, S};
      Pid       ->
         {reply, Pid, S}
   end;


% handle_call({Key, Req}, Tx, S) ->
%    ?DEBUG("pts call: bucket ~p key ~p req ~p", [S#pts.name, Key, Req]),
%    case (catch send_msg_to_key(Tx, Key, Req, S)) of
%       {error, Reason} ->
%          plib:ack({error, Reason}),
%          {noreply, S};
%       _ ->
%          {noreply, S}
%    end;

handle_call(_, _Tx, S) ->
   {noreply, S}.

%%
%%
handle_cast(_, S) ->
   {noreply, S}.

%%
%%
handle_info({set_factory, _Sup}, #pts{nofactory=true}=S) ->
   {noreply, S};

handle_info({set_factory, Sup}, S) ->
   {ok, Pid} = pts_ns_sup:factory(Sup),
   {noreply, S#pts{factory = Pid}};

%%
%% put value
handle_info({put, Tx, {Key, Val}}, #pts{readonly=true}=S) ->
   ?DEBUG("pts put: bucket ~p key ~p val ~p", [S#pts.name, Key, Val]),
   plib:ack(Tx, {error, readonly}),
   {noreply, S};

handle_info({put, Tx, {Key, Val}}, #pts{immutable=true}=S) ->
   ?DEBUG("pts put: bucket ~p key ~p val ~p", [S#pts.name, Key, Val]),
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined ->
         {ok, Pid} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
         plib:relay(Pid, put, Tx, {Key, Val}),
         {noreply, S};
      _Pid ->
         plib:ack(Tx, {error, readonly}),
         {noreply, S}
   end;

handle_info({put, Tx, {Key, Val}}, S) ->
   ?DEBUG("pts put: bucket ~p key ~p val ~p", [S#pts.name, Key, Val]),
   Uid = key_to_uid(Key, S#pts.keylen), 
   case pns:whereis(S#pts.name, Uid) of
      undefined -> 
         {ok, Pid} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
         plib:relay(Pid, put, Tx, {Key, Val}),
         {noreply, S};
      Pid       -> 
         plib:relay(Pid, put, Tx, {Key, Val}),
         {noreply, S}
   end;

%%
%% get value
handle_info({get, Tx, Key}, #pts{rthrough=true}=S) ->
   ?DEBUG("pts get: bucket ~p key ~p", [S#pts.name, Key]),
   Uid = key_to_uid(Key, S#pts.keylen), 
   case pns:whereis(S#pts.name, Uid) of
      undefined -> 
         {ok, Pid} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
         plib:relay(Pid, get, Tx, Key),
         {noreply, S};
      Pid       ->
         plib:relay(Pid, get, Tx, Key),
         {noreply, S}
   end;

handle_info({get, Tx, Key}, S) ->
   ?DEBUG("pts get: bucket ~p key ~p", [S#pts.name, Key]),
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined -> 
         plib:ack(Tx, {error, not_found}),
         {noreply, S};
      Pid       -> 
         plib:relay(Pid, get, Tx, Key),
         {noreply, S}
   end;

%%
%% handle remove
handle_info({remove, Tx, Key}, #pts{readonly=true}=S) ->
   ?DEBUG("pts remove: bucket ~p key ~p", [S#pts.name, Key]),
   plib:ack(Tx, {error, readonly}),
   {noreply, S};

handle_info({remove, Tx, Key}, S) ->
   ?DEBUG("pts remove: bucket ~p key ~p", [S#pts.name, Key]),
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined -> 
         plib:ack(Tx, ok),
         {noreply, S};
      Pid       -> 
         plib:relay(Pid, remove, Tx, Key),
         {noreply, S}
   end;

handle_info({call, Tx, {Key, Req}}, S) ->
   ?DEBUG("pts call: bucket ~p key ~p req ~p", [S#pts.name, Key, Req]),
   case (catch send_msg_to_key(Tx, Key, Req, S)) of
      {error, Reason} ->
         plib:ack(Tx, {error, Reason}),
         {noreply, S};
      _ ->
         {noreply, S}
   end;


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
%% send message to process
send_msg_to_key(Tx, Key, Msg, #pts{rthrough=true}=S) ->
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined ->
         {ok, Pid} = supervisor:start_child(S#pts.factory, [S#pts.name, Uid]),
         plib:relay(Pid, '$gen_call', Tx, Msg),
         ok;
      Pid       -> 
         plib:relay(Pid, '$gen_call', Tx, Msg),
         ok
   end;

send_msg_to_key(Tx, Key, Msg, S) ->
   Uid = key_to_uid(Key, S#pts.keylen),
   case pns:whereis(S#pts.name, Uid) of
      undefined -> 
         {error, not_found};
      Pid    -> 
         plib:relay(Pid, '$gen_call', Tx, Msg),
         ok
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
