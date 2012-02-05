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
-module(cache).
-author(dmkolesnikov@gmail.com).

-export([
   start/0,
   dataset/1,
   b/1,
   b/3
]).

%%
%%
start() ->
   ets:new(ets_cache, [set, named_table, public]),
   ok = application:start(pts),
   ok = application:start(cache).
   
%%
%%
b(N) ->
   Set = dataset(N),
   [
      {[b(pts, write, Set), b(ets, write, Set)]},
      {[b(pts, seq_read, Set), b(ets, seq_read, Set)]},
      {[b(pts, read, Set), b(ets, read, Set)]}
   ].
  
%%
%%
b(pts, write, {_, Val, Seq}) ->   
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) ->
               ok = pts:put(cache, gen_key(X), Val)
            end,
            Seq
         )
      end,
      []
   ),
   {pts, write, Tio / 1000};

b(ets, write, {_, Val, Seq}) ->   
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) ->
               true = ets:insert(ets_cache, {gen_key(X), Val})
            end,
            Seq
         )
      end,
      []
   ),
   {ets, write, Tio / 1000};
   
   
b(pts, seq_read, {_N, _, Seq}) ->   
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) -> 
               %% NOTE: {key, 1312}, {key, 2040} cannot be found
               pts:get(cache, gen_key(X))
            end,
            Seq
         )
      end,
      []
   ),
   {pts, seq_read, Tio / 1000};

b(ets, seq_read, {_N, _, Seq}) ->   
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) ->
               ets:lookup(ets_cache, gen_key(X))
            end,
            Seq
         )
      end,
      []
   ),
   {ets, seq_read, Tio / 1000};   
   
b(pts, read, {_N, _, Seq}) ->   
   Seq1 = shuffle(Seq),
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) ->
               pts:get(cache, gen_key(X))
            end,
            Seq1
         )
      end,
      []
   ),
   {pts, read, Tio / 1000};   

b(ets, read, {_N, _, Seq}) ->   
   Seq1 = shuffle(Seq),
   {Tio,  _} = timer:tc(
      fun() ->
         lists:foreach(
            fun(X) ->
               ets:lookup(ets_cache, gen_key(X))
            end,
            Seq1
         )
      end,
      []
   ),
   {ets, read, Tio / 1000}.   
   
%%
%% generates a data set
dataset(N) ->   
   {N, gen_val(1), lists:seq(1, N)}.   

   
gen_key(X) ->
   {key, X}.

gen_val(X) ->
   Val = rnd_string(1024, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890"), 
   [
      {ttl, 60000},
      {ind, X},
      {val, Val}
   ].
   


%% from http://blog.teemu.im/2009/11/07/generating-random-strings-in-erlang/
rnd_string(Length, AllowedChars) ->
   lists:foldl(
      fun(_, Acc) ->
         [lists:nth(
            random:uniform(length(AllowedChars)),
            AllowedChars
         )] ++ Acc
      end, 
      [], 
      lists:seq(1, Length)
   ).
   
   
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% shuffle(List1) -> List2
%% Takes a list and randomly shuffles it. Relies on random:uniform
%% http://www.trapexit.org/RandomShuffle
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

shuffle(List) ->
%% Determine the log n portion then randomize the list.
   randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
   randomize(List);
randomize(T, List) ->
   lists:foldl(fun(_E, Acc) ->
                  randomize(Acc)
               end, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
   D = lists:map(fun(A) ->
                    {random:uniform(), A}
             end, List),
   {_, D1} = lists:unzip(lists:keysort(1, D)), 
   D1.
   