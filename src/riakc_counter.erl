%% -------------------------------------------------------------------
%%
%% riakc_counter: Eventually-consistent counter type
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


%% @doc Encapsulates a counter data-type. Counters are integers that
%% can be incremented or decremented. Like the other
%% eventually-consistent types, the original fetched value is
%% unmodified by increments. Instead, increments are captured for
%% later application in Riak.
-module(riakc_counter).
-behaviour(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).
-endif.

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         is_type/1,
         type/0]).

%% Operations
-export([increment/1, increment/2,
         decrement/1, decrement/2]).


-record(counter, {
          value = 0 :: integer(),
          increment = undefined :: undefined | integer()
         }).

-export_type([counter/0, counter_op/0]).
-opaque counter() :: #counter{}.
-type counter_op() :: {increment, integer()}.

%% @doc Creates a new counter type with a value of 0.
-spec new() -> counter().
new() ->
    #counter{}.

%% @doc Creates a new counter type with the passed context. It's
%% ignored, but we need this constructor for new nested (in maps)
%% objects on the fly
-spec new(riakc_datatype:context()) -> counter().
new(_Context) ->
    #counter{}.

%% @doc Creates a new counter type with the passed integer and
%% context.
-spec new(integer(), riakc_datatype:context()) -> counter().
new(Value, _Context) when is_integer(Value) ->
    #counter{value=Value}.

%% @doc Gets the original value of the counter.
-spec value(counter()) -> integer().
value(#counter{value=Value}) ->
    Value.

%% @doc Increments the counter by 1.
-spec increment(counter()) -> counter().
increment(Counter) ->
    increment(1, Counter).

%% @doc Increments the counter by the passed amount.
-spec increment(integer(), counter()) -> counter().
increment(Amount, #counter{increment=undefined}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Amount};
increment(Amount, #counter{increment=Incr}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Incr+Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(counter()) -> counter().
decrement(Counter) ->
    increment(-1, Counter).

%% @doc Decrements the counter by the passed amount.
-spec decrement(integer(), counter()) -> counter().
decrement(Amount, Counter) ->
    increment(-Amount, Counter).

%% @doc Extracts the changes to this counter as an operation.
-spec to_op(counter()) -> riakc_datatype:update(counter_op()).
to_op(#counter{increment=undefined}) ->
    undefined;
to_op(#counter{increment=Incr}) ->
    {type(), {increment, Incr}, undefined}.

%% @doc Determines whether the passed term is a counter container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, counter).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> counter.

-ifdef(EQC).
gen_type() ->
    ?LET(Count, int(), new(Count, undefined)).

gen_op() ->
    {elements([increment, decrement]),
     weighted_default({1, []},
                      {5, [?SUCHTHAT(X, int(), X /= 0)]})}.
-endif.
