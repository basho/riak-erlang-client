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


%% @doc Encapsulates a counter data-type.
-module(riakc_counter).
-behaviour(riakc_datatype).

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         context/1]).

%% Operations
-export([increment/1, increment/2,
         decrement/1, decrement/2]).


-record(counter, {
          value = 0 :: integer(),
          increment = 0 :: integer()
         }).

-export_type([counter/0]).
-opaque counter() :: #counter{}.

%% @doc Creates a new counter type with a value of 0.
-spec new() -> counter().
new() ->
    new(0).

%% @doc Creates a new counter type with the passed integer value.
-spec new(integer()) -> counter().
new(Value) when is_integer(Value) ->
    #counter{value=Value}.

%% @doc Creates a new counter type with the passed integer and
%% context.
-spec new(integer(), riakc_datatype:context()) -> counter().
new(Value, _Context) when is_integer(Value) ->
    #counter{value=Value}.

%% @doc Gets the current value of the counter.
-spec value(counter()) -> integer().
value(#counter{value=Value}) ->
    Value.

%% @doc Increments the counter by 1.
-spec increment(counter()) -> counter().
increment(Counter) ->
    increment(Counter, 1).

%% @doc Increments the counter by the passed amount.
-spec increment(counter(), integer()) -> counter().
increment(#counter{value=Value, increment=Incr}, Amount) when is_integer(Amount) ->
    #counter{value=Value+Amount, increment=Incr+Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(counter()) -> counter().
decrement(Counter) ->
    increment(Counter, -1).

%% @doc Decrements the counter by the passed amount.
-spec decrement(counter(), integer()) -> counter().
decrement(Counter, Amount) ->
    increment(Counter, -Amount).

%% @doc Extracts the changes to this counter as an operation.
-spec to_op(counter()) -> {increment, integer()} | undefined.
to_op(#counter{increment=Incr}) when Incr /= 0->
    {increment, Incr};
to_op(#counter{}) ->
    undefined.

%% @doc Extracts the context from this counter (always undefined).
-spec context(counter()) -> riakc_datatype:context().
context(#counter{}) ->
     undefined.
