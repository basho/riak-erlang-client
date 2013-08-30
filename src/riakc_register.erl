%% -------------------------------------------------------------------
%%
%% riakc_counter: Eventually-consistent register type
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


%% @doc Encapsulates a register data-type, storing an opaque binary
%% value, with last-write wins semantics.
-module(riakc_register).
-behaviour(riakc_datatype).

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         context/1,
         is_type/1,
         type/0]).

%% Operations
-export([set/2]).

-record(register, {value :: binary(),
                   modified = false :: boolean()}).

-export_type([register/0]).
-opaque register() :: #register{}.

-type register_op() :: {assign, binary()}.

%% @doc Creates a new, empty register container type.
-spec new() -> register().
new() ->
    #register{value = <<>>}.

%% @doc Creates a new register with the specified value.
-spec new(binary()) -> register().
new(Value) when is_binary(Value) ->
    #register{value=Value}.

%% @doc Creates a new register with the specified value and context.
-spec new(binary(), riakc_datatype:context()) -> register().
new(Value, _Context) when is_binary(Value) ->
    #register{value=Value}.

%% @doc Extracts the value of the register.
-spec value(register()) -> boolean().
value(#register{value=V}) -> V.

%% @doc Extracts an operation from the register that can be encoded
%% into an update request.
-spec to_op(register()) -> register_op() | undefined.
to_op(#register{modified=false}) -> undefined;
to_op(#register{value=V, modified=true}) -> {assign, V}.

%% @doc Extracts the update context from the register.
-spec context(register()) -> riakc_datatype:context().
context(#register{}) -> undefined.

%% @doc Determines whether the passed term is a register container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, register).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> register.

%% @doc Sets the value of the register.
-spec set(register(), binary()) -> register().
set(#register{}=R, Value) when is_binary(Value) ->
    R#register{value=Value, modified=true}.
