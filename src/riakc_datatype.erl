%% -------------------------------------------------------------------
%%
%% riakc_datatype: Behaviour for eventually-consistent data-types
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

%% @doc When used with riakc_pb_socket:fetch_type() and
%% riakc_pb_socket:update_type(), modules implementing this behaviour
%% provide an internally consistent local view of the data while
%% capturing update operations for shipping back to the server.
-module(riakc_datatype).

-define(MODULES, [riakc_set, riakc_counter, riakc_flag, riakc_register, riakc_map]).

-export([module/1,
         module_for_term/1]).

-type maybe(T) :: T | undefined.
-type datatype() :: term().
-type typename() :: atom().
-type context() :: maybe(binary()).
-type update(T) :: maybe({typename(), T, context()}).

%% @doc Constructs a new, empty container for the type. Use this when
%% creating a new key.
-callback new() -> datatype().

%% @doc Constructs a new container for the type with the specified
%% value and opaque server-side context. This should only be used
%% internally by the client code.
-callback new(Value::term(), context()) -> datatype().

%% @doc Returns the original, unmodified value of the type. This does
%% not include the application of any locally-queued operations.
-callback value(datatype()) -> term().

%% @doc Returns a version of the value with locally-queued operations
%% applied.
-callback dirty_value(datatype()) -> term().

%% @doc Extracts an operation from the container that can be encoded
%% into an update request. 'undefined' should be returned if the type
%% is unmodified. This should be passed to
%% riakc_pb_socket:update_type() to submit modifications.
-callback to_op(datatype()) -> update(term()).

%% @doc Determines whether the given term is the type managed by the
%% container module.
-callback is_type(datatype()) -> boolean().

%% @doc Determines the symbolic name of the container's type, e.g.
%% set, map, counter.
-callback type() -> typename().

%% @doc Returns the module that is a container for the given abstract
%% type.
-spec module(Type::atom()) -> module().
module(set)      -> riakc_set;
module(counter)  -> riakc_counter;
module(flag)     -> riakc_flag;
module(register) -> riakc_register;
module(map)      -> riakc_map.

%% @doc Returns the appropriate container module for the given term,
%% if possible.
-spec module_for_term(datatype()) -> maybe(module()).
module_for_term(T) ->
    lists:foldl(fun(Mod, undefined) ->
                        case Mod:is_type(T) of
                            true -> Mod;
                            false -> undefined
                        end;
                   (_, Mod) ->
                        Mod
                end, undefined, ?MODULES).
