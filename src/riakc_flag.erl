%% -------------------------------------------------------------------
%%
%% riakc_flag: Eventually-consistent flag type
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

%% @doc Encapsulates a flag data-type. Flags are boolean values that
%% can be enabled or disabled. Like the other eventually-consistent
%% types, the original fetched value is unmodified by enabling or
%% disabling. Instead, the effective operation (`enable' or `disable')
%% is captured for later application in Riak. Note that it is
%% possible to `enable' a flag that is already `true' and `disable' a
%% flag that is already `false'. Flags are only available as values in
%% maps.
-module(riakc_flag).
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
-export([enable/1, disable/1]).

-record(flag, {value = false :: boolean(),
               op = undefined :: undefined | flag_op(),
               context = undefined :: riakc_datatype:context()}).

-export_type([flag/0, flag_op/0]).
-opaque flag() :: #flag{}.

-type flag_op() :: enable | disable.

%% @doc Creates a new, empty flag container type.
-spec new() -> flag().
new() ->
    #flag{}.

%% @doc Creates a new flag with the passed context.
-spec new(riakc_datatype:context()) -> flag().
new(Context) ->
    #flag{context=Context}.

%% @doc Creates a new flag with the specified value and context.
-spec new(boolean(), riakc_datatype:context()) -> flag().
new(Value, Context) when is_boolean(Value) ->
    #flag{value=Value, context=Context}.

%% @doc Extracts the original value of the flag. true is enabled,
%% false is disabled.
-spec value(flag()) -> boolean().
value(#flag{value=V}) -> V.

%% @doc Extracts an operation from the flag that can be encoded into
%% an update request.
-spec to_op(flag()) -> riakc_datatype:update(flag_op()).
to_op(#flag{op=undefined}) -> undefined;
to_op(#flag{op=O}) -> {type(), O, undefined}.

%% @doc Determines whether the passed term is a flag container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, flag).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> flag.

%% @doc Enables the flag, setting its value to true.
-spec enable(flag()) -> flag().
enable(#flag{}=F) -> F#flag{op=enable}.

%% @doc Disables the flag, setting its value to false.
%% @throws context_required
-spec disable(flag()) -> flag().
disable(#flag{context=undefined}) ->
    throw(context_required);
disable(#flag{}=F) -> F#flag{op=disable}.

-ifdef(EQC).
gen_type() ->
    ?LET(Flag, bool(), new(Flag, binary())).

gen_op() ->
    {elements([enable, disable]), []}.
-endif.
