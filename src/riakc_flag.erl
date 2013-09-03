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

%% @doc Encapsulates a set data-type.
-module(riakc_flag).
-behaviour(riakc_datatype).

%% Callbacks
-export([new/0, new/2,
         value/1,
         to_op/1,
         context/1,
         is_type/1,
         type/0]).


%% Operations
-export([enable/1, disable/1]).

-record(flag, {value = false :: boolean(),
               modified = false :: boolean()}).

-export_type([flag/0]).
-opaque flag() :: #flag{}.

-type flag_op() :: enable | disable.

%% @doc Creates a new, empty flag container type.
-spec new() -> flag().
new() ->
    #flag{}.

%% @doc Creates a new flag with the specified value and context.
-spec new(boolean(), riakc_datatype:context()) -> flag().
new(Value, _Context) when is_boolean(Value) ->
    #flag{value=Value}.

%% @doc Extracts the value of the flag. true is enabled, false is disabled.
-spec value(flag()) -> boolean().
value(#flag{value=V}) -> V.

%% @doc Extracts an operation from the flag that can be encoded into
%% an update request.
-spec to_op(flag()) -> flag_op() | undefined.
to_op(#flag{modified=false}) -> undefined;
to_op(#flag{value=true, modified=true}) -> enable;
to_op(#flag{value=false, modified=true}) -> disable.

%% @doc Extracts the update context from the flag.
-spec context(flag()) -> riakc_datatype:context().
context(#flag{}) -> undefined.

%% @doc Determines whether the passed term is a flag container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, flag).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> flag.

%% @doc Enables the flag, setting its value to true.
-spec enable(flag()) -> flag().
enable(#flag{value=true}=F) -> F;
enable(#flag{value=false, modified=M}=F) -> 
    F#flag{value=true, modified=(not M)}.

%% @doc Disables the flag, setting its value to false.
-spec disable(flag()) -> flag().
disable(#flag{value=false}=F) -> F;
disable(#flag{value=true, modified=M}=F) -> 
    F#flag{value=false, modified=(not M)}.
