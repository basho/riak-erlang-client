%% -------------------------------------------------------------------
%%
%% riakc_set: Eventually-consistent set type
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
-module(riakc_set).
-behaviour(riakc_datatype).

%% Callbacks
-export([new/0, new/2,
         value/1,
         to_op/1,
         context/1,
         is_type/1,
         type/0]).

%% Operations
-export([add_element/2,
         del_element/2]).

%% Query functions
-export([size/1,
         is_element/2,
         fold/3]).

-record(set, {value = ordsets:new() :: ordsets:ordset(binary()),
              adds = [] :: [binary()],
              removes = [] :: [binary()],
              context = undefined :: riakc_datatype:context() }).

-export_type([set/0]).
-opaque set() :: #set{}.

-type simple_set_op() :: {add_all, [binary()]} | {remove_all, [binary()]}.
-type set_op() :: simple_set_op() | {update, [simple_set_op()]}.

%% @doc Creates a new, empty set container type.
-spec new() -> set().
new() ->
    #set{}.

%% @doc Creates a new set container with the given members and opaque
%% context.
-spec new([binary()], riakc_datatype:context()) -> set().
new(Value, Context) when is_list(Value) ->
    #set{value=ordsets:from_list(Value),
         context=Context}.

%% @doc Returns the value of the set as an ordset.
-spec value(set()) -> ordsets:set(binary()).
value(#set{value=V}) -> V.

%% @doc Extracts an operation from the set that can be encoded into an
%% update request.
-spec to_op(set()) -> set_op() | undefined.
to_op(#set{adds=[], removes=[]}) ->
    undefined;
to_op(#set{adds=A, removes=[]}) ->
    {add_all, A};
to_op(#set{adds=[], removes=R}) ->
    {remove_all, R};
to_op(#set{adds=A, removes=R}) ->
    {update, [{add_all, A}, {remove_all, R}]}.

%% @doc Extracts the update context from the set container.
-spec context(set()) -> riakc_datatype:context().
context(#set{context=C}) -> C.

%% @doc Determines whether the passed term is a set container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, set).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> set.

%% @doc Adds an element to the set.
-spec add_element(binary(), set()) -> set().
add_element(Bin, #set{value=V0, adds=A0, removes=R0}=Set) when is_binary(Bin) ->
    case is_element(Bin, Set) of
        true -> Set;
        false ->
            {A, R} = case lists:member(Bin, R0) of
                         true ->
                             {A0, lists:delete(Bin, R0)};
                         false ->
                             {[Bin|A0], R0}
                     end,
            Set#set{value=ordsets:add_element(Bin, V0),
                    adds=A, removes=R}
    end.

%% @doc Removes an element from the set.
-spec del_element(binary(), set()) -> set().
del_element(Bin, #set{value=V0, adds=A0, removes=R0}=Set) when is_binary(Bin) ->
    case is_element(Bin, Set) of
        false -> Set;
        true ->
            {A, R} = case lists:member(Bin, A0) of
                         true ->
                             {lists:delete(Bin,A0), R0};
                         false ->
                             {A0, [Bin|R0]}
                     end,
            Set#set{value=ordsets:add_element(Bin, V0),
                    adds=A, removes=R}
    end.

%% @doc Returns the cardinality (size) of the set.
-spec size(set()) -> pos_integer().
size(#set{value=V}) ->
    ordsets:size(V).

%% @doc Test whether an element is a member of the set.
-spec is_element(binary(), set()) -> boolean().
is_element(Bin, #set{value=V}) when is_binary(Bin) ->
    ordsets:is_element(Bin, V).

%% @doc Folds over the members of the set.
-spec fold(fun((binary(), term()) -> term()), term(), set()) -> term().
fold(Fun, Acc0, #set{value=V}) ->
    ordsets:fold(Fun, Acc0, V).
