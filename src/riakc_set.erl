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

%% @doc <p>Encapsulates a set data-type. Riak's sets differ from Erlang
%% set types in several ways:</p>
%% <ul>
%% <li>Only binaries are allowed as elements. Convert other terms to a
%% binary before adding them.</li>
%% <li>Like the other eventually-consistent types, updates
%% (`add_element/2' and `del_element/2') are not applied to local
%% state. Instead, additions and removals are captured for later
%% application by Riak.</li>
%% <li>Additions and removals are non-exclusive. You can add and
%% remove the same element in the same session, both operations will
%% be performed in Riak (removal first). Removals performed without a
%% context may result in failure.</li>
%% <li>You may add an element that already exists in the original set
%% value, and remove an element that does not appear in the original
%% set value. This is non-intuitive, but acts as a safety feature: a
%% client code path that requires an element to be present in the set
%% (or removed) can ensure that intended state by applying an
%% operation.</li>
%% <li>The query functions `size/1', `is_element/1' and `fold/3' only
%% operate on the original value of the set, disregarding local
%% updates.</li>
%% </ul>
%% @end
-module(riakc_set).
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
-export([add_element/2,
         del_element/2]).

%% Query functions
-export([size/1,
         is_element/2,
         fold/3]).

-record(set, {value = ordsets:new() :: ordsets:ordset(binary()),
              adds = ordsets:new() :: ordsets:ordset(binary()),
              removes = ordsets:new() :: ordsets:ordset(binary()),
              context = undefined :: riakc_datatype:context() }).

-export_type([riakc_set/0, set_op/0]).
-opaque riakc_set() :: #set{}.

-type simple_set_op() :: {add_all, [binary()]} | {remove_all, [binary()]}.
-type set_op() :: simple_set_op() | {update, [simple_set_op()]}.

%% @doc Creates a new, empty set container type.
-spec new() -> riakc_set().
new() ->
    #set{}.

%% @doc Creates a new set container with the opaque context.
-spec new(riakc_datatype:context()) -> riakc_set().
new(Context) ->
    #set{context=Context}.

%% @doc Creates a new set container with the given members and opaque
%% context.
-spec new([binary()], riakc_datatype:context()) -> riakc_set().
new(Value, Context) when is_list(Value) ->
    #set{value=ordsets:from_list(Value),
         context=Context}.

%% @doc Returns the original value of the set as an ordset.
-spec value(riakc_set()) -> ordsets:ordset(binary()).
value(#set{value=V}) -> V.

%% @doc Extracts an operation from the set that can be encoded into an
%% update request.
-spec to_op(riakc_set()) -> riakc_datatype:update(set_op()).
to_op(#set{adds=[], removes=[]}) ->
    undefined;
to_op(#set{adds=A, removes=[], context=C}) ->
    {type(), {add_all, A}, C};
to_op(#set{adds=[], removes=R, context=C}) ->
    {type(), {remove_all, R}, C};
to_op(#set{adds=A, removes=R, context=C}) ->
    {type(), {update, [{remove_all, R}, {add_all, A}]}, C}.

%% @doc Determines whether the passed term is a set container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, set).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> set.

%% @doc Adds an element to the set.
-spec add_element(binary(), riakc_set()) -> riakc_set().
add_element(Bin, #set{adds=A0}=Set) when is_binary(Bin) ->
    Set#set{adds=ordsets:add_element(Bin, A0)}.

%% @doc Removes an element from the set.
%% @throws context_required
-spec del_element(binary(), riakc_set()) -> riakc_set().
del_element(_Bin, #set{context=undefined}) ->
    throw(context_required);
del_element(Bin, #set{removes=R0}=Set) when is_binary(Bin) ->
    Set#set{removes=ordsets:add_element(Bin, R0)}.

%% @doc Returns the cardinality (size) of the set. <em>Note: this only
%% operates on the original value as retrieved from Riak.</em>
-spec size(riakc_set()) -> pos_integer().
size(#set{value=V}) ->
    ordsets:size(V).

%% @doc Test whether an element is a member of the set. <em>Note: this
%% only operates on the original value as retrieved from Riak.</em>
-spec is_element(binary(), riakc_set()) -> boolean().
is_element(Bin, #set{value=V}) when is_binary(Bin) ->
    ordsets:is_element(Bin, V).

%% @doc Folds over the members of the set. <em>Note: this only
%% operates on the original value as retrieved from Riak.</em>
-spec fold(fun((binary(), term()) -> term()), term(), riakc_set()) -> term().
fold(Fun, Acc0, #set{value=V}) ->
    ordsets:fold(Fun, Acc0, V).

-ifdef(EQC).
gen_type() ->
    ?LET({Elems, Ctx}, {list(binary()), binary()}, new(Elems, Ctx)).

gen_op() ->
    {elements([add_element, del_element]),
     [binary()]}.
-endif.
