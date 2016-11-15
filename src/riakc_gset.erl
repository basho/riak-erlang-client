%% -------------------------------------------------------------------
%%
%% riakc_gset: Eventually-consistent set type
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc <p>Encapsulates a gset data-type. Riak's gsets differ from Erlang
%% set types in several ways:</p>
%% <ul>
%% <li>Only binaries are allowed as elements. Convert other terms to a
%% binary before adding them.</li>
%% <li>Like the other eventually-consistent types, update %% (`add_element/2')
%% is not applied to local state. Instead, additions are captured for later
%% application by Riak.</li>
%% <li>You may add an element that already exists in the original set
%% value.</li>
%% <li>The query functions `size/1', `is_element/1' and `fold/3' only
%% operate on the original value of the set, disregarding local
%% updates.</li>
%% </ul>
%% @end
-module(riakc_gset).
-behaviour(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_type/0, gen_op/0]).
-endif.

%% Callbacks
-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         is_type/1,
         type/0]).

%% Operations
-export([add_element/2]).

%% Query functions
-export([size/1,
         is_element/2,
         fold/3]).

-record(gset, {value = ordsets:new() :: ordsets:ordset(binary()),
              adds = ordsets:new() :: ordsets:ordset(binary()),
              context = undefined :: riakc_datatype:context() }).

-export_type([riakc_gset/0, gset_op/0]).
-opaque riakc_gset() :: #gset{}.

-type simple_gset_op() :: {add_all, [binary()]}.
-type gset_op() :: simple_gset_op() | {update, [simple_gset_op()]}.

%% @doc Creates a new, empty set container type.
-spec new() -> riakc_gset().
new() ->
    #gset{}.

%% @doc Creates a new sset container with the opaque context.
-spec new(riakc_datatype:context()) -> riakc_gset().
new(Context) ->
    #gset{context=Context}.

%% @doc Creates a new gset container with the given members and opaque
%% context.
-spec new([binary()], riakc_datatype:context()) -> riakc_gset().
new(Value, Context) when is_list(Value) ->
    #gset{value=ordsets:from_list(Value),
         context=Context}.

%% @doc Returns the original value of the set as an ordset.
-spec value(riakc_gset()) -> ordsets:ordset(binary()).
value(#gset{value=V}) -> V.

%% @doc Extracts an operation from the gset that can be encoded into an
%% update request.
-spec to_op(riakc_gset()) -> riakc_datatype:update(gset_op()).
to_op(#gset{adds=[]}) ->
    undefined;
to_op(#gset{adds=[A|AT], context=C}) ->
    {type(), {add_all, [A|AT]}, C};
to_op(#gset{adds=A, context=C}) ->
    {type(), {update, [{add_all, [A]}]}, C}.

%% @doc Determines whether the passed term is a gset container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, gset).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> gset.

%% @doc Adds an element to the set.
-spec add_element(binary(), riakc_gset()) -> riakc_gset().
add_element(Bin, #gset{adds=A0}=Set) when is_binary(Bin) ->
    Set#gset{adds=ordsets:add_element(Bin, A0)}.

%% @doc Returns the cardinality (size) of the set. <em>Note: this only
%% operates on the original value as retrieved from Riak.</em>
-spec size(riakc_gset()) -> pos_integer().
size(#gset{value=V}) ->
    ordsets:size(V).

%% @doc Test whether an element is a member of the set. <em>Note: this
%% only operates on the original value as retrieved from Riak.</em>
-spec is_element(binary(), riakc_gset()) -> boolean().
is_element(Bin, #gset{value=V}) when is_binary(Bin) ->
    ordsets:is_element(Bin, V).

%% @doc Folds over the members of the set. <em>Note: this only
%% operates on the original value as retrieved from Riak.</em>
-spec fold(fun((binary(), term()) -> term()), term(), riakc_gset()) -> term().
fold(Fun, Acc0, #gset{value=V}) ->
    ordsets:fold(Fun, Acc0, V).

-ifdef(EQC).
gen_type() ->
    ?LET({Elems, Ctx}, {list(binary()), binary()}, new(Elems, Ctx)).

gen_op() ->
    {elements([add_element]),
     [binary()]}.
-endif.
