%% -------------------------------------------------------------------
%%
%% riakc_set: Eventually-consistent hyperloglog(set) type
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


%%@doc
-module(riakc_hll).
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
-export([add_element/2, add_elements/2, card/1]).

-record(hll, {value = 0 :: number(),
              adds = ordsets:new() :: ordsets:ordset(binary())}).

-export_type([riakc_hll/0, hll_op/0]).
-opaque riakc_hll() :: #hll{}.

-type hll_op() :: {add_all, [binary()]}.

%% @doc Creates a new, empty hll type.
-spec new() -> riakc_hll().
new() ->
    #hll{}.

%% @doc Creates a new hll
-spec new(riakc_datatype:context()) -> riakc_hll().
new(_Context) ->
    #hll{}.

%% @doc Creates a new hll
-spec new(number(), riakc_datatype:context()) -> riakc_hll().
new(Value, _Context) ->
    #hll{value=Value}.

%% @doc Returns the
-spec value(riakc_hll()) -> number().
value(#hll{value=Value}) -> Value.

%% @doc Same as value, but better for users.
-spec card(riakc_hll()) -> number().
card(Hll) ->
    value(Hll).

%% @doc Extracts an operation from the hll that can be encoded into an
%% update request.
-spec to_op(riakc_hll()) -> riakc_datatype:update(hll_op()).
to_op(#hll{adds=[]}) ->
    undefined;
to_op(#hll{adds=A}) ->
    {type(), {add_all, A}, undefined}.

%% @doc Determines whether the passed term is a hll type.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, hll).

%% @doc Returns the symbolic name of this type.
-spec type() -> atom().
type() -> hll.

%% @doc Adds elements to the hll(set).
-spec add_elements(list(binary()), riakc_hll()) -> riakc_hll().
add_elements(Elems, Hll) when is_list(Elems) ->
    lists:foldl(fun add_element/2, Hll, Elems).

%% @doc Adds an element to the hll(set).
-spec add_element(binary(), riakc_hll()) -> riakc_hll().
add_element(Elem, #hll{adds=A0}=Hll) when is_binary(Elem) ->
    Hll#hll{adds=ordsets:add_element(Elem, A0)}.

-ifdef(EQC).
gen_type() ->
    ?LET(Elems, list(binary()), new(Elems, undefined)).

gen_op() ->
    oneof([
           {add_element, [binary()]},
           {add_elements, [non_empty(list(binary()))]}
          ]).

-endif.

