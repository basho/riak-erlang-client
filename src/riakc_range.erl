%% -------------------------------------------------------------------
%%
%% riakc_rangereg: 
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

-module(riakc_range).
-behaviour(riakc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_type/0, gen_op/0]).
-endif.

-export([new/0, new/1, new/2,
         value/1,
         to_op/1,
         is_type/1,
         type/0]).

-export([max/1, min/1, first/1, last/1,
         assign/2]).

-record(range, {
          value :: undefined | fieldlist(),
          new_value = undefined :: undefined | integer()
         }).
-type field() :: min | max | first | last.
-type fieldlist() :: [{field(), integer()}].

-export_type([range/0, range_op/0]).
-opaque range() :: #range{}.
-type range_op() :: {assign, integer()}.

-spec new() -> range().
new() ->
    #range{}.

-spec new(riakc_datatype:context()) -> range().
new(_Ctx) ->
    #range{}.

-spec new(fieldlist(), riakc_datatype:context()) -> range().
new(Value, _Ctx) ->
    #range{value=Value}.

-spec value(range()) -> undefined | fieldlist().
value(#range{value=Val}) ->
    Val.

-spec max(range()) -> undefined | integer().
max(#range{value=undefined}) -> undefined;
max(#range{value=Val}) ->
    proplists:get_value(max, Val).

-spec min(range()) -> undefined | integer().
min(#range{value=undefined}) -> undefined;
min(#range{value=Val}) ->
    proplists:get_value(min, Val).

-spec first(range()) -> undefined | integer().
first(#range{value=undefined}) -> undefined;
first(#range{value=Val}) ->
    proplists:get_value(first, Val).

-spec last(range()) -> undefined | integer().
last(#range{value=undefined}) -> undefined;
last(#range{value=Val}) ->
    proplists:get_value(last, Val).

-spec assign(integer(), range()) -> range().
assign(NewVal, MR) ->
    MR#range{new_value=NewVal}.

-spec to_op(range()) -> riakc_datatype:update(range_op()).
to_op(#range{new_value=undefined}) ->
    undefined;
to_op(#range{new_value=NewVal}) ->
    {type(), {add, NewVal}, undefined}.

-spec is_type(term()) -> boolean().
is_type(T) -> is_record(T, range).

-spec type() -> atom().
type() -> range.

-ifdef(EQC).

gen_type() ->
    ?LET(Value, int(), new([{max, Value}, {min, Value-10}, {first, Value-10}, {last, Value}], undefined)).

gen_op() ->
    {assign,
     ?LET(NewVal, int(), [NewVal])
    }.

-endif.
