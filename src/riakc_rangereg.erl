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

-module(riakc_rangereg).
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

-record(rangereg, {
          value :: undefined | fieldlist(),
          new_value = undefined :: undefined | integer()
         }).
-type field() :: min | max | first | last.
-type fieldlist() :: [{field(), integer()}].

-export_type([rangereg/0, rangereg_op/0]).
-opaque rangereg() :: #rangereg{}.
-type rangereg_op() :: {assign, integer()}.

-spec new() -> rangereg().
new() ->
    #rangereg{}.

-spec new(riakc_datatype:context()) -> rangereg().
new(_Ctx) ->
    #rangereg{}.

-spec new(fieldlist(), riakc_datatype:context()) -> rangereg().
new(Value, _Ctx) ->
    #rangereg{value=Value}.

-spec value(rangereg()) -> undefined | fieldlist().
value(#rangereg{value=Val}) ->
    Val.

-spec max(rangereg()) -> undefined | integer().
max(#rangereg{value=undefined}) -> undefined;
max(#rangereg{value=Val}) ->
    proplists:get_value(max, Val).

-spec min(rangereg()) -> undefined | integer().
min(#rangereg{value=undefined}) -> undefined;
min(#rangereg{value=Val}) ->
    proplists:get_value(min, Val).

-spec first(rangereg()) -> undefined | integer().
first(#rangereg{value=undefined}) -> undefined;
first(#rangereg{value=Val}) ->
    proplists:get_value(first, Val).

-spec last(rangereg()) -> undefined | integer().
last(#rangereg{value=undefined}) -> undefined;
last(#rangereg{value=Val}) ->
    proplists:get_value(last, Val).

-spec assign(integer(), rangereg()) -> rangereg().
assign(NewVal, MR) ->
    MR#rangereg{new_value=NewVal}.

-spec to_op(rangereg()) -> riakc_datatype:update(rangereg_op()).
to_op(#rangereg{new_value=undefined}) ->
    undefined;
to_op(#rangereg{new_value=NewVal}) ->
    {rangereg, {assign, NewVal}, undefined}.

-spec is_type(term()) -> bool.
is_type(T) -> is_record(T, rangereg).

-spec type() -> atom().
type() -> rangereg.

-ifdef(EQC).

gen_type() ->
    ?LET(Value, int(), new([{max, Value}, {min, Value-10}, {first, Value-10}, {last, Value}], undefined)).

gen_op() ->
    {assign,
     ?LET(NewVal, int(), [NewVal])
    }.

-endif.
