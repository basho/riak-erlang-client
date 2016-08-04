%% -------------------------------------------------------------------
%%
%% riakc_utils: erlang client utils
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

-module(riakc_utils).

-export([wait_for_list/1, characters_to_unicode_binary/1]).

-spec wait_for_list(non_neg_integer()) -> {ok, list()} | {error, any()}.
%% @doc Wait for the results of a listing operation
wait_for_list(ReqId) ->
    wait_for_list(ReqId, []).
wait_for_list(ReqId, Acc) ->
    receive
        {ReqId, done} -> {ok, lists:flatten(Acc)};
        {ReqId, {error, Reason}} -> {error, Reason};
        {ReqId, {_, Res}} -> wait_for_list(ReqId, [Res|Acc])
    end.

-spec characters_to_unicode_binary(string()|binary()) -> binary().
%% @doc Convert to unicode binary with informative errors
%% @throws {unicode_error, ErrMsg}
characters_to_unicode_binary(String) ->
    case unicode:characters_to_binary(String) of
        {incomplete, Encoded, Rest} ->
            ErrMsg = lists:flatten(io_lib:format("Incomplete unicode data provided. Encoded: ~p Rest: ~p", [Encoded, Rest])),
            throw({unicode_error, ErrMsg});
        {error, Encoded, Rest} ->
            ErrMsg = lists:flatten(io_lib:format("Unicode encoding error. Encoded: ~p Rest: ~p", [Encoded, Rest])),
            throw({unicode_error, ErrMsg});
        Binary ->
            Binary
    end.
