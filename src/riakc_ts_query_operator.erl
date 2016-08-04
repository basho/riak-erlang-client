%% -------------------------------------------------------------------
%%
%% riakc_ts_put_operator.erl: helper functions for query requests to Riak TS
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Helper functions for query requests to Riak TS

-module(riakc_ts_query_operator).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_pb/include/riak_ts_ttb.hrl").

-export([serialize/2,
         deserialize/1, deserialize/2]).


serialize(QueryText, Interpolations)
  when is_binary(QueryText) orelse is_list(QueryText) ->
    Q = riakc_utils:characters_to_unicode_binary(QueryText),
    Content = #tsinterpolation{
                 base           = Q,
                 interpolations = serialize_interpolations(Interpolations)},
    #tsqueryreq{'query' = Content}.

serialize_interpolations(Interpolations) ->
    serialize_interpolations(Interpolations, []).

serialize_interpolations([], SerializedInterps) ->
    SerializedInterps;
serialize_interpolations([{Key, Value} | RemainingInterps],
                         SerializedInterps) ->
    UpdatedInterps = [#rpbpair{key=Key, value=Value} | SerializedInterps],
    serialize_interpolations(RemainingInterps, UpdatedInterps).

deserialize(Response) ->
    deserialize(Response, false).

%% 2nd (boolean) argument indicates whether column types should be
%% included in the response. It's a bit silly that they aren't by
%% default, but that's an old oversight/decision that can't be
%% trivially changed without risking backwards compatibility.
deserialize({error, {Code, Message}}, _IncludeColumnTypes)
  when is_integer(Code), is_list(Message) ->
    {error, {Code, iolist_to_binary(Message)}};
deserialize({error, {Code, Message}}, _IncludeColumnTypes)
  when is_integer(Code), is_atom(Message) ->
    {error, {Code, iolist_to_binary(atom_to_list(Message))}};
deserialize({error, Message}, _IncludeColumnTypes) ->
    {error, Message};
deserialize(tsqueryresp, _Types) ->
    {ok, {[], []}};
deserialize({tsqueryresp, {ColumnNames, _ColumnTypes, Rows}}, false) ->
    {ok, {ColumnNames, Rows}};
deserialize({tsqueryresp, {ColumnNames, ColumnTypes, Rows}}, true) ->
    {ok, {lists:zip(ColumnNames, ColumnTypes), Rows}};
deserialize(#tsqueryresp{columns = C, rows = R}, false) ->
    ColumnNames = [ColName || #tscolumndescription{name = ColName} <- C],
    Rows = riak_pb_ts_codec:decode_rows(R),
    {ok, {ColumnNames, Rows}};
deserialize(#tsqueryresp{columns = C, rows = R}, true) ->
    Columns = [{ColName, ColType} || #tscolumndescription{name = ColName, type = ColType} <- C],
    Rows = riak_pb_ts_codec:decode_rows(R),
    {ok, {Columns, Rows}}.
