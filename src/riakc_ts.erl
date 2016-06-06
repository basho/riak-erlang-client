%% -------------------------------------------------------------------
%%
%% riakc_ts.erl: protobuf requests to Riak TS
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

%% @doc protobuf requests to Riak TS

-module(riakc_ts).

-export([query/2, query/3, query/4, query/5,
         get_coverage/3,
         replace_coverage/4, replace_coverage/5,
         put/3, put/4,
         get/4,
         delete/4,
         stream_list_keys/3,
         stream_query/2, stream_query/4]).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_pb/include/riak_ts_ttb.hrl").
-include("riakc.hrl").

-type table_name() :: string()|binary().
-type ts_value() :: riak_pb_ts_codec:ldbvalue().
-type ts_columnname() :: riak_pb_ts_codec:tscolumnname().

%% @doc Message types sent to the calling process of stream_query/4
-type stream_query_chunk() ::
    {rows, Continuation::binary(), ColumnNames::[binary()], Rows::[tuple()]}.
-type stream_result() ::
    {ReqId::non_neg_integer(), stream_query_chunk() | done}.

-export_type([stream_result/0, stream_query_chunk/0]).

-spec query(pid(), Query::string()|binary()) ->
            {ok, {ColumnNames::[ts_columnname()], Rows::[tuple()]}} | {error, Reason::term()}.
%% @equiv query/5
query(Pid, Query) ->
    query(Pid, Query, [], undefined, []).

-spec query(pid(), Query::string()|binary(), Interpolations::[{binary(), binary()}]) ->
            {ok, {ColumnNames::[binary()], Rows::[tuple()]}} | {error, term()}.
%% @equiv query/5
query(Pid, Query, Interpolations) ->
    query(Pid, Query, Interpolations, undefined, []).

-spec query(Pid::pid(),
            Query::string(),
            Interpolations::[{binary(), binary()}],
            Cover::term()) ->
            {ok, {ColumnNames::[binary()], Rows::[tuple()]}} | {error, term()}.
%% @equiv query/5
query(Pid, Query, Interpolations, Cover) ->
    query(Pid, Query, Interpolations, Cover, []).

-spec query(Pid::pid(),
            Query::string(),
            Interpolations::[{binary(), binary()}],
            Cover::term(),
            Options::proplists:proplist()) ->
            {ok, {ColumnNames::[binary()], Rows::[tuple()]}} | {error, term()}.
%% @doc Execute a Query.  The result returned
%%      is a tuple containing a list of columns as binaries in the
%%      first element, and a list of records, each represented as a
%%      list of values, in the second element, or an @{error, Reason@}
%%      tuple.
query(Pid, Query, Interpolations, undefined, Options) ->
    query_common(Pid, Query, Interpolations, undefined, Options);
query(Pid, Query, Interpolations, Cover, Options) when is_binary(Cover) ->
    query_common(Pid, Query, Interpolations, Cover, Options).

query_common(Pid, Query, Interpolations, Cover, Options)
  when is_pid(Pid), is_list(Query) ->
    Stream = false,
    Msg0 = riakc_ts_query_operator:serialize(Query, Interpolations, Stream),
    Msg1 = Msg0#tsqueryreq{cover_context = Cover},
    Msg = {Msg1, {msgopts, Options}},
    Response = server_call(Pid, Msg),
    riakc_ts_query_operator:deserialize(Response).

%% Convenience function for `stream_query/4'.
-spec stream_query(ClientPid::pid(), QueryString::string()) ->
    {ok, RequestId::non_neg_integer()}.
stream_query(Pid, Query) when is_pid(Pid),
                              is_list(Query) ->
    stream_query(Pid, Query, [], []).

%% @doc
%% Execute a streamed query. The immediate result
%% is a reference to the request ID. This will be used in messages
%% sent asynchronously from the socket process.
-spec stream_query(ClientPid::pid(),
                   QueryString::string(),
                   Interpolations::[{binary(), binary()}],
                   Options::proplists:proplist()) ->
    {ok, RequestId::non_neg_integer()}.
stream_query(Pid, Query, Interpolations, _Options) when is_pid(Pid),
                                                        is_list(Query),
                                                        is_list(Interpolations),
                                                        is_list(_Options) ->
    Stream = true,
    Msg = riakc_ts_query_operator:serialize(Query, Interpolations, Stream),
    ReqId = riakc_pb_socket:mk_reqid(),
    gen_server:call(
        Pid, {req, Msg, ?DEFAULT_PB_TIMEOUT, {ReqId, self()}}, infinity).

%% @doc Generate a parallel coverage plan for the specified query
-spec get_coverage(pid(), table_name(), QueryText::iolist()) ->
                          {ok, Entries::[term()]} | {error, term()}.
get_coverage(Pid, Table, Query) ->
    Message =
        #tscoveragereq{query = #tsinterpolation{base = iolist_to_binary(Query)},
                       replace_cover = undefined,
                       table = iolist_to_binary(Table)},
    case server_call(Pid, Message) of
        {ok, Entries} ->
            {ok, riak_pb_ts_codec:decode_cover_list(Entries)};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Replace a component of a parallel coverage plan
-spec replace_coverage(pid(), table_name(), QueryText::iolist(), Cover::binary()) ->
                              {ok, Entries::[term()]} | {error, term()}.
replace_coverage(Pid, Table, Query, Cover) ->
    replace_coverage(Pid, Table, Query, Cover, []).

-spec replace_coverage(pid(), table_name(), QueryText::iolist(), Cover::binary(),
                       OtherCover::list(binary())) ->
                              {ok, Entries::[term()]} | {error, term()}.
replace_coverage(Pid, Table, Query, Cover, Other) ->
    Message =
        #tscoveragereq{query = #tsinterpolation{base = iolist_to_binary(Query)},
                       replace_cover = Cover,
                       unavailable_cover = Other,
                       table = iolist_to_binary(Table)},
    case server_call(Pid, Message) of
        {ok, Entries} ->
            {ok, riak_pb_ts_codec:decode_cover_list(Entries)};
        {error, Reason} ->
            {error, Reason}
    end.


-spec put(pid(),
          table_name(),
          [[ts_value()]]) -> ok | {error, Reason::term()}.
%% @equiv put/4
put(Pid, Table, Measurements) ->
    put(Pid, Table, Measurements, []).

-spec put(pid(),
          table_name(),
          [[ts_value()]],
          Options::proplists:proplist()) -> ok | {error, Reason::term()}.
%% @doc Make data records from Data and insert them, individually,
%%      into a time-series Table, using client Pid. Each record is a
%%      list of values of appropriate types for the complete set of
%%      table column names, in the order in which they appear in table's
%%      DDL.  On success, 'ok' is returned, else an @{error, Reason@}
%%      tuple.
%%
%%      As of 2015-11-05, ColumnNames parameter is ignored, the function
%%      expects the full set of fields in each element of Data.
put(Pid, Table, Measurements, Options)
  when is_pid(Pid) andalso (is_binary(Table) orelse is_list(Table)) andalso
       is_list(Measurements) ->
    UseTTB = proplists:get_value(use_ttb, Options, true),
    Message = riakc_ts_put_operator:serialize(Table, Measurements, UseTTB),
    Msg = {Message, {msgopts, Options}},
    Response = server_call(Pid, Msg),
    riakc_ts_put_operator:deserialize(Response).


-spec delete(pid(), table_name(), Key::[ts_value()],
             Options::proplists:proplist()) ->
                    ok | {error, Reason::term()}.
%% @doc Delete a record, if there is one, having the fields
%%      constituting the primary key in the Table equal to the
%%      composite Key (given as a list), using client Pid.  Options is
%%      a proplist which can include values for 'vclock' and
%%      'timeout'.  Unless vclock is supplied, a get (@see get/4) is
%%      called in order to obtain one.
delete(Pid, Table, Key, Options)
  when is_pid(Pid), (is_binary(Table) orelse is_list(Table)),
       is_list(Key), is_list(Options) ->
    Message = #tsdelreq{table   = iolist_to_binary(Table),
                        key     = riak_pb_ts_codec:encode_cells_non_strict(Key),
                        vclock  = proplists:get_value(vclock, Options),
                        timeout = proplists:get_value(timeout, Options)},
    _Response = server_call(Pid, Message).


-spec get(pid(), table_name(), Key::[ts_value()],
          Options::proplists:proplist()) ->
                 {ok, {Columns::[binary()], Record::[ts_value()]}} |
                 {error, {ErrCode::integer(), ErrMsg::binary()}}.
%% @doc Get a record, if there is one, having the fields constituting
%%      the primary key in the Table equal to the composite Key
%%      (supplied as a list), using client Pid.  Options is a proplist
%%      which can include a value for 'timeout'.  Returns @{ok,
%%      @{Columns, Record@}@} where Columns has column names, and
%%      Record is the record found as a list of values, further as a
%%      single element in enclosing list. If no record is found, the
%%      return value is @{ok, @{[], []@}@}. On error, the function
%%      returns @{error, @{ErrCode, ErrMsg@}@}.
get(Pid, Table, Key, Options)
  when is_pid(Pid), (is_binary(Table) orelse is_list(Table)),
       is_list(Key), is_list(Options) ->
    UseTTB = proplists:get_value(use_ttb, Options, true),
    Msg0 = riakc_ts_get_operator:serialize(Table, Key, UseTTB),
    Msg1 = Msg0#tsgetreq{timeout = proplists:get_value(timeout, Options)},
    Msg = {Msg1, {msgopts, Options}},
    Response = server_call(Pid, Msg),
    riakc_ts_get_operator:deserialize(Response).


-spec stream_list_keys(pid(), table_name(), proplists:proplist()) ->
                              {ok, req_id()} | {error, term()}.
%% @doc Streaming lists keys in Table, using client Pid.  Parameter
%%      Options is a proplist that can include a value for
%%      'timeout'. Returns @{ok, ReqId@} or @{error, Reason@}.
stream_list_keys(Pid, Table, infinity) ->
    stream_list_keys(Pid, Table, [{timeout, undefined}]);
stream_list_keys(Pid, Table, Timeout) when is_integer(Timeout) ->
    stream_list_keys(Pid, Table, [{timeout, Timeout}]);
stream_list_keys(Pid, Table, Options)
  when is_pid(Pid), (is_binary(Table) orelse is_list(Table)), is_list(Options) ->
    ReqTimeout = proplists:get_value(timeout, Options),
    Req = #tslistkeysreq{table   = iolist_to_binary(Table),
                         timeout = ReqTimeout},
    ReqId = riakc_pb_socket:mk_reqid(),
    gen_server:call(Pid, {req, Req, ?DEFAULT_PB_TIMEOUT, {ReqId, self()}}, infinity).

%% --------------------------------------------
%% local functions

-spec server_call(pid(), tuple()) -> term().
server_call(Pid, Message) ->
    gen_server:call(Pid,
                    {req, Message, riakc_pb_socket:default_timeout(timeseries)},
                    infinity).
