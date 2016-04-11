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

-export([query/2, query/3, query/4,
         get_coverage/3,
         put/3, put/4,
         get/4,
         delete/4,
         stream_list_keys/3,
         convert_to_binary/1,
         convert_list_to_binary/1]).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_pb/include/riak_ts_ttb.hrl").
-include("riakc.hrl").

-type table_name() :: binary().
-type ts_value() :: riak_pb_ts_codec:ldbvalue().
-type ts_columnname() :: riak_pb_ts_codec:tscolumnname().

-spec query(Pid::pid(), Query::string()) ->
                   {ColumnNames::[ts_columnname()], Rows::[tuple()]} | {error, Reason::term()}.
%% @doc Execute a "SELECT ..." Query with client.  The result returned
%%      is a tuple containing a list of columns as binaries in the
%%      first element, and a list of records, each represented as a
%%      list of values, in the second element, or an @{error, Reason@}
%%      tuple.
%%

query(Pid, QueryText) ->
    query(Pid, QueryText, []).

-spec query(Pid::pid(), Query::string(), Interpolations::[{binary(), binary()}]) ->
                   {ColumnNames::[binary()], Rows::[tuple()]} | {error, term()}.
%% @doc Execute a "SELECT ..." Query with client.  The result returned
%%      is a tuple containing a list of columns as binaries in the
%%      first element, and a list of records, each represented as a
%%      list of values, in the second element, or an @{error, Reason@}
%%      tuple.
%%

query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).

-spec query(Pid::pid(), Query::string(), Interpolations::[{binary(), binary()}], Cover::term()) ->
                   {ColumnNames::[binary()], Rows::[tuple()]} | {error, term()}.
%% @doc Execute a "SELECT ..." Query with client.  The result returned
%%      is a tuple containing a list of columns as binaries in the
%%      first element, and a list of records, each represented as a
%%      list of values, in the second element, or an @{error, Reason@}
%%      tuple.

query(Pid, QueryText, Interpolations, Cover) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message#tsqueryreq{cover_context=Cover}),
    riakc_ts_query_operator:deserialize(Response).

%% @doc Generate a parallel coverage plan for the specified query
-spec get_coverage(Pid :: pid(),
                   Table :: binary(),
                   QueryText :: binary()) -> {ok, any()} | {'EXIT', any()}.
get_coverage(Pid, Table, QueryText) ->
    server_call(Pid,
                #tscoveragereq{query = #tsinterpolation{base=riakc_ts:convert_to_binary(QueryText)},
                               replace_cover=undefined,
                               table = riakc_ts:convert_to_binary(Table)}).

-spec put(Pid::pid(), Table::table_name(), [[ts_value()]]) ->
                 ok | {error, Reason::term()}.
%% @doc Make data records from Data and insert them, individually,
%%      into a time-series Table, using client Pid. Each record is a
%%      list of values of appropriate types for the complete set of
%%      table column names, in the order in which they appear in table's
%%      DDL.  On success, 'ok' is returned, else an @{error, Reason@}
%%      tuple.  Also @see put/3.
%%
%%      As of 2015-11-05, ColumnNames parameter is ignored, the function
%%      expects the full set of fields in each element of Data.
%%

put(Pid, TableName, Measurements) ->
    put(Pid, TableName, [], Measurements).

-spec put(Pid::pid(), Table::table_name(), ColumnNames::[ts_columnname()], Data::[[ts_value()]]) ->
                 ok | {error, Reason::term()}.
%% @doc Make data records from Data and insert them, individually,
%%      into a time-series Table, using client Pid. Each record is a
%%      list of values of appropriate types for the complete set of
%%      table column names, in the order in which they appear in table's
%%      DDL.  On success, 'ok' is returned, else an @{error, Reason@}
%%      tuple.  
%%
%%      As of 2015-11-05, ColumnNames parameter is ignored, the function
%%      expects the full set of fields in each element of Data.
%%

put(Pid, TableName, ColumnNames, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, ColumnNames, Measurements),
    Response = server_call(Pid, Message),
    riakc_ts_put_operator:deserialize(Response).

-spec delete(Pid::pid(), Table::table_name(), Key::[ts_value()],
             Options::proplists:proplist()) ->
                    ok | {error, Reason::term()}.
%% @doc Delete a record, if there is one, having the fields
%%      constituting the primary key in the Table equal to the
%%      composite Key (given as a list), using client Pid.  Options is
%%      a proplist which can include values for 'vclock' and
%%      'timeout'.  Unless vclock is supplied, a get (@see get/4) is
%%      called in order to obtain one.
delete(Pid, TableName, Key, Options)
  when is_list(Key) ->
    Message = #tsdelreq{table   = TableName,
                        key     = riak_pb_ts_codec:encode_cells_non_strict(Key),
                        vclock  = proplists:get_value(vclock, Options),
                        timeout = proplists:get_value(timeout, Options)},
    _Response = server_call(Pid, Message).


-spec get(Pid::pid(), Table::table_name(), Key::[ts_value()],
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
get(Pid, TableName, Key, Options) ->
    Message = #tsgetreq{table   = TableName,
                        key     = Key,
                        timeout = proplists:get_value(timeout, Options)},

    case server_call(Pid, Message) of
        {error, {_NotFoundErrCode, <<"notfound">>}} ->
            {ok, {[], []}};
        {error, OtherError} ->
            {error, OtherError};
        Response ->
            Columns = Response#tsgetresp.columns,
            Rows = Response#tsgetresp.rows,
            {ok, {Columns, Rows}}
    end.


-spec stream_list_keys(pid(), table_name(), proplists:proplist()) ->
                              {ok, req_id()} | {error, term()}.
%% @doc Streaming lists keys in Table, using client Pid.  Parameter
%%      Options is a proplist that can include a value for
%%      'timeout'. Returns @{ok, ReqId@} or @{error, Reason@}.
stream_list_keys(Pid, Table, infinity) ->
    stream_list_keys(Pid, Table, [{timeout, undefined}]);
stream_list_keys(Pid, Table, Timeout) when is_integer(Timeout) ->
    stream_list_keys(Pid, Table, [{timeout, Timeout}]);
stream_list_keys(Pid, Table, Options) ->
    ReqTimeout = proplists:get_value(timeout, Options),
    Req = #tslistkeysreq{table = Table,
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

-spec convert_to_binary(Value :: binary() | list()) -> binary().
convert_to_binary(Value) when is_binary(Value) ->
    Value;
convert_to_binary(Value) when is_list(Value) ->
    list_to_binary(Value).

-spec convert_list_to_binary(Value :: [binary()] | [list()]) -> binary().
convert_list_to_binary(Values) ->
    list:map(fun convert_to_binary/1, Values).
