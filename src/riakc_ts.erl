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

-export([query/2,
         query/3,
         put/3,
         put/4,
         get/4,
         delete/4]).

-include_lib("riak_pb/include/riak_kv_pb.hrl").

-type table_name() :: binary().
-type ts_value() :: number() | binary().


-spec query(Pid::pid(), Query::string()) ->
                   {ColumnNames::[binary()], Rows::[tuple()]} | {error, Reason::term()}.
%% @doc Execute a "SELECT ..." Query with client.  The result returned
%%      is a tuple containing a list of columns as binaries in the
%%      first element, and a list of records, each represented as a
%%      list of values, in the second element, or an @{error, Reason@}
%%      tuple.
query(Pid, QueryText) ->
    query(Pid, QueryText, []).

-spec query(Pid::pid(), Query::string(), Interpolations::[{binary(), binary()}]) ->
                   {ColumnNames::[binary()], Rows::[tuple()]} | {error, term()}.
%% @doc Execute a "SELECT ..." Query with client Pid, using
%%      Interpolations.  The result returned is a tuple containing a
%%      list of columns as binaries in the first element, and a list
%%      of records, each represented as a list of values, in the
%%      second element, or an @{error, Reason@} tuple.
query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).


-spec put(Pid::pid(), Table::table_name(), Data::[[ts_value()]]) ->
                 ok | {error, Reason::term()}.
%% @doc Make data records from Data and insert them, individually,
%%      into a time-series Table, using client Pid. Each record is a
%%      list of values of appropriate types for the complete set of
%%      table columns, in the order in which they appear in table's
%%      DDL.  On success, 'ok' is returned, else an @{error, Reason@}
%%      tuple.
%%
%%      Note: Type validation is done on the first record only.  If
%%      any subsequent record contains fewer or more elements than
%%      there are columns, or some element fails to convert to the
%%      appropriate type, the rest of the records will not get
%%      inserted.
put(Pid, TableName, Measurements) ->
    put(Pid, TableName, [], Measurements).

-spec put(Pid::pid(), Table::table_name(), Columns::[binary()], Data::[[ts_value()]]) ->
                 ok | {error, Reason::term()}.
%% @doc Make data records from Data and insert them, individually,
%%      into a time-series Table, using client Pid. Each record is a
%%      list of values of appropriate types for the complete set of
%%      table columns, in the order in which they appear in table's
%%      DDL.  On success, 'ok' is returned, else an @{error, Reason@}
%%      tuple.  Also @see put/3.
%%
%%      As of 2015-11-05, Columns parameter is ignored, the function
%%      expexts the full set of fields in each element of Data.
put(Pid, TableName, Columns, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, Columns, Measurements),
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
    Message = riak_pb_ts_codec:encode_tsdelreq(TableName, Key, Options),
    _Response = server_call(Pid, Message).


-spec get(Pid::pid(), Table::table_name(), Key::[ts_value()],
          Options::proplists:proplist()) ->
                 {Columns::[binary()], Record::[ts_value()]}.
%% @doc Get a record, if there is one, having the fields constituting
%%      the primary key in the Table equal to the composite Key
%%      (supplied as a list), using client Pid.  Options is a proplist
%%      which can include a value for 'timeout'.  Returns a tuple with
%%      a list of column names in its 1st element, and a record found
%%      as a list of values, further as a single element in enclosing
%%      list, in its 2nd element. If no record is found, the return
%%      value is {[], []}.
get(Pid, TableName, Key, Options) ->
    Message = riak_pb_ts_codec:encode_tsgetreq(TableName, Key, Options),
    case server_call(Pid, Message) of
        {error, {_NotFoundErrCode, <<"notfound">>}} ->
            {[], []};
        Response ->
            Columns = [C || #tscolumndescription{name = C} <- Response#tsgetresp.columns],
            Rows = [tuple_to_list(X) || X <- riak_pb_ts_codec:decode_rows(Response#tsgetresp.rows)],
            {Columns, Rows}
    end.


%% --------------------------------------------
%% local functions

-spec server_call(pid(), tuple()) -> term().
server_call(Pid, Message) ->
    gen_server:call(Pid,
                    {req, Message, riakc_pb_socket:default_timeout(timeseries)},
                    infinity).
