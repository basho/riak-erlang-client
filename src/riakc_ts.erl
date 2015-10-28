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


-spec query(pid(), string()) ->
                   {[binary()], [tuple()]} | {error, term()}.
%% @doc Execute a SELECT query given in a string (2nd argument) with
%%      client Pid (1st arg).  The result returned is a tuple
%%      containing a list of columns as binaries in the first element,
%%      and a list of records, each represented as a list of values,
%%      in the second element.
query(Pid, QueryText) ->
    query(Pid, QueryText, []).

-spec query(pid(), string(), [{binary(), binary()}]) ->
                   {[binary()], [tuple()]} | {error, term()}.
%% @doc Execute a SELECT query given in a string (2nd argument) with
%%      client Pid (1st arg), using interpolations (3rd arg). The
%%      result returned is a tuple containing a list of columns as
%%      binaries in the first element, and a list of records, each
%%      represented as a list of values, in the second element.
%%
%%      Note that for the 'float' field type, the actual values
%%      returned will always be of type double (64-bit), even when the
%%      data were previously inserted (possibly by some other client)
%%      in `#tscell.float_value`.
query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).


-spec put(pid(), table_name(), [{binary(), ts_value()}]) ->
                 ok | {error, term()}.
%% @doc Make data records from a list (3rd arg) and insert them, one
%%      by one, into a TS table (2nd arg), using client Pid (1st
%%      arg). Each record is a list of values of appropriate types for
%%      the complete set of table columns, in the order in which they
%%      appear in table's DDL. Type validation is done on the first
%%      record only. If any subsequent record contains fewer or more
%%      elements than there are columns, or some element fails to
%%      convert to the appropriate type, the rest of the records will
%%      not get inserted.
put(Pid, TableName, Measurements) ->
    put(Pid, TableName, [], Measurements).

-spec put(pid(), table_name(), [binary()], [{binary(), ts_value()}]) ->
                 ok | {error, term()}.
%% @doc Make data records from a list (4th arg) and insert them, one
%%      by one, into a TS table (2nd arg), using client Pid (1st
%%      arg). Each record is a list of values of types appropriate for
%%      the fields mentioned in the column list (3rd arg), in that
%%      order. Type validation is done on the first record only. If
%%      any subsequent record contains fewer or more elements than
%%      there are columns, or some element fails to convert to the
%%      appropriate type, the rest of the records will not get
%%      inserted.
put(Pid, TableName, Columns, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, Columns, Measurements),
    Response = server_call(Pid, Message),
    riakc_ts_put_operator:deserialize(Response).


-spec delete(pid(), table_name(), [ts_value()], proplists:proplist()) ->
                    ok | {error, term()}.
%% @doc Delete a record, if there is one, having the fields
%%      constituting the primary key in the table (2nd arg) equal to
%%      the composite key (3rd arg, supplied as a list), using client
%%      Pid (1st arg).  Options (4th arg) is a proplist which can
%%      include values for 'vclock' and 'timeout'. Unless vclock is
%%      supplied, a get is called in order to obtain one.
delete(Pid, TableName, Key, Options)
  when is_list(Key) ->
    Message = riak_pb_ts_codec:encode_tsdelreq(TableName, Key, Options),
    _Response = server_call(Pid, Message).


-spec get(pid(), table_name(), [ts_value()], proplists:proplist()) ->
                 {[binary()], [[ts_value()]]}.
%% @doc Get a record, if there is one, having the fields constituting
%%      the primary key in the table (2nd arg) equal to the composite
%%      key (3rd arg, supplied as a list), using client Pid (1st arg).
%%      Options (4th arg) is a proplist which can include a value for
%%      'timeout'. Returns a tuple with a list of column names in its
%%      1st element, and a record found as a list of values, further
%%      as a single element in enclosing list, in its 2nd element. If
%%      no record is found, the return value is {[], []}.
%%
%%      Note that for the 'float' field type, the actual values
%%      returned will always be of type double (64-bit), even when the
%%      data were previously inserted (possibly by some other client)
%%      in `#tscell.float_value`.
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
