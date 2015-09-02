-module(riakc_timeseries).

-export([query/2,
         query/3,
         put/3,
         put/4]).

query(Pid, QueryText) ->
    query(Pid, QueryText, []).

query(Pid, QueryText, Interpolations) ->
    Message = riakc_ts_query_operator:serialize(QueryText, Interpolations),
    Response = server_call(Pid, Message),
    riakc_ts_query_operator:deserialize(Response).

put(Pid, TableName, Measurements) ->
    put(Pid, TableName, undefined, Measurements).

put(Pid, TableName, Columns, Measurements) ->
    Message = riakc_ts_put_operator:serialize(TableName, Columns, Measurements),
    Response = server_call(Pid, Message),
    riakc_ts_put_operator:deserialize(Response).

server_call(Pid, Message) ->
    gen_server:call(Pid, 
                    {req, Message, riakc_pb_socket:default_timeout(timeseries)},
                    infinity).
