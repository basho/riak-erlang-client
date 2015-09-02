-module(riakc_ts_query_operator).

-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").

-export([serialize/2,
         deserialize/1]).

serialize(QueryText, Interpolations) ->
    Content = #tsinterpolation{
                 base = QueryText,
                 interpolations = serialize_interpolations(Interpolations)},
    #tsqueryreq{query = Content}.

serialize_interpolations(Interpolations) ->
    serialize_interpolations(Interpolations, []).

serialize_interpolations([], SerializedInterps) ->
    SerializedInterps;
serialize_interpolations([{Key, Value} | RemainingInterps],
                         SerializedInterps) ->
    UpdatedInterps = [#rpbpair{key=Key, value=Value} | SerializedInterps],
    serialize_interpolations(RemainingInterps, UpdatedInterps).

deserialize(Response) ->
    Response.
