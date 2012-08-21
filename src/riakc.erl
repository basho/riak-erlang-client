-module(riakc).

-export([start/0, stop/0, conn_count/0, execute/1]).

%%----------------------------------------------------------------------
%% @doc  Given a fun that contains a riakc_pb_socket call, use one of
%%       the enqueued connections.
%% 
%% @spec execute(Fun::fun()) -> term()
%% @end
%%----------------------------------------------------------------------
-spec execute(Fun::fun()) -> term().
execute(Fun) ->
    poolboy:transaction(riak, fun(Worker) ->
        gen_server:call(Worker, {execute, Fun})
    end).

%% @doc Returns the number of connections as seen by the supervisor.
-spec conn_count() -> integer().
conn_count() ->
    Props = supervisor:count_children(riakc_sup),
    case proplists:get_value(active, Props) of
        N when is_integer(N) -> N;
        undefined -> 0
    end.

%% Application behavior API
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).
