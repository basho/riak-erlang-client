-module(riakc_connection).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(Args) ->
    [Host, Port] = Args,
    {ok, Conn} = riakc_pb_socket:start_link(Host, Port),
    
    {ok, #state{conn=Conn}}.

handle_call({execute, Fun}, _From, State) ->
    Result = Fun(State#state.conn),
    {reply, Result, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, undefined) -> ok;
terminate(_Reason, #state{conn=Conn}) -> 
    case is_process_alive(Conn) of
        true -> riakc_pb_socket:stop(Conn), ok;
        false -> ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
