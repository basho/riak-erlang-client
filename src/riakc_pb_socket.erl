%% -------------------------------------------------------------------
%%
%% riakc_pb_socket: protocol buffer client
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Manages a connection to Riak via the Protocol Buffers
%% transport and executes the commands that can be performed over that
%% connection.
%% @end

-module(riakc_pb_socket).
-include_lib("kernel/include/inet.hrl").
-include_lib("riak_pb/include/riak_dt_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riak_pb/include/riak_search_pb.hrl").
-include_lib("riak_pb/include/riak_ts_pb.hrl").
-include_lib("riak_pb/include/riak_ts_ttb.hrl").
-include_lib("riak_pb/include/riak_yokozuna_pb.hrl").
-include("riakc.hrl").
-behaviour(gen_server).

-export([start_link/2, start_link/3,
         start/2, start/3,
         stop/1,
         set_options/2, set_options/3,
         is_connected/1, is_connected/2,
         ping/1, ping/2,
         get_client_id/1, get_client_id/2,
         set_client_id/2, set_client_id/3,
         get_server_info/1, get_server_info/2,
         get/3, get/4, get/5,
         put/2, put/3, put/4,
         delete/3, delete/4, delete/5,
         delete_vclock/4, delete_vclock/5, delete_vclock/6,
         delete_obj/2, delete_obj/3, delete_obj/4,
         list_buckets/1, list_buckets/2, list_buckets/3,
         stream_list_buckets/1, stream_list_buckets/2, stream_list_buckets/3,
         legacy_list_buckets/2,
         list_keys/2, list_keys/3,
         stream_list_keys/2, stream_list_keys/3,
         get_bucket/2, get_bucket/3, get_bucket/4,
         get_bucket_type/2, get_bucket_type/3,
         set_bucket/3, set_bucket/4, set_bucket/5,
         set_bucket_type/3, set_bucket_type/4,
         reset_bucket/2, reset_bucket/3, reset_bucket/4,
         mapred/3, mapred/4, mapred/5,
         mapred_stream/4, mapred_stream/5, mapred_stream/6,
         mapred_bucket/3, mapred_bucket/4, mapred_bucket/5,
         mapred_bucket_stream/5, mapred_bucket_stream/6,
         search/3, search/4, search/5, search/6,
         get_index/4, get_index/5, get_index/6, get_index/7, %% @deprecated
         get_index_eq/4, get_index_range/5, get_index_eq/5, get_index_range/6,
         cs_bucket_fold/3,
         default_timeout/1,
         tunnel/4,
         get_preflist/3, get_preflist/4,
         get_coverage/2, get_coverage/3,
         replace_coverage/3, replace_coverage/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Yokozuna admin commands
-export([list_search_indexes/1, list_search_indexes/2,
         create_search_index/2, create_search_index/3, create_search_index/4,
         get_search_index/2, get_search_index/3,
         delete_search_index/2, delete_search_index/3,
         set_search_index/3,
         get_search_schema/2, get_search_schema/3,
         create_search_schema/3, create_search_schema/4]).

%% Pre-Riak 2.0 Counter API - NOT for CRDT counters
-export([counter_incr/4, counter_val/3]).
-export([counter_incr/5, counter_val/4]).

%% Datatypes API
-export([fetch_type/3, fetch_type/4,
         update_type/4, update_type/5,
         modify_type/5]).

%% supporting functions used in riakc_ts
-export([mk_reqid/0]).

-deprecated({get_index,'_', eventually}).

-type ctx() :: any().
-type rpb_req() :: {tunneled, msg_id(), binary()} | atom() | tuple().
-type rpb_resp() :: atom() | tuple().
-type msg_id() :: non_neg_integer(). %% Request identifier for tunneled message types
-type search_admin_opt() :: {timeout, timeout()} |
                     {call_timeout, timeout()}.
-type search_admin_opts() :: [search_admin_opt()].
-type index_opt() :: {timeout, timeout()} |
                     {call_timeout, timeout()} |
                     {stream, boolean()} |
                     {continuation, binary()} |
                     {pagination_sort, boolean()} |
                     {max_results, non_neg_integer() | all}.
-type index_opts() :: [index_opt()].
-type range_index_opt() :: {return_terms, boolean()} |
                           {term_regex, binary()}.
-type range_index_opts() :: [index_opt() | range_index_opt()].
-type cs_opt() :: {timeout, timeout()} |
                  {continuation, binary()} |
                  {max_results, non_neg_integer() | all} |
                  {start_key, binary()} |
                  {start_incl, boolean()} |
                  {end_key, binary()} |
                  {end_incl, boolean()}.
-type cs_opts() :: [cs_opt()].

%% Which client operation the default timeout is being requested
%% for. `timeout' is the global default timeout. Any of these defaults
%% can be overridden by setting the application environment variable
%% of the same name on the `riakc' application, for example:
%% `application:set_env(riakc, ping_timeout, 5000).'
-record(request, {ref :: reference(),
                  msg :: rpb_req(),
                  from, ctx :: ctx(),
                  timeout :: timeout(),
                  tref :: reference() | undefined,
                  opts :: proplists:proplist()
                 }).

-ifdef(namespaced_types).
-type request_queue_t() :: queue:queue(#request{}).
-else.
-type request_queue_t() :: queue().
-endif.

-ifdef(deprecated_now).
-define(NOW, erlang:system_time(micro_seconds)).
-else.
-define(NOW, erlang:now()).
-endif.

-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's Protocol Buffers interface
-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-record(state, {address :: address(),    % address to connect to
                port :: portnum(),       % port to connect to
                auto_reconnect = false :: boolean(), % if true, automatically reconnects to server
                                        % if false, exits on connection failure/request timeout
                queue_if_disconnected = false :: boolean(), % if true, add requests to queue if disconnected
                sock :: port() | ssl:sslsocket() | undefined,       % gen_tcp socket
                keepalive = false :: boolean(), % if true, enabled TCP keepalive for the socket
                transport = gen_tcp :: 'gen_tcp' | 'ssl',
                active :: #request{} | undefined,     % active request
                queue :: request_queue_t() | undefined,      % queue of pending requests
                connects=0 :: non_neg_integer(), % number of successful connects
                failed=[] :: [connection_failure()],  % breakdown of failed connects
                connect_timeout=infinity :: timeout(), % timeout of TCP connection
                credentials :: undefined | {string(), string()}, % username/password
                cacertfile,    % Path to CA certificate file
                certfile,      % Path to client certificate file, when using
                               % certificate authentication
                keyfile,       % Path to certificate keyfile, when using
                               % certificate authentication
                ssl_opts = [], % Arbitrary SSL options, see the erlang SSL
                               % documentation.
                reconnect_interval=?FIRST_RECONNECT_INTERVAL :: non_neg_integer()}).

-export_type([address/0, portnum/0]).

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port with Options.
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Create a process to talk with the riak server on Address:Port.
%%      Client id will be assigned by the server.
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port with Options.
-spec start(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start(Address, Port, Options) when is_list(Options) ->
    gen_server:start(?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @doc Change the options for this socket.  Allows you to connect with one
%%      set of options then run with another (e.g. connect with no options to
%%      make sure the server is there, then enable queue_if_disconnected).
%% @equiv set_options(Pid, Options, infinity)
%% @see start_link/3
-spec set_options(pid(), client_options()) -> ok.
set_options(Pid, Options) ->
    call_infinity(Pid, {set_options, Options}).

%% @doc Like set_options/2, but with a gen_server timeout.
%% @see start_link/3
%% @deprecated
-spec set_options(pid(), client_options(), timeout()) -> ok.
set_options(Pid, Options, Timeout) ->
    gen_server:call(Pid, {set_options, Options}, Timeout).

%% @doc Determines whether the client is connected. Returns true if
%% connected, or false and a list of connection failures and frequencies if
%% disconnected.
%% @equiv is_connected(Pid, infinity)
-spec is_connected(pid()) -> true | {false, [connection_failure()]}.
is_connected(Pid) ->
    call_infinity(Pid, is_connected).

%% @doc Determines whether the client is connected, with the specified
%% timeout to the client process. Returns true if connected, or false
%% and a list of connection failures and frequencies if disconnected.
%% @see is_connected/1
%% @deprecated
-spec is_connected(pid(), timeout()) -> true | {false, [connection_failure()]}.
is_connected(Pid, Timeout) ->
    gen_server:call(Pid, is_connected, Timeout).

%% @doc Ping the server
%% @equiv ping(Pid, default_timeout(ping_timeout))
-spec ping(pid()) -> pong.
ping(Pid) ->
    call_infinity(Pid, {req, rpbpingreq, default_timeout(ping_timeout)}).

%% @doc Ping the server specifying timeout
-spec ping(pid(), timeout()) -> pong.
ping(Pid, Timeout) ->
    call_infinity(Pid, {req, rpbpingreq, Timeout}).

%% @doc Get the client id for this connection
%% @equiv get_client_id(Pid, default_timeout(get_client_id_timeout))
-spec get_client_id(pid()) -> {ok, client_id()} | {error, term()}.
get_client_id(Pid) ->
    get_client_id(Pid, default_timeout(get_client_id_timeout)).

%% @doc Get the client id for this connection specifying timeout
-spec get_client_id(pid(), timeout()) -> {ok, client_id()} | {error, term()}.
get_client_id(Pid, Timeout) ->
    call_infinity(Pid, {req, rpbgetclientidreq, Timeout}).

%% @doc Set the client id for this connection
%% @equiv set_client_id(Pid, ClientId, default_timeout(set_client_id_timeout))
-spec set_client_id(pid(), client_id()) -> {ok, client_id()} | {error, term()}.
set_client_id(Pid, ClientId) ->
    set_client_id(Pid, ClientId, default_timeout(set_client_id_timeout)).

%% @doc Set the client id for this connection specifying timeout
-spec set_client_id(pid(), client_id(), timeout()) -> {ok, client_id()} | {error, term()}.
set_client_id(Pid, ClientId, Timeout) ->
    call_infinity(Pid,
                  {req, #rpbsetclientidreq{client_id = ClientId},
                   Timeout}).

%% @doc Get the server information for this connection
%% @equiv get_server_info(Pid, default_timeout(get_server_info_timeout))
-spec get_server_info(pid()) -> {ok, server_info()} | {error, term()}.
get_server_info(Pid) ->
    get_server_info(Pid, default_timeout(get_server_info_timeout)).

%% @doc Get the server information for this connection specifying timeout
-spec get_server_info(pid(), timeout()) -> {ok, server_info()} | {error, term()}.
get_server_info(Pid, Timeout) ->
    call_infinity(Pid, {req, rpbgetserverinforeq, Timeout}).

%% @doc Get bucket/key from the server.
%%      Will return {error, notfound} if the key is not on the server.
%% @equiv get(Pid, Bucket, Key, [], default_timeout(get_timeout))
-spec get(pid(), bucket() | bucket_and_type(), key()) -> {ok, riakc_obj()} | {error, term()}.
get(Pid, Bucket, Key) ->
    get(Pid, Bucket, Key, [], default_timeout(get_timeout)).

%% @doc Get bucket/key from the server specifying timeout.
%%      Will return {error, notfound} if the key is not on the server.
%% @equiv get(Pid, Bucket, Key, Options, Timeout)
-spec get(pid(), bucket() | bucket_and_type(), key(), TimeoutOrOptions::timeout() |  get_options()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(Pid, Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    get(Pid, Bucket, Key, [], Timeout);
get(Pid, Bucket, Key, Options) ->
    get(Pid, Bucket, Key, Options, default_timeout(get_timeout)).

%% @doc Get bucket/key from the server supplying options and timeout.
%%      <code>unchanged</code> will be returned when the
%%      <code>{if_modified, Vclock}</code> option is specified and the
%%      object is unchanged.
-spec get(pid(), bucket() | bucket_and_type(), key(), get_options(), timeout()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(Pid, Bucket, Key, Options, Timeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = get_options(Options, #rpbgetreq{type =T, bucket = B, key = Key}),
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Put the metadata/value in the object under bucket/key
%% @equiv put(Pid, Obj, [])
%% @see put/4
-spec put(pid(), riakc_obj()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(Pid, Obj) ->
    put(Pid, Obj, []).

%% @doc Put the metadata/value in the object under bucket/key with options or timeout.
%% @equiv put(Pid, Obj, Options, Timeout)
%% @see put/4
-spec put(pid(), riakc_obj(), TimeoutOrOptions::timeout() | put_options()) ->
                 ok | {ok, riakc_obj()} |  riakc_obj() | {ok, key()} | {error, term()}.
put(Pid, Obj, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    put(Pid, Obj, [], Timeout);
put(Pid, Obj, Options) ->
    put(Pid, Obj, Options, default_timeout(put_timeout)).

%% @doc Put the metadata/value in the object under bucket/key with
%%      options and timeout. Put throws `siblings' if the
%%      riakc_obj contains siblings that have not been resolved by
%%      calling {@link riakc_obj:select_sibling/2.} or {@link
%%      riakc_obj:update_value/2} and {@link
%%      riakc_obj:update_metadata/2}.  If the object has no key and
%%      the Riak node supports it, `{ok, Key::key()}' will be returned
%%      when the object is created, or `{ok, Obj::riakc_obj()}' if
%%      `return_body' was specified.
%% @throws siblings
%% @end
-spec put(pid(), riakc_obj(), put_options(), timeout()) ->
                 ok | {ok, riakc_obj()} | riakc_obj() | {ok, key()} | {error, term()}.
put(Pid, Obj, Options, Timeout) ->
    Content = riak_pb_kv_codec:encode_content({riakc_obj:get_update_metadata(Obj),
                                               riakc_obj:get_update_value(Obj)}),
    Req = put_options(Options,
                      #rpbputreq{bucket = riakc_obj:only_bucket(Obj),
                                 type = riakc_obj:bucket_type(Obj),
                                 key = riakc_obj:key(Obj),
                                 vclock = riakc_obj:vclock(Obj),
                                 content = Content}),
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Delete the key/value
%% @equiv delete(Pid, Bucket, Key, [])
-spec delete(pid(), bucket() | bucket_and_type(), key()) -> ok | {error, term()}.
delete(Pid, Bucket, Key) ->
    delete(Pid, Bucket, Key, []).

%% @doc Delete the key/value specifying timeout or options. <em>Note that the rw quorum is deprecated, use r and w.</em>
%% @equiv delete(Pid, Bucket, Key, Options, Timeout)
-spec delete(pid(), bucket() | bucket_and_type(), key(), TimeoutOrOptions::timeout() | delete_options()) ->
                    ok | {error, term()}.
delete(Pid, Bucket, Key, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    delete(Pid, Bucket, Key, [], Timeout);
delete(Pid, Bucket, Key, Options) ->
    delete(Pid, Bucket, Key, Options, default_timeout(delete_timeout)).

%% @doc Delete the key/value with options and timeout. <em>Note that the rw quorum is deprecated, use r and w.</em>
-spec delete(pid(), bucket() | bucket_and_type(), key(), delete_options(), timeout()) -> ok | {error, term()}.
delete(Pid, Bucket, Key, Options, Timeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = delete_options(Options, #rpbdelreq{type = T, bucket = B, key = Key}),
    call_infinity(Pid, {req, Req, Timeout}).


%% @doc Delete the object at Bucket/Key, giving the vector clock.
%% @equiv delete_vclock(Pid, Bucket, Key, VClock, [])
-spec delete_vclock(pid(), bucket() | bucket_and_type(), key(), riakc_obj:vclock()) -> ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock) ->
    delete_vclock(Pid, Bucket, Key, VClock, []).

%% @doc Delete the object at Bucket/Key, specifying timeout or options and giving the vector clock.
%% @equiv delete_vclock(Pid, Bucket, Key, VClock, Options, Timeout)
-spec delete_vclock(pid(), bucket() | bucket_and_type(), key(), riakc_obj:vclock(), TimeoutOrOptions::timeout() | delete_options()) ->
                           ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    delete_vclock(Pid, Bucket, Key, VClock, [], Timeout);
delete_vclock(Pid, Bucket, Key, VClock, Options) ->
    delete_vclock(Pid, Bucket, Key, VClock, Options, default_timeout(delete_timeout)).

%% @doc Delete the key/value with options and timeout and giving the
%% vector clock. This form of delete ensures that subsequent get and
%% put requests will be correctly ordered with the delete.
%% @see delete_obj/4
-spec delete_vclock(pid(), bucket() | bucket_and_type(), key(), riakc_obj:vclock(), delete_options(), timeout()) ->
                           ok | {error, term()}.
delete_vclock(Pid, Bucket, Key, VClock, Options, Timeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = delete_options(Options, #rpbdelreq{type = T, bucket = B, key = Key,
            vclock=VClock}),
    call_infinity(Pid, {req, Req, Timeout}).


%% @doc Delete the riak object.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj))
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj()) -> ok | {error, term()}.
delete_obj(Pid, Obj) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), [], default_timeout(delete_timeout)).

%% @doc Delete the riak object with options.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj), Options)
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj(), delete_options()) -> ok | {error, term()}.
delete_obj(Pid, Obj, Options) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), Options, default_timeout(delete_timeout)).

%% @doc Delete the riak object with options and timeout.
%% @equiv delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj), riakc_obj:vclock(Obj), Options, Timeout)
%% @see delete_vclock/6
-spec delete_obj(pid(), riakc_obj(), delete_options(), timeout()) -> ok | {error, term()}.
delete_obj(Pid, Obj, Options, Timeout) ->
    delete_vclock(Pid, riakc_obj:bucket(Obj), riakc_obj:key(Obj),
        riakc_obj:vclock(Obj), Options, Timeout).

%% @doc List all buckets on the server in the "default" bucket type.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_buckets(Pid, default_timeout(list_buckets_timeout))
-spec list_buckets(pid()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(Pid) ->
    list_buckets(Pid, <<"default">>, []).

%% @doc List all buckets in a bucket type, specifying server-side timeout.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_buckets(pid(), timeout()|list()|binary()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(Pid, Type) when is_binary(Type) ->
    list_buckets(Pid, Type, []);
list_buckets(Pid, Timeout) when is_integer(Timeout) ->
    list_buckets(Pid, <<"default">>, [{timeout, Timeout}]);
list_buckets(Pid, Options) ->
    list_buckets(Pid, <<"default">>, Options).

list_buckets(Pid, Type, Options) when is_binary(Type), is_list(Options) ->
    case stream_list_buckets(Pid, Type, Options) of
        {ok, ReqId} ->
            riakc_utils:wait_for_list(ReqId);
        Error ->
            Error
    end.

stream_list_buckets(Pid) ->
    stream_list_buckets(Pid, <<"default">>, []).

stream_list_buckets(Pid, Type) when is_binary(Type) ->
    stream_list_buckets(Pid, Type, []);
stream_list_buckets(Pid, Timeout) when is_integer(Timeout) ->
    stream_list_buckets(Pid, <<"default">>,[{timeout, Timeout}]);
stream_list_buckets(Pid, Options) ->
    stream_list_buckets(Pid, <<"default">>, Options).

stream_list_buckets(Pid, Type, Options) ->
    ST = case proplists:get_value(timeout, Options) of
             undefined -> ?DEFAULT_PB_TIMEOUT;
             T -> T
         end,
    ReqId = mk_reqid(),
    CT = ST + ?DEFAULT_ADDITIONAL_CLIENT_TIMEOUT,
    Req = #rpblistbucketsreq{timeout=ST, type=Type, stream=true},
    call_infinity(Pid, {req, Req, CT, {ReqId, self()}}).

legacy_list_buckets(Pid, Options) ->
    ST = case proplists:get_value(timeout, Options) of
             undefined -> ?DEFAULT_PB_TIMEOUT;
             T -> T
         end,
    CT = ST + ?DEFAULT_ADDITIONAL_CLIENT_TIMEOUT,
    Req = #rpblistbucketsreq{timeout=ST},
    call_infinity(Pid, {req, Req, CT}).

%% @doc List all keys in a bucket
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv list_keys(Pid, Bucket, default_timeout(list_keys_timeout))
-spec list_keys(pid(), bucket() | bucket_and_type()) -> {ok, [key()]} | {error, term()}.
list_keys(Pid, Bucket) ->
    list_keys(Pid, Bucket, []).

%% @doc List all keys in a bucket specifying timeout. This is
%% implemented using {@link stream_list_keys/3} and then waiting for
%% the results to complete streaming.
%% <em>This is a potentially expensive operation and should not be used in production.</em>
-spec list_keys(pid(), bucket() | bucket_and_type(), list()|timeout()) -> {ok, [key()]} |
                                                                          {error, term()}.
list_keys(Pid, Bucket, infinity) ->
    list_keys(Pid, Bucket, [{timeout, undefined}]);
list_keys(Pid, Bucket, Timeout) when is_integer(Timeout) ->
    list_keys(Pid, Bucket, [{timeout, Timeout}]);
list_keys(Pid, Bucket, Options) ->
    case stream_list_keys(Pid, Bucket, Options) of
        {ok, ReqId} ->
            riakc_utils:wait_for_list(ReqId);
        Error ->
            Error
    end.

%% @doc Stream list of keys in the bucket to the calling process.  The
%%      process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Pid, Bucket, default_timeout(stream_list_keys_timeout))
-spec stream_list_keys(pid(), bucket()) -> {ok, req_id()} | {error, term()}.
stream_list_keys(Pid, Bucket) ->
    stream_list_keys(Pid, Bucket, []).

%% @doc Stream list of keys in the bucket to the calling process specifying server side
%%      timeout.
%%      The process receives these messages.
%% ```    {ReqId::req_id(), {keys, [key()]}}
%%        {ReqId::req_id(), done}'''
%% <em>This is a potentially expensive operation and should not be used in production.</em>
%% @equiv stream_list_keys(Pid, Bucket, Timeout, default_timeout(stream_list_keys_call_timeout))
-spec stream_list_keys(pid(), bucket() | bucket_and_type(), integer()|list()) ->
                              {ok, req_id()} |
                              {error, term()}.
stream_list_keys(Pid, Bucket, infinity) ->
    stream_list_keys(Pid, Bucket, [{timeout, undefined}]);
stream_list_keys(Pid, Bucket, Timeout) when is_integer(Timeout) ->
    stream_list_keys(Pid, Bucket, [{timeout, Timeout}]);
stream_list_keys(Pid, Bucket, Options) ->
    ST = case proplists:get_value(timeout, Options) of
             undefined -> ?DEFAULT_PB_TIMEOUT;
             T -> T
         end,
    {BT, B} = maybe_bucket_type(Bucket),
    CT = ST + ?DEFAULT_ADDITIONAL_CLIENT_TIMEOUT,
    Req = #rpblistkeysreq{type=BT, bucket=B, timeout=ST},
    ReqId = mk_reqid(),
    call_infinity(Pid, {req, Req, CT, {ReqId, self()}}).

%% @doc Get bucket properties.
%% @equiv get_bucket(Pid, Bucket, default_timeout(get_bucket_timeout))
-spec get_bucket(pid(), bucket() | bucket_and_type()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Pid, Bucket) ->
    get_bucket(Pid, Bucket, default_timeout(get_bucket_timeout)).

%% @doc Get bucket properties specifying a server side timeout.
%% @equiv get_bucket(Pid, Bucket, Timeout, default_timeout(get_bucket_call_timeout))
-spec get_bucket(pid(), bucket() | bucket_and_type(), timeout()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(Pid, Bucket, Timeout) ->
    get_bucket(Pid, Bucket, Timeout, default_timeout(get_bucket_call_timeout)).

%% @doc Get bucket properties specifying a server side and local call timeout.
%% @deprecated because `CallTimeout' is ignored
-spec get_bucket(pid(), bucket() | bucket_and_type(), timeout(), timeout()) -> {ok, bucket_props()} |
                                                           {error, term()}.
get_bucket(Pid, Bucket, Timeout, _CallTimeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = #rpbgetbucketreq{type = T, bucket = B},
    call_infinity(Pid, {req, Req, Timeout}).

get_bucket_type(Pid, BucketType) ->
    get_bucket_type(Pid, BucketType, default_timeout(get_bucket_timeout)).

get_bucket_type(Pid, BucketType, Timeout) ->
    Req = #rpbgetbuckettypereq{type = BucketType},
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Set bucket properties.
%% @equiv set_bucket(Pid, Bucket, BucketProps, default_timeout(set_bucket_timeout))
-spec set_bucket(pid(), bucket() | bucket_and_type(), bucket_props()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps) ->
    set_bucket(Pid, Bucket, BucketProps, default_timeout(set_bucket_timeout)).

%% @doc Set bucket properties specifying a server side timeout.
%% @equiv set_bucket(Pid, Bucket, BucketProps, Timeout, default_timeout(set_bucket_call_timeout))
-spec set_bucket(pid(), bucket() | bucket_and_type(), bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps, Timeout) ->
    set_bucket(Pid, Bucket, BucketProps, Timeout,
               default_timeout(set_bucket_call_timeout)).

%% @doc Set bucket properties specifying a server side and local call timeout.
%% @deprecated because `CallTimeout' is ignored
-spec set_bucket(pid(), bucket() | bucket_and_type(), bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(Pid, Bucket, BucketProps, Timeout, _CallTimeout) ->
    PbProps = riak_pb_codec:encode_bucket_props(BucketProps),
    {T, B} = maybe_bucket_type(Bucket),
    Req = #rpbsetbucketreq{type = T, bucket = B, props = PbProps},
    call_infinity(Pid, {req, Req, Timeout}).

set_bucket_type(Pid, BucketType, BucketProps) ->
    set_bucket_type(Pid, BucketType, BucketProps, default_timeout(set_bucket_timeout)).

set_bucket_type(Pid, BucketType, BucketProps, Timeout) ->
    PbProps = riak_pb_codec:encode_bucket_props(BucketProps),
    Req = #rpbsetbuckettypereq{type = BucketType, props = PbProps},
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Reset bucket properties back to the defaults.
%% @equiv reset_bucket(Pid, Bucket, default_timeout(reset_bucket_timeout), default_timeout(reset_bucket_call_timeout))
-spec reset_bucket(pid(), bucket() | bucket_and_type()) -> ok | {error, term()}.
reset_bucket(Pid, Bucket) ->
    reset_bucket(Pid, Bucket, default_timeout(reset_bucket_timeout), default_timeout(reset_bucket_call_timeout)).

%% @doc Reset bucket properties back to the defaults.
%% @equiv reset_bucket(Pid, Bucket, Timeout, default_timeout(reset_bucket_call_timeout))
-spec reset_bucket(pid(), bucket() | bucket_and_type(), timeout()) -> ok | {error, term()}.
reset_bucket(Pid, Bucket, Timeout) ->
    reset_bucket(Pid, Bucket, Timeout, default_timeout(reset_bucket_call_timeout)).

%% @doc Reset bucket properties back to the defaults.
%% @deprecated because `CallTimeout' is ignored
-spec reset_bucket(pid(), bucket() | bucket_and_type(), timeout(), timeout()) -> ok | {error, term()}.
reset_bucket(Pid, Bucket, Timeout, _CallTimeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = #rpbresetbucketreq{type = T, bucket = B},
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Perform a MapReduce job across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Inputs, Query, default_timeout(mapred))
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()]) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query) ->
    mapred(Pid, Inputs, Query, default_timeout(mapred_timeout)).

%% @doc Perform a MapReduce job across the cluster with a job timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% @equiv mapred(Pid, Inputs, Query, Timeout, default_timeout(mapred_call_timeout))
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query, Timeout) ->
    mapred(Pid, Inputs, Query, Timeout, default_timeout(mapred_call_timeout)).

%% @doc Perform a MapReduce job across the cluster with a job and
%%      local call timeout.  See the MapReduce documentation for
%%      explanation of behavior. This is implemented by using
%%      <code>mapred_stream/6</code> and then waiting for all results.
%% @see mapred_stream/6
-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query, Timeout, CallTimeout) ->
    case mapred_stream(Pid, Inputs, Query, self(), Timeout, CallTimeout) of
        {ok, ReqId} ->
            wait_for_mapred(ReqId, Timeout);
        Error ->
            Error
    end.

%% @doc Perform a streaming MapReduce job across the cluster sending results
%%      to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(ConnectionPid, Inputs, Query, ClientPid, default_timeout(mapred_stream_timeout))
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, Inputs, Query, ClientPid) ->
    mapred_stream(Pid, Inputs, Query, ClientPid, default_timeout(mapred_stream_timeout)).

%% @doc Perform a streaming MapReduce job with a timeout across the cluster.
%%      sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv mapred_stream(ConnectionPid, Inputs, Query, ClientPid, Timeout, default_timeout(mapred_stream_call_timeout))
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),Query::[mapred_queryterm()], ClientPid::pid(), Timeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, Inputs, Query, ClientPid, Timeout) ->
    mapred_stream(Pid, Inputs, Query, ClientPid, Timeout,
                  default_timeout(mapred_stream_call_timeout)).

%% @doc Perform a streaming MapReduce job with a map/red timeout across the cluster,
%%      a local call timeout and sending results to ClientPid.
%%      See the MapReduce documentation for explanation of behavior.
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @deprecated because `CallTimeout' is ignored
-spec mapred_stream(ConnectionPid::pid(),Inputs::mapred_inputs(),
                    Query::[mapred_queryterm()], ClientPid::pid(),
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, req_id()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_stream(Pid, {index,Bucket,Name,Key}, Query, ClientPid, Timeout, CallTimeout) when is_tuple(Name) ->
    Index = riakc_obj:index_id_to_bin(Name),
    mapred_stream(Pid, {index,Bucket,Index,Key}, Query, ClientPid, Timeout, CallTimeout);
mapred_stream(Pid, {index,Bucket,Name,StartKey,EndKey}, Query, ClientPid, Timeout, CallTimeout) when is_tuple(Name) ->
    Index = riakc_obj:index_id_to_bin(Name),
    mapred_stream(Pid, {index,Bucket,Index,StartKey,EndKey}, Query, ClientPid, Timeout, CallTimeout);
mapred_stream(Pid, {index,Bucket,Name,Key}, Query, ClientPid, Timeout, CallTimeout) when is_binary(Name) andalso is_integer(Key) ->
    BinKey = list_to_binary(integer_to_list(Key)),
    mapred_stream(Pid, {index,Bucket,Name,BinKey}, Query, ClientPid, Timeout, CallTimeout);
mapred_stream(Pid, {index,Bucket,Name,StartKey,EndKey}, Query, ClientPid, Timeout, CallTimeout) when is_binary(Name) andalso is_integer(StartKey) ->
    BinStartKey = list_to_binary(integer_to_list(StartKey)),
    mapred_stream(Pid, {index,Bucket,Name,BinStartKey,EndKey}, Query, ClientPid, Timeout, CallTimeout);
mapred_stream(Pid, {index,Bucket,Name,StartKey,EndKey}, Query, ClientPid, Timeout, CallTimeout) when is_binary(Name) andalso is_integer(EndKey) ->
    BinEndKey = list_to_binary(integer_to_list(EndKey)),
    mapred_stream(Pid, {index,Bucket,Name,StartKey,BinEndKey}, Query, ClientPid, Timeout, CallTimeout);
mapred_stream(Pid, Inputs, Query, ClientPid, Timeout, _CallTimeout) ->
    MapRed = [{'inputs', Inputs},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid).

%% @doc Perform a MapReduce job against a bucket across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Pid, Bucket, Query, default_timeout(mapred_bucket_timeout))
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()]) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query) ->
    mapred_bucket(Pid, Bucket, Query, default_timeout(mapred_bucket_timeout)).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%% @equiv mapred_bucket(Pid, Bucket, Query, Timeout, default_timeout(mapred_bucket_call_timeout))
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()], Timeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query, Timeout) ->
    mapred_bucket(Pid, Bucket, Query, Timeout, default_timeout(mapred_bucket_call_timeout)).

%% @doc Perform a MapReduce job against a bucket with a timeout
%%      across the cluster and local call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
-spec mapred_bucket(Pid::pid(), Bucket::bucket(), Query::[mapred_queryterm()],
                    Timeout::timeout(), CallTimeout::timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, Err :: term()}.
mapred_bucket(Pid, Bucket, Query, Timeout, CallTimeout) ->
    case mapred_bucket_stream(Pid, Bucket, Query, self(), Timeout, CallTimeout) of
        {ok, ReqId} ->
            wait_for_mapred(ReqId, Timeout);
        Error ->
            Error
    end.

%% @doc Perform a streaming MapReduce job against a bucket with a timeout
%%      across the cluster.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @equiv     mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout, default_timeout(mapred_bucket_stream_call_timeout))
-spec mapred_bucket_stream(ConnectionPid::pid(), bucket(), [mapred_queryterm()], ClientPid::pid(), timeout()) ->
                                  {ok, req_id()} |
                                  {error, term()}.
mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout) ->
    mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout,
                         default_timeout(mapred_bucket_stream_call_timeout)).

%% @doc Perform a streaming MapReduce job against a bucket with a server timeout
%%      across the cluster and a call timeout.
%%      See the MapReduce documentation for explanation of behavior.
%% <em>This uses list_keys under the hood and so is potentially an expensive operation that should not be used in production.</em>
%%      The ClientPid will receive messages in this format:
%% ```  {ReqId::req_id(), {mapred, Phase::non_neg_integer(), mapred_result()}}
%%      {ReqId::req_id(), done}'''
%% @deprecated because `CallTimeout' is ignored
-spec mapred_bucket_stream(ConnectionPid::pid(), bucket(), [mapred_queryterm()], ClientPid::pid(), timeout(), timeout()) ->
                                  {ok, req_id()} | {error, term()}.
mapred_bucket_stream(Pid, Bucket, Query, ClientPid, Timeout, _CallTimeout) ->
    MapRed = [{'inputs', Bucket},
              {'query', Query},
              {'timeout', Timeout}],
    send_mapred_req(Pid, MapRed, ClientPid).


%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
-spec search(pid(), binary(), binary()) ->
                    {ok, search_result()} | {error, term()}.
search(Pid, Index, SearchQuery) ->
    search(Pid, Index, SearchQuery, []).

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
-spec search(pid(), binary(), binary(), search_options()) ->
                    {ok, search_result()} | {error, term()}.
search(Pid, Index, SearchQuery, Options) ->
    Timeout = default_timeout(search_timeout),
    search(Pid, Index, SearchQuery, Options, Timeout).

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
-spec search(pid(), binary(), binary(), search_options(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(Pid, Index, SearchQuery, Options, Timeout) ->
    CallTimeout = default_timeout(search_call_timeout),
    search(Pid, Index, SearchQuery, Options, Timeout, CallTimeout).

%% @doc Execute a search query. This command will return an error
%%      unless executed against a Riak Search cluster.
%% @deprecated because `CallTimeout' is ignored
-spec search(pid(), binary(), binary(), search_options(), timeout(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(Pid, Index, SearchQuery, Options, Timeout, _CallTimeout) ->
    Req = search_options(Options, #rpbsearchqueryreq{q = SearchQuery, index = Index}),
    call_infinity(Pid, {req, Req, Timeout}).

-spec get_search_schema(pid(), binary(), search_admin_opts()) ->
                    {ok, search_schema()} | {error, term()}.
get_search_schema(Pid, SchemaName, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    Req = #rpbyokozunaschemagetreq{ name = SchemaName },
    call_infinity(Pid, {req, Req, Timeout}).

-spec get_search_schema(pid(), binary()) ->
                    {ok, search_schema()} | {error, term()}.
get_search_schema(Pid, SchemaName) ->
    get_search_schema(Pid, SchemaName, []).

-spec get_search_index(pid(), binary(), search_admin_opts()) ->
                    {ok, search_index()} | {error, term()}.
get_search_index(Pid, Index, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    Req = #rpbyokozunaindexgetreq{ name = Index },
    Results = call_infinity(Pid, {req, Req, Timeout}),
    case Results of
        {ok, [Result]} ->
            {ok, Result};
        {ok, []} ->
            {error, notfound};
        X -> X
    end.

-spec get_search_index(pid(), binary()) ->
                    {ok, search_index()} | {error, term()}.
get_search_index(Pid, Index) ->
    get_search_index(Pid, Index, []).

-spec list_search_indexes(pid(), search_admin_opts()) ->
                    {ok, [search_index()]} | {error, term()}.
list_search_indexes(Pid, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    call_infinity(Pid, {req, #rpbyokozunaindexgetreq{}, Timeout}).

-spec list_search_indexes(pid()) ->
                    {ok, [search_index()]} | {error, term()}.
list_search_indexes(Pid) ->
    list_search_indexes(Pid, []).

%% @doc Create a schema, which is a required component of an index.
-spec create_search_schema(pid(), binary(), binary()) ->
                    ok | {error, term()}.
create_search_schema(Pid, SchemaName, Content) ->
    create_search_schema(Pid, SchemaName, Content, []).

%% @doc Create a schema, which is a required component of an index.
-spec create_search_schema(pid(), binary(), binary(), search_admin_opts()) ->
                    ok | {error, term()}.
create_search_schema(Pid, SchemaName, Content, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    Req = #rpbyokozunaschemaputreq{
        schema = #rpbyokozunaschema{name = SchemaName, content = Content}
    },
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Create a search index.
-spec create_search_index(pid(), binary()) ->
                    ok | {error, term()}.
create_search_index(Pid, Index) ->
    create_search_index(Pid, Index, <<>>, []).

-spec create_search_index(pid(), binary(), timeout() | search_admin_opts()) ->
                                 ok | {error, term()}.
create_search_index(Pid, Index, Timeout)
  when is_integer(Timeout); Timeout =:= infinity ->
    create_search_index(Pid, Index, <<>>, [{timeout, Timeout}]);
create_search_index(Pid, Index, Opts) ->
    create_search_index(Pid, Index, <<>>, Opts).

-spec create_search_index(pid(), binary(), binary(),
                          timeout()|search_admin_opts()) ->
                                 ok | {error, term()}.
create_search_index(Pid, Index, SchemaName, Timeout)
  when is_integer(Timeout); Timeout =:= infinity  ->
    create_search_index(Pid, Index, SchemaName, [{timeout, Timeout}]);
create_search_index(Pid, Index, SchemaName, Opts) ->
    ST = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    NVal = proplists:get_value(n_val, Opts),
    Req = set_index_create_req_nval(NVal, Index, SchemaName),
    Req1 = case proplists:is_defined(timeout, Opts) of
               true ->
                   set_index_create_req_timeout(ST, Req);
               _ ->
                   Req
           end,

    CT = if
             is_integer(ST) ->
                 %% Add an extra 500ms to the create_search_index timeout
                 %% and use that for the client-side timeout.
                 %% This should give the creation process time to throw
                 %% back a proper response.
                 ST + ?DEFAULT_ADDITIONAL_CLIENT_TIMEOUT;
             true ->
                 ST
         end,
    call_infinity(Pid, {req, Req1, CT}).

%% @doc Delete a search index.
-spec delete_search_index(pid(), binary()) ->
                    ok | {error, term()}.
delete_search_index(Pid, Index) ->
    delete_search_index(Pid, Index, []).

%% @doc Delete a search index.
-spec delete_search_index(pid(), binary(), search_admin_opts()) ->
                    ok | {error, term()}.
delete_search_index(Pid, Index, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, default_timeout(search_timeout)),
    Req = #rpbyokozunaindexdeletereq{name = Index},
    call_infinity(Pid, {req, Req, Timeout}).

-spec set_search_index(pid(), bucket() | bucket_and_type(), binary()) ->
                    ok | {error, term()}.
set_search_index(Pid, Bucket, Index) ->
    set_bucket(Pid, Bucket, [{search_index, Index}]).


%% Deprecated, argument explosion functions for indexes

%% @doc Execute a secondary index equality query.
%%
%% @deprecated use {@link get_index_eq/4}
%% @see get_index_eq/4
-spec get_index(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(Pid, Bucket, Index, Key) ->
    get_index_eq(Pid, Bucket, Index, Key).

%% @doc Execute a secondary index equality query with specified
%% timeouts.
%%
%% @deprecated use {@link get_index_eq/5}
%% @see get_index_eq/5
-spec get_index(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(Pid, Bucket, Index, Key, Timeout, _CallTimeout) ->
    get_index_eq(Pid, Bucket, Index, Key, [{timeout, Timeout}]).

%% @doc Execute a secondary index range query.
%%
%% @deprecated use {@link get_index_range/5}
%% @see get_index_range/5
-spec get_index(pid(), bucket(), binary() | secondary_index_id(), key() | integer(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(Pid, Bucket, Index, StartKey, EndKey) ->
    get_index_range(Pid, Bucket, Index, StartKey, EndKey).

%% @doc Execute a secondary index range query with specified
%% timeouts.
%%
%% @deprecated use {@link get_index_range/6}
%% @see get_index_range/6
-spec get_index(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer() | list(),
                key() | integer() | list(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(Pid, Bucket, Index, StartKey, EndKey, Timeout, _CallTimeout) ->
    get_index_range(Pid, Bucket, Index, StartKey, EndKey, [{timeout, Timeout}]).

%% @doc Execute a secondary index equality query.
%% equivalent to all defaults for the options.
%% @see get_index_eq/5. for options and their effect
-spec get_index_eq(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index_eq(Pid, Bucket, Index, Key) ->
    get_index_eq(Pid, Bucket, Index, Key, []).

%% @doc Execute a secondary index equality query with specified options
%% <dl>
%% <dt>timeout:</dt> <dd>milliseconds to wait for a response from riak</dd>
%% <dt>stream:</dt> <dd> true | false. Stream results to calling process</dd>
%% <dt>continuation:</dt> <dd> The opaque, binary continuation returned from a previous query.
%%                             Requests the next results.</dd>
%% <dt>max_results:</dt> <dd>Positive integer, maximum number of results to return.
%%                           Expect a <code>continuation</code> in the response if this option is used.</dd>
%% </dl>
%% @end
-spec get_index_eq(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer(), index_opts()) ->
                       {ok, index_results()} | {error, term()}.
get_index_eq(Pid, Bucket, {binary_index, Name}, Key, Opts) when is_binary(Key) ->
    Index = list_to_binary(lists:append([Name, "_bin"])),
    get_index_eq(Pid, Bucket, Index, Key, Opts);
get_index_eq(Pid, Bucket, {integer_index, Name}, Key, Opts) when is_integer(Key) ->
    Index = list_to_binary(lists:append([Name, "_int"])),
    BinKey = list_to_binary(integer_to_list(Key)),
    get_index_eq(Pid, Bucket, Index, BinKey, Opts);
get_index_eq(Pid, Bucket, Index, Key, Opts) ->
    Timeout = proplists:get_value(timeout, Opts),
    MaxResults = proplists:get_value(max_results, Opts),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Stream = proplists:get_value(stream, Opts, false),
    Continuation = proplists:get_value(continuation, Opts),
    Cover = proplists:get_value(cover_context, Opts),
    ReturnBody = proplists:get_value(return_body, Opts),

    {T, B} = maybe_bucket_type(Bucket),

    Req = #rpbindexreq{type=T, bucket=B, index=Index, qtype=eq,
                       key=encode_2i(Key),
                       max_results=MaxResults,
                       pagination_sort=PgSort,
                       stream=Stream,
                       continuation=Continuation,
                       cover_context=Cover,
                       return_body=ReturnBody,
                       timeout=Timeout},
    Call = case Stream of
               true ->
                   ReqId = mk_reqid(),
                   {req, Req, Timeout, {ReqId, self()}};
               false ->
                   {req, Req, Timeout}
           end,
    call_infinity(Pid, Call).

%% @doc Execute a secondary index range query.
-spec get_index_range(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index_range(Pid, Bucket, Index, StartKey, EndKey) ->
    get_index_range(Pid, Bucket, Index, StartKey, EndKey, []).

%% @doc Execute a secondary index range query with specified options.
%% As well as the options documented for `get_index_eq/5', there is a further options
%% `{return_terms, boolean{}'. When `true' the indexed values will be returned
%% as well as the primary key. The formt of the returned values is
%% `{results, [{value, primary_key}]}'
%% @end
%% @see get_index_eq/5. for effect of options.
-spec get_index_range(pid(), bucket() | bucket_and_type(), binary() | secondary_index_id(), key() | integer() | list(),
                key() | integer() | list(), range_index_opts()) ->
                       {ok, index_results()} | {error, term()}.
get_index_range(Pid, Bucket, {binary_index, Name}, StartKey, EndKey, Opts) when is_binary(StartKey) andalso is_binary(EndKey) ->
    Index = list_to_binary(lists:append([Name, "_bin"])),
    get_index_range(Pid, Bucket, Index, StartKey, EndKey, Opts);
get_index_range(Pid, Bucket, {integer_index, Name}, StartKey, EndKey, Opts) when is_integer(StartKey) andalso is_integer(EndKey) ->
    Index = list_to_binary(lists:append([Name, "_int"])),
    BinStartKey = list_to_binary(integer_to_list(StartKey)),
    BinEndKey = list_to_binary(integer_to_list(EndKey)),
    get_index_range(Pid, Bucket, Index, BinStartKey, BinEndKey, Opts);
get_index_range(Pid, Bucket, Index, StartKey, EndKey, Opts) ->
    Timeout = proplists:get_value(timeout, Opts),
    ReturnTerms = proplists:get_value(return_terms, Opts),
    TermRegex = proplists:get_value(term_regex, Opts),
    MaxResults = proplists:get_value(max_results, Opts),
    PgSort = proplists:get_value(pagination_sort, Opts),
    Stream = proplists:get_value(stream, Opts, false),
    Continuation = proplists:get_value(continuation, Opts),
    Cover = proplists:get_value(cover_context, Opts),
    ReturnBody = proplists:get_value(return_body, Opts),

    {T, B} = maybe_bucket_type(Bucket),

    Req = #rpbindexreq{type=T, bucket=B, index=Index, qtype=range,
                       range_min=encode_2i(StartKey),
                       range_max=encode_2i(EndKey),
                       return_terms=ReturnTerms,
                       term_regex=TermRegex,
                       max_results=MaxResults,
                       pagination_sort = PgSort,
                       stream=Stream,
                       continuation=Continuation,
                       cover_context=Cover,
                       return_body=ReturnBody,
                       timeout=Timeout},
    Call = case Stream of
               true ->
                   ReqId = mk_reqid(),
                   {req, Req, Timeout, {ReqId, self()}};
               false ->
                   {req, Req, Timeout}
           end,
    call_infinity(Pid, Call).

encode_2i(Value) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value));
encode_2i(Value) when is_list(Value) ->
    list_to_binary(Value);
encode_2i(Value) when is_binary(Value) ->
    Value.

%% @doc secret function, do not use, or I come to your house and keeel you.
-spec cs_bucket_fold(pid(), bucket() | bucket_and_type(), cs_opts()) -> {ok, reference()} | {error, term()}.
cs_bucket_fold(Pid, Bucket, Opts) when is_pid(Pid), (is_binary(Bucket) orelse
                                                     is_tuple(Bucket)), is_list(Opts) ->
    Timeout = proplists:get_value(timeout, Opts),
    StartKey = proplists:get_value(start_key, Opts, <<>>),
    EndKey = proplists:get_value(end_key, Opts),
    MaxResults = proplists:get_value(max_results, Opts),
    StartIncl = proplists:get_value(start_incl, Opts, true),
    EndIncl = proplists:get_value(end_incl, Opts, false),
    Continuation = proplists:get_value(continuation, Opts),
    Cover = proplists:get_value(cover_context, Opts),

    {T, B} = maybe_bucket_type(Bucket),

    Req = #rpbcsbucketreq{type=T, bucket=B,
                          start_key=StartKey,
                          end_key=EndKey,
                          start_incl=StartIncl,
                          end_incl=EndIncl,
                          max_results=MaxResults,
                          continuation=Continuation,
                          cover_context=Cover,
                          timeout=Timeout},
    ReqId = mk_reqid(),
    Call = {req, Req, Timeout, {ReqId, self()}},
    call_infinity(Pid, Call).

%% @doc Return the default timeout for an operation if none is provided.
%%      Falls back to the default timeout.
-spec default_timeout(timeout_name()) -> timeout().
default_timeout(OpTimeout) ->
    case application:get_env(riakc, OpTimeout) of
        {ok, EnvTimeout} ->
            EnvTimeout;
        undefined ->
            case application:get_env(riakc, timeout) of
                {ok, Timeout} ->
                    Timeout;
                undefined ->
                    ?DEFAULT_PB_TIMEOUT
            end
    end.

%% @doc Send a pre-encoded msg over the protocol buffer connection
%% Returns {ok, Response} or {error, Reason}
-spec tunnel(pid(), msg_id(), iolist(), timeout()) -> {ok, binary()} | {error, term()}.
tunnel(Pid, MsgId, Pkt, Timeout) ->
    Req = {tunneled, MsgId, Pkt},
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc increment the pre-Riak 2 counter at `bucket', `key' by `amount'
-spec counter_incr(pid(), bucket() | bucket_and_type(), key(), integer()) -> ok.
counter_incr(Pid, Bucket, Key, Amount) ->
    counter_incr(Pid, Bucket, Key, Amount, []).

%% @doc increment the pre-Riak 2 counter at `Bucket', `Key' by `Amount'.
%% use the provided `write_quorum()' `Options' for the operation.
%% A counter increment is a lot like a riak `put' so the semantics
%% are the same for the given options.
-spec counter_incr(pid(), bucket() | bucket_and_type(), key(), integer(), [write_quorum()]) ->
    ok | {error, term()}.
counter_incr(Pid, Bucket, Key, Amount, Options) ->
    {_, B} = maybe_bucket_type(Bucket),
    Req = counter_incr_options(Options, #rpbcounterupdatereq{bucket=B, key=Key, amount=Amount}),
    call_infinity(Pid, {req, Req, default_timeout(put_timeout)}).

%% @doc get the current value of the pre-Riak 2 counter at `Bucket', `Key'.
-spec counter_val(pid(), bucket() | bucket_and_type(), key()) ->
                         {ok, integer()} | {error, notfound}.
counter_val(Pid, Bucket, Key) ->
    counter_val(Pid, Bucket, Key, []).

%% @doc get the current value of the pre-Riak 2 counter at `Bucket', `Key' using
%% the `read_qurom()' `Options' provided.
-spec counter_val(pid(), bucket() | bucket_and_type(), key(), [read_quorum()]) ->
                         {ok, integer()} | {error, term()}.
counter_val(Pid, Bucket, Key, Options) ->
    {_, B} = maybe_bucket_type(Bucket),
    Req = counter_val_options(Options, #rpbcountergetreq{bucket=B, key=Key}),
    call_infinity(Pid, {req, Req, default_timeout(get_timeout)}).


%% @doc Fetches the representation of a convergent datatype from Riak.
-spec fetch_type(pid(), bucket_and_type(), Key::binary()) ->
                        {ok, riakc_datatype:datatype()} | {error, Reason}
                            when Reason :: {notfound, Type::atom()} | term().
fetch_type(Pid, BucketAndType, Key) ->
    fetch_type(Pid, BucketAndType, Key, []).

%% @doc Fetches the representation of a convergent datatype from Riak,
%% using the given request options.
-spec fetch_type(pid(), bucket_and_type(), Key::binary(), [proplists:property()]) ->
                        {ok, riakc_datatype:datatype()} | {error, Reason}
                            when Reason :: {notfound, Type::atom()} | term().
fetch_type(Pid, BucketAndType, Key, Options) ->
    Req = riak_pb_dt_codec:encode_fetch_request(BucketAndType, Key, Options),
    call_infinity(Pid, {req, Req, default_timeout(get_timeout)}).

%% @doc Updates the convergent datatype in Riak with local
%% modifications stored in the container type.
-spec update_type(pid(), bucket_and_type(), Key::binary(), Update::riakc_datatype:update(term())) ->
                         ok | {ok, Key::binary()} | {ok, riakc_datatype:datatype()} |
                         {ok, Key::binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(Pid, BucketAndType, Key, Update) ->
    update_type(Pid, BucketAndType, Key, Update, []).

%% @doc Updates the convergent datatype in Riak with local
%% modifications stored in the container type, using the given request
%% options.
-spec update_type(pid(), bucket_and_type(), Key::binary(),
                  Update::riakc_datatype:update(term()), [proplists:property()]) ->
                         ok | {ok, Key::binary()} | {ok, riakc_datatype:datatype()} |
                         {ok, Key::binary(), riakc_datatype:datatype()} | {error, term()}.
update_type(_Pid, _BucketAndType, _Key, undefined, _Options) ->
    {error, unmodified};
update_type(Pid, BucketAndType, Key, {Type, Op, Context}, Options) ->
    Req = riak_pb_dt_codec:encode_update_request(BucketAndType, Key, {Type, Op, Context}, Options),
    call_infinity(Pid, {req, Req, default_timeout(put_timeout)}).

%% @doc Fetches, applies the given function to the value, and then
%% updates the datatype in Riak. If an existing value is not found,
%% but you want the updates to apply anyway, use the 'create' option.
-spec modify_type(pid(), fun((riakc_datatype:datatype()) -> riakc_datatype:datatype()),
                  bucket_and_type(), Key::binary(), [proplists:property()]) ->
                         ok | {ok, riakc_datatype:datatype()} | {error, term()}.
modify_type(Pid, Fun, BucketAndType, Key, ModifyOptions) ->
    Create = proplists:get_value(create, ModifyOptions, true),
    Options = proplists:delete(create, ModifyOptions),

    case fetch_type(Pid, BucketAndType, Key, Options) of
        {ok, Data} ->
            NewData = Fun(Data),
            Mod = riakc_datatype:module_for_term(NewData),
            update_type(Pid, BucketAndType, Key, Mod:to_op(NewData), Options);
        {error, {notfound, Type}} when Create ->
            %% Not found, but ok to create it
            Mod = riakc_datatype:module_for_type(Type),
            NewData = Fun(Mod:new()),
            update_type(Pid, BucketAndType, Key, Mod:to_op(NewData), Options);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get active preflist.
%% @equiv get_preflist(Pid, Bucket, Key, default_timeout(get_preflist_timeout))
-spec get_preflist(pid(), bucket() | bucket_and_type(), key()) -> {ok, preflist()}
                                                                      | {error, term()}.
get_preflist(Pid, Bucket, Key) ->
    get_preflist(Pid, Bucket, Key, default_timeout(get_preflist_timeout)).

%% @doc Get active preflist specifying a server side timeout.
%% @equiv get_preflist(Pid, Bucket, Key, default_timeout(get_preflist_timeout))
-spec get_preflist(pid(), bucket() | bucket_and_type(), key(), timeout()) -> {ok, preflist()}
                                                                                 | {error, term()}.
get_preflist(Pid, Bucket, Key, Timeout) ->
    {T, B} = maybe_bucket_type(Bucket),
    Req = #rpbgetbucketkeypreflistreq{type = T, bucket = B, key = Key},
    call_infinity(Pid, {req, Req, Timeout}).

%% @doc Get minimal coverage plan
%% @equiv get_coverage(Pid, Bucket, undefined)
-spec get_coverage(pid(), bucket()) -> {ok, term()}
                                           | {error, term()}.
get_coverage(Pid, Bucket) ->
    get_coverage(Pid, Bucket, undefined).

%% @doc Get parallel coverage plan if 3rd argument is >= 0
-spec get_coverage(pid(), bucket(), undefined | non_neg_integer()) -> {ok, term()}
                                                 | {error, term()}.
get_coverage(Pid, Bucket, MinPartitions) ->
    Timeout = default_timeout(get_coverage_timeout),
    {T, B} = maybe_bucket_type(Bucket),
    call_infinity(Pid,
                  {req, #rpbcoveragereq{type=T, bucket=B, min_partitions=MinPartitions},
                   Timeout}).

replace_coverage(Pid, Bucket, Cover) ->
    replace_coverage(Pid, Bucket, Cover, []).

replace_coverage(Pid, Bucket, Cover, Other) ->
    Timeout = default_timeout(get_coverage_timeout),
    {T, B} = maybe_bucket_type(Bucket),
    call_infinity(Pid,
                  {req, #rpbcoveragereq{type=T, bucket=B, replace_cover=Cover, unavailable_cover=Other},
                   Timeout}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

%% @private
init([Address, Port, InOptions]) ->
    %% If callback then send the startup of the child
    CallbackMod = proplists:get_value(mod_callback, InOptions),   
 
    Options = proplists:delete(mod_callback, InOptions), 

    %% Schedule a reconnect as the first action.  If the server is up then
    %% the handle_info(reconnect) will run before any requests can be sent.
    State = parse_options(Options, #state{address = Address,
                                          port = Port,
                                          queue = queue:new()}),
    case connect(State) of
        {error, Reason} when State#state.auto_reconnect /= true ->
            {stop, {tcp, Reason}};
        {error, _Reason} ->
            erlang:send_after(State#state.reconnect_interval, self(), reconnect),
            {ok, State};
        Ok ->
            % Only if ok then Mod:Fun
            case CallbackMod of
                undefined  -> ok;
                [Mod, Fun] -> 
                    Mod:Fun(self(), Address, Port) 
            end,
            Ok
    end.

%% @private
handle_call({req, Msg, Timeout}, From, State) when State#state.sock =:= undefined ->
    case State#state.queue_if_disconnected of
        true ->
            {noreply, queue_request(new_request(Msg, From, Timeout), State)};
        false ->
            {reply, {error, disconnected}, State}
    end;
handle_call({req, Msg, Timeout, Ctx}, From, State) when State#state.sock =:= undefined ->
    case State#state.queue_if_disconnected of
        true ->
            {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
        false ->
            {reply, {error, disconnected}, State}
    end;
handle_call({req, Msg, Timeout}, From, State) when State#state.active =/= undefined ->
    {noreply, queue_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State) when State#state.active =/= undefined ->
    {noreply, queue_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call({req, Msg, Timeout}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout), State)};
handle_call({req, Msg, Timeout, Ctx}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout, Ctx), State)};
handle_call(is_connected, _From, State) ->
    case State#state.sock of
        undefined ->
            {reply, {false, State#state.failed}, State};
        _ ->
            {reply, true, State}
    end;
handle_call({set_options, Options}, _From, State) ->
    {reply, ok, parse_options(Options, State)};
handle_call(stop, _From, State) ->
    _ = disconnect(State),
    {stop, normal, ok, State}.

%% @private
handle_info({tcp_error, _Socket, Reason}, State) ->
    error_logger:error_msg("PBC client TCP error for ~p:~p - ~p\n",
                           [State#state.address, State#state.port, Reason]),
    disconnect(State);
handle_info({tcp_closed, _Socket}, State) ->
    disconnect(State);
handle_info({ssl_error, _Socket, Reason}, State) ->
    error_logger:error_msg("PBC client SSL error for ~p:~p - ~p\n",
                           [State#state.address, State#state.port, Reason]),
    disconnect(State);
handle_info({ssl_closed, _Socket}, State) ->
    disconnect(State);
%% Make sure the two Socks match.  If a request timed out, but there was
%% a response queued up behind it we do not want to process it.  Instead
%% it should drop through and be ignored.
handle_info({Proto, Sock, Data}, State=#state{sock = Sock, active = Active})
        when Proto == tcp; Proto == ssl ->
    <<MsgCode:8, MsgData/binary>> = Data,
    Resp = case Active#request.msg of
        {tunneled, _MsgID} ->
            %% don't decode tunneled replies, we may not recognize the msgid
            {MsgCode, MsgData};
        _ ->
            decode(MsgCode, MsgData)
    end,
    NewState = case Resp of
        #rpberrorresp{} ->
            NewState1 = maybe_reply(on_error(Active, Resp, State)),
            dequeue_request(NewState1#state{active = undefined});
        _ ->
            case process_response(Active, Resp, State) of
                {reply, Response, NewState0} ->
                    %% Send reply and get ready for the next request - send the next request
                    %% if one is queued up
                    cancel_req_timer(Active#request.tref),
                    _ = send_caller(Response, NewState0#state.active),
                    dequeue_request(NewState0#state{active = undefined});
                {pending, NewState0} -> %% Request is still pending - do not queue up a new one
                    NewActive = restart_req_timer(Active),
                    NewState0#state{active = NewActive}
            end
    end,
    case State#state.transport of
        gen_tcp ->
            ok = inet:setopts(Sock, [{active, once}]);
        ssl ->
            ok = ssl:setopts(Sock, [{active, once}])
    end,
    {noreply, NewState};
handle_info({req_timeout, Ref}, State) ->
    case State#state.active of %%
        undefined ->
            {noreply, remove_queued_request(Ref, State)};
        Active ->
            case Ref == Active#request.ref of
                true ->  %% Matches the current operation
                    NewState = maybe_reply(on_timeout(State#state.active, State)),
                    disconnect(NewState#state{active = undefined});
                false ->
                    {noreply, remove_queued_request(Ref, State)}
            end
    end;
handle_info(reconnect, State) ->
    case connect(State) of
        {ok, NewState} ->
            {noreply, dequeue_request(NewState)};
        {error, Reason} ->
            %% Update the failed count and reschedule a reconnection
            NewState = State#state{failed = orddict:update_counter(Reason, 1, State#state.failed)},
            disconnect(NewState)
    end;
handle_info(_, State) ->
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ====================================================================
%% internal functions
%% ====================================================================

%% @private
decode(?TTB_MSG_CODE, MsgData) ->
    riak_ttb_codec:decode(MsgData);
decode(MsgCode, MsgData) ->
    riak_pb_codec:decode(MsgCode, MsgData).

%% @private
%% Parse options
parse_options([], State) ->
    %% Once all options are parsed, make sure auto_reconnect is enabled
    %% if queue_if_disconnected is enabled.
    case State#state.queue_if_disconnected of
        true ->
            State#state{auto_reconnect = true};
        _ ->
            State
    end;
parse_options([{connect_timeout, T}|Options], State) when is_integer(T) ->
    parse_options(Options, State#state{connect_timeout = T});
parse_options([{queue_if_disconnected,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{queue_if_disconnected = Bool});
parse_options([queue_if_disconnected|Options], State) ->
    parse_options([{queue_if_disconnected, true}|Options], State);
parse_options([{auto_reconnect,Bool}|Options], State) when
      Bool =:= true; Bool =:= false ->
    parse_options(Options, State#state{auto_reconnect = Bool});
parse_options([auto_reconnect|Options], State) ->
    parse_options([{auto_reconnect, true}|Options], State);
parse_options([{keepalive,Bool}|Options], State) when is_boolean(Bool) ->
    parse_options(Options, State#state{keepalive = Bool});
parse_options([keepalive|Options], State) ->
    parse_options([{keepalive, false}|Options], State);
parse_options([{credentials, User, Pass}|Options], State) ->
    parse_options(Options, State#state{credentials={User, Pass}});
parse_options([{certfile, File}|Options], State) ->
    parse_options(Options, State#state{certfile=File});
parse_options([{cacertfile, File}|Options], State) ->
    parse_options(Options, State#state{cacertfile=File});
parse_options([{keyfile, File}|Options], State) ->
    parse_options(Options, State#state{keyfile=File});
parse_options([{ssl_opts, Opts}|Options], State) ->
    parse_options(Options, State#state{ssl_opts=Opts}).

maybe_reply({reply, Reply, State}) ->
    Request = State#state.active,
    NewRequest = send_caller(Reply, Request),
    State#state{active = NewRequest};
maybe_reply({noreply, State}) ->
    State.

%% @private
%% Reply to caller - form clause first in case a ReqId/Client was passed
%% in as the context and gen_server:reply hasn't been called yet.
send_caller(Msg, #request{ctx = {ReqId, Client},
                          from = undefined}=Request) ->
    Client ! {ReqId, Msg},
    Request;
send_caller(Msg, #request{from = From}=Request) when From /= undefined ->
    gen_server:reply(From, Msg),
    Request#request{from = undefined}.

get_options([], Req) ->
    Req;
get_options([{basic_quorum, BQ} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{basic_quorum = BQ});
get_options([{notfound_ok, NFOk} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{notfound_ok = NFOk});
get_options([{r, R} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{r = riak_pb_kv_codec:encode_quorum(R)});
get_options([{pr, PR} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{pr = riak_pb_kv_codec:encode_quorum(PR)});
get_options([{timeout, T} | Rest], Req) when is_integer(T)->
    get_options(Rest, Req#rpbgetreq{timeout = T});
get_options([{timeout, _T} | _Rest], _Req) ->
    erlang:error(badarg);
get_options([{if_modified, VClock} | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{if_modified = VClock});
get_options([head | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{head = true});
get_options([deletedvclock | Rest], Req) ->
    get_options(Rest, Req#rpbgetreq{deletedvclock = true});
get_options([{n_val, N} | Rest], Req)
  when is_integer(N), N > 0 ->
    get_options(Rest, Req#rpbgetreq{n_val = N});
get_options([{sloppy_quorum, Bool} | Rest], Req)
  when Bool == true; Bool == false ->
    get_options(Rest, Req#rpbgetreq{sloppy_quorum = Bool});
get_options([{_, _} | _Rest], _Req) ->
    erlang:error(badarg).

put_options([], Req) ->
    Req;
put_options([{w, W} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{w = riak_pb_kv_codec:encode_quorum(W)});
put_options([{dw, DW} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{dw = riak_pb_kv_codec:encode_quorum(DW)});
put_options([{pw, PW} | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{pw = riak_pb_kv_codec:encode_quorum(PW)});
put_options([{timeout, T} | Rest], Req) when is_integer(T) ->
    put_options(Rest, Req#rpbputreq{timeout = T});
put_options([{timeout, _T} | _Rest], _Req) ->
    erlang:error(badarg);
put_options([return_body | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{return_body = true});
put_options([return_head | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{return_head = true});
put_options([if_not_modified | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{if_not_modified = true});
put_options([if_none_match | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{if_none_match = true});
put_options([asis | Rest], Req) ->
    put_options(Rest, Req#rpbputreq{asis = true});
put_options([{asis, Val} | Rest], Req) when is_boolean(Val) ->
    put_options(Rest, Req#rpbputreq{asis = Val});
put_options([{n_val, N} | Rest], Req)
  when is_integer(N), N > 0 ->
    put_options(Rest, Req#rpbputreq{n_val = N});
put_options([{sloppy_quorum, Bool} | Rest], Req)
  when Bool == true; Bool == false ->
    put_options(Rest, Req#rpbputreq{sloppy_quorum = Bool});
put_options([{_, _} | _Rest], _Req) ->
    erlang:error(badarg).


delete_options([], Req) ->
    Req;
delete_options([{rw, RW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{rw = riak_pb_kv_codec:encode_quorum(RW)});
delete_options([{r, R} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{r = riak_pb_kv_codec:encode_quorum(R)});
delete_options([{w, W} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{w = riak_pb_kv_codec:encode_quorum(W)});
delete_options([{pr, PR} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{pr = riak_pb_kv_codec:encode_quorum(PR)});
delete_options([{pw, PW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{pw = riak_pb_kv_codec:encode_quorum(PW)});
delete_options([{dw, DW} | Rest], Req) ->
    delete_options(Rest, Req#rpbdelreq{dw = riak_pb_kv_codec:encode_quorum(DW)});
delete_options([{timeout, T} | Rest], Req) when is_integer(T) ->
    delete_options(Rest, Req#rpbdelreq{timeout = T});
delete_options([{timeout, _T} | _Rest], _Req) ->
    erlang:error(badarg);
delete_options([{n_val, N} | Rest], Req)
  when is_integer(N), N > 0 ->
    delete_options(Rest, Req#rpbdelreq{n_val = N});
delete_options([{sloppy_quorum, Bool} | Rest], Req)
  when Bool == true; Bool == false ->
    delete_options(Rest, Req#rpbdelreq{sloppy_quorum = Bool});
delete_options([{_, _} | _Rest], _Req) ->
    erlang:error(badarg).

search_options([], Req) ->
    Req;
search_options([{rows, Rows} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{rows=Rows});
search_options([{start, Start} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{start=Start});
search_options([{sort, Sort} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{sort=Sort});
search_options([{filter, Filter} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{filter=Filter});
search_options([{df, DF} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{df=DF});
search_options([{op, OP} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{op=OP});
search_options([{fl, FL} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{fl=FL});
search_options([{presort, Presort} | Rest], Req) ->
    search_options(Rest, Req#rpbsearchqueryreq{presort=Presort});
search_options([{_, _} | _Rest], _Req) ->
    erlang:error(badarg).

counter_incr_options([], Req) ->
    Req;
counter_incr_options([{w, W} | Rest], Req) ->
    counter_incr_options(Rest, Req#rpbcounterupdatereq{w=riak_pb_kv_codec:encode_quorum(W)});
counter_incr_options([{dw, DW} | Rest], Req) ->
    counter_incr_options(Rest, Req#rpbcounterupdatereq{dw=riak_pb_kv_codec:encode_quorum(DW)});
counter_incr_options([{pw, PW} | Rest], Req) ->
    counter_incr_options(Rest, Req#rpbcounterupdatereq{pw=riak_pb_kv_codec:encode_quorum(PW)});
counter_incr_options([returnvalue | Rest], Req) ->
    counter_incr_options(Rest, Req#rpbcounterupdatereq{returnvalue=true});
counter_incr_options([_ | _Rest], _Req) ->
    erlang:error(badarg).

counter_val_options([], Req) ->
    Req;
counter_val_options([{basic_quorum, BQ} | Rest], Req) ->
    counter_val_options(Rest, Req#rpbcountergetreq{basic_quorum=BQ});
counter_val_options([{notfound_ok, NFOK} | Rest], Req) ->
    counter_val_options(Rest, Req#rpbcountergetreq{notfound_ok=NFOK});
counter_val_options([{r, R} | Rest], Req) ->
    counter_val_options(Rest, Req#rpbcountergetreq{r=riak_pb_kv_codec:encode_quorum(R)});
counter_val_options([{pr, PR} | Rest], Req) ->
    counter_val_options(Rest, Req#rpbcountergetreq{pr=riak_pb_kv_codec:encode_quorum(PR)});
counter_val_options([_ | _Rest], _Req) ->
    erlang:error(badarg).

%% Process response from the server - passes back in the request and
%% context the request was issued with.
%% Return noreply if the request is completed, but no reply needed
%%        reply if the request is completed with a reply to the caller
%%        pending if the request has not completed yet (streaming op)
%% @private
-spec process_response(#request{}, rpb_resp(), #state{}) ->
                              {reply, term(), #state{}} |
                              {pending, #state{}}.
process_response(#request{msg = rpbpingreq}, rpbpingresp, State) ->
    {reply, pong, State};
process_response(#request{msg = rpbgetclientidreq},
                 #rpbgetclientidresp{client_id = ClientId}, State) ->
    {reply, {ok, ClientId}, State};
process_response(#request{msg = #rpbsetclientidreq{}},
                 rpbsetclientidresp, State) ->
    {reply, ok, State};
process_response(#request{msg = rpbgetserverinforeq},
                 #rpbgetserverinforesp{node = Node, server_version = ServerVersion}, State) ->
    NodeInfo = case Node of
        undefined ->
            [];
        Node ->
            [{node, Node}]
    end,
    VersionInfo = case ServerVersion of
        undefined ->
            [];
        ServerVersion ->
            [{server_version, ServerVersion}]
    end,
    {reply, {ok, NodeInfo++VersionInfo}, State};

%% rpbgetreq
process_response(#request{msg = #rpbgetreq{}}, rpbgetresp, State) ->
    %% server just returned the rpbgetresp code - no message was encoded
    {reply, {error, notfound}, State};
process_response(#request{msg = #rpbgetreq{deletedvclock=true}},
                 #rpbgetresp{vclock=VC, content=[]}, State) ->
    %% server returned a notfound with a vector clock, meaning a tombstone
    {reply, {error, notfound, VC}, State};
process_response(#request{msg = #rpbgetreq{}}, #rpbgetresp{unchanged=true}, State) ->
    %% object was unchanged
    {reply, unchanged, State};
process_response(#request{msg = #rpbgetreq{type = Type, bucket = Bucket, key = Key}},
                 #rpbgetresp{content = RpbContents, vclock = Vclock}, State) ->
    Contents = riak_pb_kv_codec:decode_contents(RpbContents),
    B = maybe_make_bucket_type(Type, Bucket),
    {reply, {ok, riakc_obj:new_obj(B, Key, Vclock, Contents)}, State};

%% rpbputreq
process_response(#request{msg = #rpbputreq{}},
                 rpbputresp, State) ->
    %% server just returned the rpbputresp code - no message was encoded
    {reply, ok, State};
process_response(#request{msg = #rpbputreq{}},
                 #rpbputresp{key = Key, content=undefined, vclock=undefined},
                 State) when is_binary(Key) ->
    %% server generated a key and the client didn't request return_body, but
    %% the created key is returned
    {reply, {ok, Key}, State};
process_response(#request{msg = #rpbputreq{}},
                 #rpbputresp{key = Key, content=[], vclock=undefined},
                 State) when is_binary(Key) ->
    %% server generated a key and the client didn't request return_body, but
    %% the created key is returned
    {reply, {ok, Key}, State};
process_response(#request{msg = #rpbputreq{type = Type, bucket = Bucket, key = Key}},
                 #rpbputresp{content = RpbContents, vclock = Vclock,
                             key = NewKey}, State) ->
    Contents = riak_pb_kv_codec:decode_contents(RpbContents),
    ReturnKey = case NewKey of
                    undefined -> Key;
                    _ -> NewKey
                end,
    B = maybe_make_bucket_type(Type, Bucket),
    {reply, {ok, riakc_obj:new_obj(B, ReturnKey, Vclock, Contents)}, State};

process_response(#request{msg = #rpbdelreq{}},
                 rpbdelresp, State) ->
    %% server just returned the rpbdelresp code - no message was encoded
    {reply, ok, State};

process_response(#request{msg = #rpblistbucketsreq{}}=Request,
                 #rpblistbucketsresp{buckets = Buckets, done = undefined},
                 State) ->
    _ = send_caller({buckets, Buckets}, Request),
    {pending, State};

process_response(#request{msg = #rpblistbucketsreq{}},
                 #rpblistbucketsresp{done = true},
                 State) ->
    {reply, done, State};

process_response(#request{msg = #rpblistkeysreq{}}=Request,
                 #rpblistkeysresp{done = Done, keys = Keys}, State) ->
    _ = case Keys of
            undefined ->
                ok;
            _ ->
                %% Have to directly use send_caller as may want to reply with done below.
                send_caller({keys, Keys}, Request)
        end,
    case Done of
        true ->
            {reply, done, State};
        _ ->
            {pending, State}
    end;

process_response(#request{msg = #rpbgetbucketreq{}},
                 #rpbgetbucketresp{props = PbProps}, State) ->
    Props = riak_pb_codec:decode_bucket_props(PbProps),
    {reply, {ok, Props}, State};

process_response(#request{msg = #rpbgetbuckettypereq{}},
                 #rpbgetbucketresp{props = PbProps}, State) ->
    Props = riak_pb_codec:decode_bucket_props(PbProps),
    {reply, {ok, Props}, State};

process_response(#request{msg = #rpbsetbucketreq{}},
                 rpbsetbucketresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbsetbuckettypereq{}},
                 rpbsetbucketresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbmapredreq{content_type = ContentType}}=Request,
                 #rpbmapredresp{done = Done, phase=PhaseId, response=Data}, State) ->
    _ = case Data of
            undefined ->
                ok;
            _ ->
                Response = decode_mapred_resp(Data, ContentType),
                send_caller({mapred, PhaseId, Response}, Request)
        end,
    case Done of
        true ->
            {reply, done, State};
        _ ->
            {pending, State}
    end;

process_response(#request{msg = #rpbindexreq{}}, rpbindexresp, State) ->
    Results = ?INDEX_RESULTS{keys=[], continuation=undefined},
    {reply, {ok, Results}, State};
process_response(#request{msg = #rpbindexreq{stream=true, return_terms=Terms, return_body=Body}}=Request,
                 #rpbindexresp{results=Results, keys=Keys, done=Done, continuation=Cont}, State) ->
    ToSend = process_index_response(response_type(Terms, Body), Keys, Results),
    _ = send_caller(ToSend, Request),
    DoneResponse = {reply, {done, Cont}, State},
    case Done of
                true -> DoneResponse;
                _ -> {pending, State}
    end;
process_response(#request{msg = #rpbindexreq{return_terms=Terms, return_body=Body}}, #rpbindexresp{results=Results, keys=Keys, continuation=Cont}, State) ->
    StreamResponse = process_index_response(response_type(Terms, Body), Keys, Results),
    RegularResponse = index_stream_result_to_index_result(StreamResponse),
    RegularResponseWithContinuation = RegularResponse?INDEX_RESULTS{continuation=Cont},
    {reply, {ok, RegularResponseWithContinuation}, State};
process_response(#request{msg = #rpbindexreq{stream=true, bucket=Bucket}}=Request,
                 #rpbindexbodyresp{objects=Objects, done=Done, continuation=Cont}, State) ->
    ToSend =
        case Objects of
            undefined -> [];
            _ ->
                %% make client objects
                lists:foldr(fun(#rpbindexobject{key=Key,
                                                object=#rpbgetresp{content=Contents, vclock=VClock}}, Acc) ->
                                    DContents = riak_pb_kv_codec:decode_contents(Contents),
                                    [riakc_obj:new_obj(Bucket, Key, VClock, DContents) | Acc] end,
                            [],
                            Objects)
        end,
    _ = send_caller({ok,
                     ?INDEX_STREAM_BODY_RESULT{objects = ToSend}},
                    Request),
    DoneResponse = {reply, {done, Cont}, State},
    case Done of
        true -> DoneResponse;
        _ -> {pending, State}
    end;
process_response(#request{msg = #rpbindexreq{bucket=Bucket}},
                 #rpbindexbodyresp{objects=Objects, continuation=Cont}, State) ->
    ToSend =
        case Objects of
            undefined -> [];
            _ ->
                %% make client objects
                lists:foldr(fun(#rpbindexobject{key=Key,
                                                object=#rpbgetresp{content=Contents, vclock=VClock}}, Acc) ->
                                    DContents = riak_pb_kv_codec:decode_contents(Contents),
                                    [riakc_obj:new_obj(Bucket, Key, VClock, DContents) | Acc] end,
                            [],
                            Objects)
        end,
    {reply, {ok, ?INDEX_BODY_RESULTS{objects=ToSend, continuation=Cont}}, State};
process_response(#request{msg = #rpbcsbucketreq{}}, rpbcsbucketresp, State) ->
    {pending, State};
process_response(#request{msg = #rpbcsbucketreq{bucket=Bucket}}=Request, #rpbcsbucketresp{objects=Objects, done=Done, continuation=Cont}, State) ->
    %% TEMP - cs specific message for fold_objects
    ToSend =  case Objects of
                  undefined -> {ok, []};
                  _ ->
                      %% make client objects
                      CObjects = lists:foldr(fun(#rpbindexobject{key=Key,
                                                                 object=#rpbgetresp{content=Contents, vclock=VClock}}, Acc) ->
                                                     DContents = riak_pb_kv_codec:decode_contents(Contents),
                                                     [riakc_obj:new_obj(Bucket, Key, VClock, DContents) | Acc] end,
                                             [],
                                             Objects),
                      {ok, CObjects}
              end,
    _ = send_caller(ToSend, Request),
    DoneResponse = {reply, {done, Cont}, State},
    case Done of
        true -> DoneResponse;
        _ -> {pending, State}
    end;
process_response(#request{msg = #rpbsearchqueryreq{}}, prbsearchqueryresp, State) ->
    {reply, {error, notfound}, State};
process_response(#request{msg = #rpbsearchqueryreq{index=Index}},
                 #rpbsearchqueryresp{docs=PBDocs,max_score=MaxScore,
                                     num_found=NumFound}, State) ->
    Values = [ {Index, [ riak_pb_codec:decode_pair(Field) || Field <- Doc#rpbsearchdoc.fields] }
               || Doc <- PBDocs ],
    Result = #search_results{docs=Values, max_score=MaxScore, num_found=NumFound},
    {reply, {ok, Result}, State};

process_response(#request{msg=#rpbresetbucketreq{}}, rpbresetbucketresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbcounterupdatereq{returnvalue=true}},
                 #rpbcounterupdateresp{value=Value}, State) ->
    {reply, {ok, Value}, State};
process_response(#request{msg = #rpbcounterupdatereq{}},
                 rpbcounterupdateresp, State) ->
    %% server just returned the rpbcounterupdateresp code - no message was encoded
    {reply, ok, State};
process_response(#request{msg = #rpbcountergetreq{}},
                 rpbcountergetresp, State) ->
    {reply, {error, notfound}, State};
process_response(#request{msg = #rpbcountergetreq{}},
                 #rpbcountergetresp{value=Value}, State) ->
    {reply, {ok, Value}, State};

process_response(#request{msg = #dtfetchreq{}}, #dtfetchresp{}=Resp,
                 State) ->
    Reply = case riak_pb_dt_codec:decode_fetch_response(Resp) of
                {Type, Value, Context}  ->
                    Mod = riakc_datatype:module_for_type(Type),
                    {ok, Mod:new(Value, Context)};
                {notfound, _Type}=NF ->
                    {error, NF}
            end,
    {reply, Reply, State};

process_response(#request{msg = #dtupdatereq{}},
                 dtupdateresp,
                 State) ->
    {reply, ok, State};

process_response(#request{msg = #dtupdatereq{op=Op, return_body=RB}},
                 #dtupdateresp{}=Resp,
                 State) ->
    OpType = riak_pb_dt_codec:operation_type(Op),
    Reply = case riak_pb_dt_codec:decode_update_response(Resp, OpType, RB) of
                ok -> ok;
                {ok, Key} -> {ok, Key};
                {OpType, Value, Context} ->
                    Mod = riakc_datatype:module_for_type(OpType),
                    {ok, Mod:new(Value, Context)};
                {Key, {OpType, Value, Context}} when is_binary(Key) ->
                    Mod = riakc_datatype:module_for_type(OpType),
                    {ok, Key, Mod:new(Value, Context)}
            end,
    {reply, Reply, State};

process_response(#request{msg={tunneled,_MsgId}}, Reply, State) ->
    %% Tunneled msg response
    {reply, {ok, Reply}, State};

process_response(#request{msg = #rpbyokozunaschemaputreq{}},
                 rpbputresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbyokozunaindexputreq{}},
                 rpbputresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbyokozunaindexdeletereq{}},
                 rpbdelresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #rpbyokozunaindexgetreq{}},
                 rpbyokozunaindexgetresp, State) ->
    {reply, {ok, []}, State};

process_response(#request{msg = #rpbyokozunaindexgetreq{}},
                 #rpbyokozunaindexgetresp{index=Indexes}, State) ->
    Results = [[{index,Index#rpbyokozunaindex.name},
                {schema,Index#rpbyokozunaindex.schema},
                {n_val,Index#rpbyokozunaindex.n_val}]
        || Index <- Indexes ],
    {reply, {ok, Results}, State};

process_response(#request{msg = #rpbyokozunaschemagetreq{}},
                 #rpbyokozunaschemagetresp{schema=Schema}, State) ->
    Result = [{name,Schema#rpbyokozunaschema.name}, {content,Schema#rpbyokozunaschema.content}],
    {reply, {ok, Result}, State};

process_response(#request{msg = #rpbgetbucketkeypreflistreq{}},
                 #rpbgetbucketkeypreflistresp{preflist=Preflist}, State) ->
    Result = [#preflist_item{partition=T#rpbbucketkeypreflistitem.partition,
                             node=T#rpbbucketkeypreflistitem.node,
                             primary=T#rpbbucketkeypreflistitem.primary}
              || T <- Preflist],
    {reply, {ok, Result}, State};

process_response(#request{msg = #tsputreq{}},
                 #tsputresp{}, State) ->
    {reply, ok, State};
process_response(#request{msg = #tsputreq{}},
                 tsputresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #tsdelreq{}},
                 tsdelresp, State) ->
    {reply, ok, State};

process_response(#request{msg = #tslistkeysreq{}} = Request,
                 #tslistkeysresp{done = Done, keys = Keys}, State) ->
    _ = case Keys of
            undefined ->
                ok;
            _ ->
                CompoundKeys = riak_pb_ts_codec:decode_rows(Keys),
                send_caller({keys, CompoundKeys}, Request)
        end,
    case Done of
        true ->
            {reply, done, State};
        _ ->
            {pending, State}
    end;

process_response(#request{msg = #tsqueryreq{}},
                 tsqueryresp, State) ->
    {reply, tsqueryresp, State};
process_response(#request{msg = #tsqueryreq{}},
                 Result = {tsqueryresp, _},
                 State) ->
    {reply, Result, State};
process_response(#request{msg = #tsqueryreq{}},
                 Result = #tsqueryresp{},
                 State) ->
    {reply, Result, State};
process_response(#request{msg = #tscoveragereq{}},
                 #tscoverageresp{entries = E}, State) ->
    {reply, {ok, E}, State};
process_response(#request{msg = #rpbcoveragereq{}},
                 #rpbcoverageresp{entries = E}, State) ->
    {reply, {ok, E}, State};
process_response(#request{msg = #tsgetreq{}},
                 tsgetresp, State) ->
    {reply, tsgetresp, State};
process_response(#request{msg = #tsgetreq{}},
                 Result = {tsgetresp, _},
                 State) ->
    {reply, Result, State};
process_response(#request{msg = #tsgetreq{}},
                 Result = #tsgetresp{},
                 State) ->
    {reply, Result, State};
process_response(Request, Reply, State) ->
    %% Unknown request/response combo
    {reply, {error, {unknown_response, Request, Reply}}, State}.

%% Return `keys', `terms', or `objects' depending on the value of
%% `return_terms' and `return_body'. Values can be true, false, or
%% undefined.
response_type(_ReturnTerms, true) ->
    objects;
response_type(true, _ReturnBody) ->
    terms;
response_type(_ReturnTerms, _ReturnBody) ->
    keys.


%% Helper for index responses
-spec process_index_response('keys'|'terms'|'objects', list(), list()) ->
    index_stream_result().
process_index_response(keys, Keys, _) ->
    ?INDEX_STREAM_RESULT{keys=Keys};
process_index_response(_, [], Results) ->
    Res = [{V, K} ||  #rpbpair{key=V, value=K} <- Results],
    ?INDEX_STREAM_RESULT{terms=Res};
process_index_response(_, Keys, []) ->
    ?INDEX_STREAM_RESULT{keys=Keys}.

-spec index_stream_result_to_index_result(index_stream_result()) ->
    index_results().
index_stream_result_to_index_result(?INDEX_STREAM_RESULT{keys=Keys,
                                                         terms=Terms}) ->
    ?INDEX_RESULTS{keys=Keys,
                   terms=Terms}.

%% Called after sending a message - supports returning a
%% request id for streaming calls
%% @private
after_send(#request{msg = #rpblistbucketsreq{}, ctx = {ReqId, _Client}},
           State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #rpblistkeysreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #tslistkeysreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #rpbmapredreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #rpbindexreq{stream=true}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(#request{msg = #rpbcsbucketreq{}, ctx = {ReqId, _Client}}, State) ->
    {reply, {ok, ReqId}, State};
after_send(_Request, State) ->
    {noreply, State}.

%% Called on timeout for an operation
%% @private
on_timeout(_Request, State) ->
    {reply, {error, timeout}, State}.
%%
%% Called after receiving an error message - supports reruning
%% an error for streaming calls
%% @private
on_error(_Request, ErrMsg, State) ->
    {reply, fmt_err_msg(ErrMsg), State}.

%% Format the PB encoded error message
fmt_err_msg(ErrMsg) ->
    case ErrMsg#rpberrorresp.errcode of
        Code when Code =:= 0; Code =:= 1; Code =:= undefined ->
            {error, ErrMsg#rpberrorresp.errmsg};
        Code ->
            {error, {Code, ErrMsg#rpberrorresp.errmsg}}
    end.

%% deliberately crash if the handling an error response after
%% the client has been replied to

%% Common code for sending a single bucket or multiple inputs map/request
%% @private
send_mapred_req(Pid, MapRed, ClientPid) ->
    ReqMsg = #rpbmapredreq{request = encode_mapred_req(MapRed),
                           content_type = <<"application/x-erlang-binary">>},
    ReqId = mk_reqid(),
    Timeout = proplists:get_value(timeout, MapRed, default_timeout(mapred_timeout)),
    Timeout1 = if
           is_integer(Timeout) ->
               %% Add an extra 100ms to the mapred timeout and use that
               %% for the socket timeout. This should give the
               %% map/reduce a chance to fail and let us know.
               Timeout + 100;
           true ->
               Timeout
           end,
    call_infinity(Pid, {req, ReqMsg, Timeout1, {ReqId, ClientPid}}).

%% @private
%% Make a new request that can be sent or queued
new_request({Msg, {msgopts, Options}}, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref,
             msg = Msg,
             from = From,
             timeout = Timeout,
             tref = create_req_timer(Timeout, Ref),
             opts = Options};
new_request(Msg, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref, msg = Msg, from = From, timeout = Timeout,
             tref = create_req_timer(Timeout, Ref), opts = []}.
new_request(Msg, From, Timeout, Context) ->
    Ref = make_ref(),
    #request{ref =Ref, msg = Msg, from = From, ctx = Context, timeout = Timeout,
             tref = create_req_timer(Timeout, Ref), opts = []}.

%% @private
%% Create a request timer if desired, otherwise return undefined.
create_req_timer(infinity, _Ref) ->
    undefined;
create_req_timer(undefined, _Ref) ->
    undefined;
create_req_timer(Msecs, Ref) ->
    erlang:send_after(Msecs, self(), {req_timeout, Ref}).

%% @private
%% Cancel a request timer made by create_timer/2
cancel_req_timer(undefined) ->
    ok;
cancel_req_timer(Tref) ->
    _ = erlang:cancel_timer(Tref),
    ok.

%% @private
%% Restart a request timer
-spec restart_req_timer(#request{}) -> #request{}.
restart_req_timer(Request) ->
    case Request#request.tref of
        undefined ->
            Request;
        Tref ->
            cancel_req_timer(Tref),
            NewTref = create_req_timer(Request#request.timeout,
                                       Request#request.ref),
            Request#request{tref = NewTref}
    end.

%% @private
%% Connect the socket if disconnected
connect(State) when State#state.sock =:= undefined ->
    #state{address = Address, port = Port, connects = Connects} = State,
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4},
                          {keepalive, State#state.keepalive}],
                         State#state.connect_timeout) of
        {ok, Sock} ->
            State1 = State#state{sock = Sock, connects = Connects+1,
                                 reconnect_interval = ?FIRST_RECONNECT_INTERVAL},
            case State#state.credentials of
                undefined ->
                    {ok, State1};
                _ ->
                    start_tls(State1)
            end;
        Error ->
            Error
    end.

-spec start_tls(#state{}) -> {ok, #state{}} | {error, term()}.
start_tls(State=#state{sock=Sock}) ->
    %% Send STARTTLS
    StartTLSCode = riak_pb_codec:msg_code(rpbstarttls),
    ok = gen_tcp:send(Sock, <<StartTLSCode:8>>),
    receive
        {tcp_error, Sock, Reason} ->
            {error, Reason};
        {tcp_closed, Sock} ->
            {error, closed};
        {tcp, Sock, Data} ->
            <<MsgCode:8, MsgData/binary>> = Data,
            case riak_pb_codec:decode(MsgCode, MsgData) of
                rpbstarttls ->
                    Options = [{verify, verify_peer},
                               {cacertfile, State#state.cacertfile}] ++
                              [{K, V} || {K, V} <- [{certfile,
                                                     State#state.certfile},
                                                    {keyfile,
                                                     State#state.keyfile}],
                                         V /= undefined] ++
                              State#state.ssl_opts,
                    case ssl:connect(Sock, Options, 1000) of
                        {ok, SSLSock} ->
                            ok = ssl:setopts(SSLSock, [{active, once}]),
                            start_auth(State#state{sock=SSLSock, transport=ssl});
                        {error, Reason2} ->
                            {error, Reason2}
                    end;
                #rpberrorresp{} ->
                    %% Server doesn't know about STARTTLS or security is
                    %% disabled. We can't fall back to the regular old
                    %% protocol here because then SSL could be stripped by a
                    %% man-in-the-middle proxy that presents insecure
                    %% communication to the client, but does secure
                    %% communication to the server.
                    {error, no_security}
            end
    end.

start_auth(State=#state{credentials={User,Pass}, sock=Sock}) ->
    ok = ssl:send(Sock, riak_pb_codec:encode(#rpbauthreq{user=User,
                                                         password=Pass})),
    receive
        {ssl_error, Sock, Reason} ->
            {error, Reason};
        {ssl_closed, Sock} ->
            {error, closed};
        {ssl, Sock, Data} ->
            <<MsgCode:8, MsgData/binary>> = Data,
            case riak_pb_codec:decode(MsgCode, MsgData) of
                rpbauthresp ->
                    ok = ssl:setopts(Sock, [{active, once}]),
                    {ok, State};
                #rpberrorresp{} = Err ->
                    fmt_err_msg(Err)
            end
    end.

%% @private
%% Disconnect socket if connected
disconnect(State) ->
    %% Tell any pending requests we've disconnected
    _ = case State#state.active of
            undefined ->
                ok;
            Request ->
                send_caller({error, disconnected}, Request)
        end,

    %% Make sure the connection is really closed
    case State#state.sock of
        undefined ->
            ok;
        Sock ->
            Transport = State#state.transport,
            Transport:close(Sock)
    end,

    %% Decide whether to reconnect or exit
    NewState = State#state{sock = undefined, active = undefined},
    case State#state.auto_reconnect of
        true ->
            %% Schedule the reconnect message and return state
            erlang:send_after(State#state.reconnect_interval, self(), reconnect),
            {noreply, increase_reconnect_interval(NewState)};
        false ->
            {stop, disconnected, NewState}
    end.

%% Double the reconnect interval up to the maximum
increase_reconnect_interval(State) ->
    case State#state.reconnect_interval of
        Interval when Interval < ?MAX_RECONNECT_INTERVAL ->
            NewInterval = min(Interval+Interval, ?MAX_RECONNECT_INTERVAL),
            State#state{reconnect_interval = NewInterval};
        _ ->
            State
    end.

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Request0, State) when State#state.active =:= undefined ->
    {Request, Pkt} = encode_request_message(Request0),
    Transport = State#state.transport,
    case Transport:send(State#state.sock, Pkt) of
        ok ->
            maybe_reply(after_send(Request, State#state{active = Request}));
        {error, Reason} ->
            error_logger:warning_msg("Socket error while sending riakc request: ~p.", [Reason]),
            Transport:close(State#state.sock),
            maybe_enqueue_and_reconnect(Request, State#state{sock=undefined})
    end.

%% @private
encode(Msg=#tsputreq{}, true) ->
    riak_ttb_codec:encode(Msg);
encode(Msg=#tsgetreq{}, true) ->
    riak_ttb_codec:encode(Msg);
encode(Msg=#tsqueryreq{}, true) ->
    riak_ttb_codec:encode(Msg);
encode(Msg, _UseTTB) ->
    riak_pb_codec:encode(Msg).

%% Already encoded (for tunneled messages), but must provide Message Id
%% for responding to the second form of send_request.
encode_request_message(#request{msg={tunneled,MsgId,Pkt}}=Req) ->
    {Req#request{msg={tunneled,MsgId}},[MsgId|Pkt]};
%% Unencoded Request (the normal PB client path)
encode_request_message(#request{msg=Msg,opts=Opts}=Req) ->
    UseTTB = proplists:get_value(use_ttb, Opts, true),
    {Req, encode(Msg, UseTTB)}.

%% If the socket was closed, see if we can enqueue the request and
%% trigger a reconnect. Otherwise, return an error to the requestor.
maybe_enqueue_and_reconnect(Request, State) ->
    maybe_reconnect(State),
    enqueue_or_reply_error(Request, State).

%% Trigger an immediate reconnect if automatic reconnection is
%% enabled.
maybe_reconnect(#state{auto_reconnect=true}) -> self() ! reconnect;
maybe_reconnect(_) -> ok.

%% If we can queue while disconnected, do so, otherwise tell the
%% caller that the socket was disconnected.
enqueue_or_reply_error(Request, #state{queue_if_disconnected=true}=State) ->
    queue_request(Request, State);
enqueue_or_reply_error(Request, State) ->
    _ = send_caller({error, disconnected}, Request),
    State.

%% Queue up a request if one is pending
%% @private
queue_request(Request, State) ->
    State#state{queue = queue:in(Request, State#state.queue)}.

%% Try and dequeue request and send onto the server if one is waiting
%% @private
dequeue_request(State) ->
    case queue:out(State#state.queue) of
        {empty, _} ->
            State;
        {{value, Request}, Q2} ->
            send_request(Request, State#state{queue = Q2})
    end.

%% Remove a queued request by reference - returns same queue if ref not present
%% @private
remove_queued_request(Ref, State) ->
    L = queue:to_list(State#state.queue),
    case lists:keytake(Ref, #request.ref, L) of
        false -> % Ref not queued up
            State;
        {value, Req, L2} ->
            {reply, Reply, NewState} = on_timeout(Req, State),
            _ = send_caller(Reply, Req),
            NewState#state{queue = queue:from_list(L2)}
    end.

%% @private
-ifdef(deprecated_19).
mk_reqid() -> erlang:phash2(crypto:strong_rand_bytes(10)). % only has to be unique per-pid
-else.
mk_reqid() -> erlang:phash2(crypto:rand_bytes(10)). % only has to be unique per-pid
-endif.

%% @private
wait_for_mapred(ReqId, Timeout) ->
    wait_for_mapred_first(ReqId, Timeout).

%% Wait for the first mapred result, so we know at least one phase
%% that will be delivering results.
wait_for_mapred_first(ReqId, Timeout) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, []};
        {mapred, Phase, Res} ->
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, []));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, []}}
    end.

%% So far we have only received results from one phase.  This method
%% of accumulating a single phases's outputs will be more efficient
%% than the repeated orddict:append_list/3 used when accumulating
%% outputs from multiple phases.
wait_for_mapred_one(ReqId, Timeout, Phase, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_one(Phase, Acc)};
        {mapred, Phase, Res} ->
            %% still receiving for just one phase
            wait_for_mapred_one(ReqId, Timeout, Phase,
                                acc_mapred_one(Res, Acc));
        {mapred, NewPhase, Res} ->
            %% results from a new phase have arrived - track them all
            Dict = [{NewPhase, Res},{Phase, Acc}],
            wait_for_mapred_many(ReqId, Timeout, Dict);
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_one(Phase, Acc)}}
    end.

%% Single-phase outputs are kept as a reverse list of results.
acc_mapred_one([R|Rest], Acc) ->
    acc_mapred_one(Rest, [R|Acc]);
acc_mapred_one([], Acc) ->
    Acc.

finish_mapred_one(Phase, Acc) ->
    [{Phase, lists:reverse(Acc)}].

%% Tracking outputs from multiple phases.
wait_for_mapred_many(ReqId, Timeout, Acc) ->
    case receive_mapred(ReqId, Timeout) of
        done ->
            {ok, finish_mapred_many(Acc)};
        {mapred, Phase, Res} ->
            wait_for_mapred_many(
              ReqId, Timeout, acc_mapred_many(Phase, Res, Acc));
        {error, _}=Error ->
            Error;
        timeout ->
            {error, {timeout, finish_mapred_many(Acc)}}
    end.

%% Many-phase outputs are kepts as a proplist of reversed lists of
%% results.
acc_mapred_many(Phase, Res, Acc) ->
    case lists:keytake(Phase, 1, Acc) of
        {value, {Phase, PAcc}, RAcc} ->
            [{Phase,acc_mapred_one(Res,PAcc)}|RAcc];
        false ->
            [{Phase,acc_mapred_one(Res,[])}|Acc]
    end.

finish_mapred_many(Acc) ->
    [ {P, lists:reverse(A)} || {P, A} <- lists:keysort(1, Acc) ].

%% Receive one mapred message.
-spec receive_mapred(req_id(), timeout()) ->
         done | {mapred, integer(), [term()]} | {error, term()} | timeout.
receive_mapred(ReqId, Timeout) ->
    receive {ReqId, Msg} ->
            %% Msg should be `done', `{mapred, Phase, Results}', or
            %% `{error, Reason}'
            Msg
    after Timeout ->
            timeout
    end.


%% Encode the MapReduce request using term to binary
%% @private
-spec encode_mapred_req(term()) -> binary().
encode_mapred_req(Req) ->
    term_to_binary(Req).

%% Decode a partial phase response
%% @private
-spec decode_mapred_resp(binary(), binary()) -> term().
decode_mapred_resp(Data, <<"application/x-erlang-binary">>) ->
    try
        binary_to_term(Data)
    catch
        _:Error -> % On error, merge in with the other results
            [{error, Error}]
    end.

maybe_bucket_type({Type, Bucket}) ->
    {Type, Bucket};
maybe_bucket_type(Bucket) ->
    {undefined, Bucket}.

maybe_make_bucket_type(undefined, Bucket) ->
    Bucket;
maybe_make_bucket_type(Type, Bucket) ->
    {Type, Bucket}.

%% @private
%% @doc Create/Set record based on NVal value or throw an error.
-spec set_index_create_req_nval(pos_integer()|undefined, binary(), binary()) ->
                                 #rpbyokozunaindexputreq{}.
set_index_create_req_nval(NVal, Index, SchemaName) when is_integer(NVal) ->
    #rpbyokozunaindexputreq{index = #rpbyokozunaindex{
                                       name = Index,
                                       schema = SchemaName,
                                       n_val = NVal}};
set_index_create_req_nval(NVal, Index, SchemaName) when NVal =:= undefined ->
    #rpbyokozunaindexputreq{index = #rpbyokozunaindex{
                                       name = Index,
                                       schema = SchemaName}};
set_index_create_req_nval(NVal, _Index, _SchemaName)
  when not is_integer(NVal); NVal =/= undefined ->
    erlang:error(badarg).

%% @private
%% @doc Set record based on Timeout value or throw an error.
-spec set_index_create_req_timeout(timeout(), #rpbyokozunaindexputreq{}) ->
                                    #rpbyokozunaindexputreq{}.
set_index_create_req_timeout(Timeout, Req) when is_integer(Timeout) ->
    Req#rpbyokozunaindexputreq{timeout = Timeout};
set_index_create_req_timeout(Timeout, Req) when Timeout =:= infinity ->
    Req;
set_index_create_req_timeout(Timeout, _Req) when not is_integer(Timeout) ->
    erlang:error(badarg).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Check the reconnect interval increases up to the max and sticks there
increase_reconnect_interval_test() ->
    increase_reconnect_interval_test(#state{}).

increase_reconnect_interval_test(State) ->
    CurrInterval = State#state.reconnect_interval,
    NextState = increase_reconnect_interval(State),
    case NextState#state.reconnect_interval of
        ?MAX_RECONNECT_INTERVAL ->
            FinalState = increase_reconnect_interval(NextState),
            ?assertEqual(?MAX_RECONNECT_INTERVAL, FinalState#state.reconnect_interval);
        NextInterval->
            ?assert(NextInterval > CurrInterval),
            increase_reconnect_interval_test(NextState)
    end.

-endif.
