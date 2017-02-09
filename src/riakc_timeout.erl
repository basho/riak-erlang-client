%% ------------------------------------------------------------------------
%% Copyright 2017-present Basho Technologies, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% ------------------------------------------------------------------------

-module(riakc_timeout).

-export([default/1, timeouts/2]).

-define(DEFAULT_PB_TIMEOUT, 60000).
-define(DEFAULT_ADDITIONAL_CLIENT_TIMEOUT, 500).

-type timeout_name() :: atom().

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-spec timeouts(timeout_name(), proplists:proplist()) -> {timeout(), timeout()}.
timeouts(stream_list_buckets_timeout=Op, Opts) ->
    get_timeouts(Op, Opts);
timeouts(stream_list_keys_timeout=Op, Opts) ->
    get_timeouts(Op, Opts);
timeouts(create_search_index_timeout=Op, Opts) ->
    get_timeouts(Op, Opts);
timeouts(mapred_timeout=Op, Opts) ->
    get_timeouts(Op, Opts).

-spec default(timeout_name()) -> timeout().
default(ping_timeout=Op) ->
    get_default(Op);
default(get_client_id_timeout=Op) ->
    get_default(Op);
default(set_client_id_timeout=Op) ->
    get_default(Op);
default(get_server_info_timeout=Op) ->
    get_default(Op);
default(get_timeout=Op) ->
    get_default(Op);
default(put_timeout=Op) ->
    get_default(Op);
default(delete_timeout=Op) ->
    get_default(Op);
default(stream_list_keys_timeout=Op) ->
    get_default(Op);
default(stream_list_buckets_timeout=Op) ->
    get_default(Op);
default(get_bucket_timeout=Op) ->
    get_default(Op);
default(get_bucket_type_timeout=Op) ->
    get_default(Op);
default(set_bucket_timeout=Op) ->
    get_default(Op);
default(reset_bucket_timeout=Op) ->
    get_default(Op);
default(set_bucket_type_timeout=Op) ->
    get_default(Op);
default(mapred_timeout=Op) ->
    get_default(Op);
default(mapred_bucket_timeout=Op) ->
    get_default(Op);
default(mapred_bucket_call_timeout=Op) ->
    get_default(Op);
default(search_timeout=Op) ->
    get_default(Op);
default(search_call_timeout=Op) ->
    get_default(Op);
default(create_search_index_timeout=Op) ->
    get_default(Op);
default(get_preflist_timeout=Op) ->
    get_default(Op);
default(timeseries=Op) ->
    get_default(Op).

-spec get_timeouts(timeout_name(), proplists:proplist()) -> {timeout(), timeout()}.
get_timeouts(Op, Opts) ->
    ST = proplists:get_value(timeout, Opts, default(Op)),
    CT = get_client_timeout(ST),
    {CT, ST}.

-spec get_client_timeout(undefined|timeout()) -> timeout().
get_client_timeout(infinity) ->
    infinity;
get_client_timeout(undefined) ->
    infinity;
get_client_timeout(ST) when is_integer(ST) ->
    ST + ?DEFAULT_ADDITIONAL_CLIENT_TIMEOUT.

-spec get_default(timeout_name()) -> timeout().
get_default(Op) ->
    case application:get_env(riakc, Op) of
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

-ifdef(TEST).

-define(TIMEOUT_NAMES,
	[ping_timeout, get_client_id_timeout,
	 set_client_id_timeout, get_server_info_timeout,
	 get_timeout, put_timeout, delete_timeout,
	 stream_list_buckets_timeout,
	 stream_list_keys_timeout,
	 get_bucket_timeout, set_bucket_timeout, reset_bucket_timeout,
	 get_bucket_type_timeout, set_bucket_type_timeout,
	 mapred_timeout,
     mapred_bucket_timeout, mapred_bucket_call_timeout, search_timeout, search_call_timeout,
	 create_search_index_timeout,
     get_preflist_timeout, timeseries]).

default_test_() ->
    [?_assertEqual(?DEFAULT_PB_TIMEOUT, default(TN)) || TN <- ?TIMEOUT_NAMES].

-endif.
