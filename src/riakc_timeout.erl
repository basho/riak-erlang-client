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

-export([default/1]).

-define(DEFAULT_PB_TIMEOUT, 60000).
-define(DEFAULT_ADDITIONAL_CLIENT_TIMEOUT, 500).

-type timeout_name() :: atom().

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
default(list_buckets_timeout=Op) ->
    get_default(Op);
default(list_buckets_call_timeout=Op) ->
    get_default(Op);
default(list_keys_timeout=Op) ->
    get_default(Op);
default(stream_list_keys_timeout=Op) ->
    get_default(Op);
default(stream_list_keys_call_timeout=Op) ->
    get_default(Op);
default(get_bucket_timeout=Op) ->
    get_default(Op);
default(get_bucket_call_timeout=Op) ->
    get_default(Op);
default(set_bucket_timeout=Op) ->
    get_default(Op);
default(set_bucket_call_timeout=Op) ->
    get_default(Op);
default(mapred_timeout=Op) ->
    get_default(Op);
default(mapred_call_timeout=Op) ->
    get_default(Op);
default(mapred_stream_timeout=Op) ->
    get_default(Op);
default(mapred_stream_call_timeout=Op) ->
    get_default(Op);
default(mapred_bucket_timeout=Op) ->
    get_default(Op);
default(mapred_bucket_call_timeout=Op) ->
    get_default(Op);
default(mapred_bucket_stream_call_timeout=Op) ->
    get_default(Op);
default(search_timeout=Op) ->
    get_default(Op);
default(search_call_timeout=Op) ->
    get_default(Op);
default(get_preflist_timeout=Op) ->
    get_default(Op).

%% @doc Return the default timeout for an operation if none is provided.
%%      Falls back to the default timeout.
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
	 list_buckets_timeout, list_buckets_call_timeout,
	 list_keys_timeout, stream_list_keys_timeout,
	 stream_list_keys_call_timeout, get_bucket_timeout,
	 get_bucket_call_timeout, set_bucket_timeout,
	 set_bucket_call_timeout, mapred_timeout,
	 mapred_call_timeout, mapred_stream_timeout,
	 mapred_stream_call_timeout, mapred_bucket_timeout,
	 mapred_bucket_call_timeout, mapred_bucket_stream_call_timeout,
	 search_timeout, search_call_timeout, get_preflist_timeout]).

default_test() ->
    [?assertEqual(?DEFAULT_PB_TIMEOUT, default(TN)) || TN <- ?TIMEOUT_NAMES].

-endif.
