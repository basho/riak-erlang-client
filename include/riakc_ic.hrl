%%%-------------------------------------------------------------------
%%% @author paulhunt
%%% @copyright (C) 2019, bet365
%%% @doc
%%%
%%% @end
%%% Created : 17. Dec 2019 13:28
%%%-------------------------------------------------------------------
-author("paulhunt").

%% Request Macros
-define(GET_REQUEST, get_request).
-define(PUT_REQUEST, put_request).
-define(DELETE_REQUEST, delete_request).
-define(DELETE_VCLOCK_REQUEST, delete_vclock_request).
-define(DELETE_OBJ_REQUEST, delete_obj_request).
-define(LIST_BUCKETS_REQUEST, list_buckets_request).
-define(STREAM_LIST_BUCKETS_REQUEST, stream_list_buckets_request).
-define(LEGACY_LIST_BUCKETS_REQUEST, legacy_list_buckets_request).
-define(LIST_KEYS_REQUEST, list_keys_request).
-define(STREAM_LIST_KEYS_REQUEST, stream_list_keys_request).
-define(GET_BUCKET_REQUEST, get_bucket_request).
-define(GET_BUCKET_TYPE_REQUEST, get_bucket_type_request).
-define(SET_BUCKET_REQUEST, set_bucket_request).
-define(SET_BUCKET_TYPE_REQUEST, set_bucket_type_request).
-define(RESET_BUCKET_REQUEST, reset_bucket_request).
-define(MAPRED_REQUEST, map_request).

%% Timeout Macros
-define(GET_TIMEOUT, get_timeout).
-define(PUT_TIMEOUT, put_timeout).
-define(DELETE_TIMEOUT, delete_timeout).
-define(DELETE_VCLOCK_TIMEOUT, delete_vclock_timeout).
-define(DELETE_OBJ_TIMEOUT, delete_obj_timeout).
-define(GET_BUCKET_TIMEOUT, get_bucket_timeout).
-define(GET_BUCKET_CALL_TIMEOUT, get_bucket_call_timeout).
-define(GET_BUCKET_TYPE_TIMEOUT, get_bucket_type_timeout).
-define(SET_BUCKET_TIMEOUT, set_bucket_timeout).
-define(SET_BUCKET_CALL_TIMEOUT, set_bucket_call_timeout).
-define(SET_BUCKET_TYPE_TIMEOUT, set_bucket_type_timeout).
-define(RESET_BUCKET_TIMEOUT, reset_bucket_timeout).
-define(RESET_BUCKET_CALL_TIMEOUT, reset_bucket_call_timeout).
-define(MAPRED_TIMEOUT, mapred_timeout).
-define(MAPRED_CALL_TIMEOUT, mapred_call_timeout).