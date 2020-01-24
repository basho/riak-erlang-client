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
-define(GET_REQ, get_req).
-define(PUT_REQ, put_req).
-define(DELETE_REQ, delete_req).
-define(DELETE_VCLOCK_REQ, delete_vclock_req).
-define(DELETE_OBJ_REQ, delete_obj_req).
-define(LIST_BUCKETS_REQ, list_buckets_req).
-define(STREAM_LIST_BUCKETS_REQ, stream_list_buckets_req).
-define(LEGACY_LIST_BUCKETS_REQ, legacy_list_buckets_req).
-define(LIST_KEYS_REQ, list_keys_req).
-define(STREAM_LIST_KEYS_REQ, stream_list_keys_req).
-define(GET_BUCKET_REQ, get_bucket_req).
-define(GET_BUCKET_TYPE_REQ, get_bucket_type_req).
-define(SET_BUCKET_REQ, set_bucket_req).
-define(SET_BUCKET_TYPE_REQ, set_bucket_type_req).
-define(RESET_BUCKET_REQ, reset_bucket_req).
-define(MAPRED_REQ, mapred_req).
-define(MAPRED_STREAM_REQ, mapred_stream_req).
-define(MAPRED_BUCKET_REQ, mapred_bucket_req).
-define(MAPRED_BUCKET_STREAM_REQ, mapred_bucket_stream_req).
-define(SEARCH_REQ, search_req).
-define(GET_INDEX_EQ_REQ, get_index_eq_req).
-define(GET_INDEX_RANGE_REQ, get_index_range_req).
-define(CS_BUCKET_FOLD_REQ, cs_bucket_fold_req).
-define(TUNNEL_REQ, tunnel_req).
-define(GET_PREFLIST_REQ, get_preflist_req).
-define(GET_COVERAGE_REQ, get_coverage_req).
-define(REPLACE_COVERAGE_REQ, replace_coverage_req).
-define(GET_RING_REQ, get_ring_req).
-define(GET_DEFAULT_BUCKET_PROPS_REQ, get_default_bucket_props_req).
-define(GET_NODES_REQ, get_nodes_req).
-define(LIST_SEARCH_INDEXES_REQ, list_search_indexes_req).
-define(CREATE_SEARCH_INDEX_REQ, create_search_index_req).
-define(GET_SEARCH_INDEX_REQ, get_search_index_req).
-define(DELETE_SEARCH_INDEX_REQ, delete_search_index_req).
-define(SET_SEARCH_INDEX_REQ, set_search_index_req).
-define(GET_SEARCH_SCHEMA_REQ, get_search_schema_req).
-define(CREATE_SEARCH_SCHEMA_REQ, create_search_schema_req).
-define(COUNTER_INCR_REQ, counter_incr_req).
-define(COUNTER_VAL_REQ, counter_val_req).
-define(FETCH_TYPE_REQ, fetch_type_req).
-define(UPDATE_TYPE_REQ, update_type).
-define(MODIFY_TYPE_REQ, modify_type).

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
-define(MAPRED_STREAM_TIMEOUT, mapred_stream_timeout).
-define(MAPRED_STREAM_CALL_TIMEOUT, mapred_stream_call_timeout).
-define(MAPRED_BUCKET_TIMEOUT, mapred_bucket_timeout).
-define(MAPRED_BUCKET_CALL_TIMEOUT, mapred_bucket_call_timeout).
-define(MAPRED_BUCKET_STREAM_CALL_TIMEOUT, mapred_bucket_stream_call_timeout).
-define(SEARCH_TIMEOUT, search_timeout).
-define(SEARCH_CALL_TIMEOUT, search_call_timeout).
-define(GET_PREFLIST_TIMEOUT, get_preflist_timeout).
-define(GET_RING_TIMEOUT, get_ring_timeout).
-define(GET_DEFAULT_BUCKET_PROPS_TIMEOUT, get_default_bucket_props_timeout).
-define(GET_NODES_TIMEOUT, get_nodes_timeout).

%% Error Reasons
-define(INVALID_CONFIG, invalid_config).
-define(NO_AVAILABLE_CONNECTIONS, no_available_connections).