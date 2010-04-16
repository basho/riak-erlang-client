%% -------------------------------------------------------------------
%%
%% riakc_obj: container for Riak data and metadata
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc container for Riak data and metadata required to use protocol buffer interface


-module(riakc_obj).
-export([new/2, new/4,
         bucket/1,
         key/1,
         vclock/1,
         value_count/1,
         get_contents/1,
         get_metadata/1,
         get_metadatas/1,
         get_value/1,
         get_values/1,
         update_metadata/2,
         update_value/2,
         update_value/3,
         update_content_type/2,
         get_update_metadata/1,
         get_update_content_type/1,
         get_update_value/1]).
-include("riakc_obj.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type bucket() :: binary().
-type key() :: binary().
-type vclock() :: binary().
-type metadata() :: dict().
-type content_type() :: binary().
-type value() :: term().
-type contents() :: [{metadata(), value()}].

%% @type riakc_obj().  Opaque container for Riak objects.
-record(riakc_obj, {
          bucket :: bucket(),
          key :: key(),
          vclock :: vclock(),
          contents :: contents(),
          updatemetadata=dict:new() :: dict(),
          updatevalue :: value()
         }).

%% ====================================================================
%% object functions
%% ====================================================================

%% @doc Constructor for new riak client objects.
-spec new(bucket(), key()) -> #riakc_obj{}.
new(Bucket, Key) ->
    #riakc_obj{bucket = Bucket, key = Key}.

%% @doc  INTERNAL USE ONLY.  Set the contents of riak_object to the
%%       {Metadata, Value} pairs in MVs. Normal clients should use the
%%       set_update_[value|metadata]() + apply_updates() method for changing
%%       object contents.
%% @private
-spec new(bucket(), key(), vclock(), contents()) -> #riakc_obj{}.
new(Bucket, Key, Vclock, Contents) ->
    #riakc_obj{bucket = Bucket, key = Key, vclock = Vclock, contents = Contents}.

%% @doc Return the containing bucket for this riakc_obj.
-spec bucket(#riakc_obj{}) -> bucket().
bucket(O) ->
    O#riakc_obj.bucket.

%% @spec key(riakc_obj()) -> key()
%% @doc  Return the key for this riakc_obj.
-spec key(#riakc_obj{}) -> key().
key(O) ->
    O#riakc_obj.key.

%% @doc  Return the vector clock for this riakc_obj.
-spec vclock(#riakc_obj{}) -> vclock().
vclock(O) ->
    O#riakc_obj.vclock.
    
%% @doc  Return the number of values (siblings) of this riakc_obj.
-spec value_count(#riakc_obj{}) -> non_neg_integer().
value_count(#riakc_obj{contents=Contents}) -> length(Contents).

%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riakc_obj.
-spec get_contents(#riakc_obj{}) -> contents().
get_contents(O) ->    
    O#riakc_obj.contents.

%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will fail with a badmatch error if the
%%       object has siblings (value_count() > 1).
-spec get_metadata(#riakc_obj{}) -> metadata().
get_metadata(O=#riakc_obj{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{Metadata,_V}] = get_contents(O),
    Metadata.

%% @doc  Return a list of the metadata values for this riak_object.
-spec get_metadatas(#riakc_obj{}) -> [metadata()].
get_metadatas(#riakc_obj{contents=Contents}) ->
    [M || {M,_V} <- Contents].

%% @doc  Assert that this riakc_obj has no siblings and return its associated
%%       value.  This function will fail with a badmatch error if the object
%%       has siblings (value_count() > 1).
-spec get_value(#riakc_obj{}) -> value().
get_value(#riakc_obj{contents=Contents}) ->
    [{_M,V}] = Contents,
    V.

%% @doc  Return a list of object values for this riakc_obj.
-spec get_values(#riakc_obj{}) -> [value()].
get_values(#riakc_obj{contents=Contents}) ->
    [V || {_,V} <- Contents].

%% @doc  Set the updated metadata of an object to M.
-spec update_metadata(#riakc_obj{}, metadata()) -> #riakc_obj{}.
update_metadata(Object=#riakc_obj{}, M) ->
    Object#riakc_obj{updatemetadata=M}.

%% @doc  Set the updated content-type of an object to CT.
-spec update_content_type(#riakc_obj{}, metadata()) -> #riakc_obj{}.
update_content_type(Object=#riakc_obj{}, CT) ->
    M1 = get_update_metadata(Object),
    Object#riakc_obj{updatemetadata=dict:store(?MD_CTYPE, riakc_pb:to_binary(CT), M1)}.

%% @doc  Set the updated value of an object to V
-spec update_value(#riakc_obj{}, value()) -> #riakc_obj{}.
update_value(Object=#riakc_obj{}, V) -> Object#riakc_obj{updatevalue=V}.

%% @doc  Set the updated value of an object to V
-spec update_value(#riakc_obj{}, value(), content_type()) -> #riakc_obj{}.
update_value(Object=#riakc_obj{}, V, CT) -> 
    O1 = update_content_type(Object, CT),
    O1#riakc_obj{updatevalue=V}.

%% @doc  Return the updated metadata of this riakc_obj.
-spec get_update_metadata(#riakc_obj{}) -> metadata().
get_update_metadata(#riakc_obj{updatemetadata=UM}) -> 
    UM.
           
%% @doc Return the content type of the update value
get_update_content_type(Object=#riakc_obj{}) ->
    UM = get_update_metadata(Object),
    case dict:find(?MD_CTYPE, UM) of
        error ->
            undefined;
        {ok, Ctype} ->
            Ctype
    end.

%% @doc  Return the updated value of this riakc_obj.
-spec get_update_value(#riakc_obj{}) -> value().
get_update_value(#riakc_obj{updatevalue=UV}) -> UV.

%% ===================================================================
%% Unit Tests
%% ===================================================================
-ifdef(TEST).

bucket_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(<<"b">>, bucket(O)).

key_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(<<"k">>, key(O)).

vclock_test() ->
    %% For internal use only
    O = riakc_obj:new(<<"b">>, <<"k">>, <<"vclock">>, []),
    ?assertEqual(<<"vclock">>, vclock(O)).

contents0_test() ->   
    O = riakc_obj:new(<<"b">>, <<"k">>, <<"vclock">>, []),
    ?assertEqual(0, value_count(O)),
    ?assertEqual([], get_metadatas(O)),
    ?assertEqual([], get_values(O)),
    ?assertEqual([], get_contents(O)),
    ?assertError({badmatch, []}, get_metadata(O)),
    ?assertError({badmatch, []}, get_value(O)).

contents1_test() ->   
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O = riakc_obj:new(<<"b">>, <<"k">>, <<"vclock">>,
                      [{M1, <<"val1">>}]),
    ?assertEqual(1, value_count(O)),
    ?assertEqual([M1], get_metadatas(O)),
    ?assertEqual([<<"val1">>], get_values(O)),
    ?assertEqual([{M1,<<"val1">>}], get_contents(O)),
    ?assertEqual(M1, get_metadata(O)),
    ?assertEqual(<<"val1">>, get_value(O)).

contents2_test() ->   
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    M2 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O = riakc_obj:new(<<"b">>, <<"k">>, <<"vclock">>,
                      [{M1, <<"val1">>},
                       {M2, <<"val2">>}]),
    ?assertEqual(2, value_count(O)),
    ?assertEqual([M1, M2], get_metadatas(O)),
    ?assertEqual([<<"val1">>, <<"val2">>], get_values(O)),
    ?assertEqual([{M1,<<"val1">>},{M2,<<"val2">>}], get_contents(O)),
    ?assertError({badmatch, [{M1,<<"val1">>},{M2,<<"val2">>}]}, get_metadata(O)),
    ?assertError({badmatch, [{M1,<<"val1">>},{M2,<<"val2">>}]}, get_value(O)).

update_metadata_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    UM = riakc_obj:get_update_metadata(O),
    ?assertEqual([], dict:to_list(UM)).

update_value_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(undefined, get_update_value(O)),
    O1 = riakc_obj:update_value(O, <<"v">>),
    ?assertEqual(<<"v">>, get_update_value(O1)),
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O2 = riakc_obj:update_metadata(O1, M1),
    ?assertEqual(M1, get_update_metadata(O2)).
  
updatevalue_ct_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    ?assertEqual(undefined, get_update_value(O)),
    O1 = riakc_obj:update_value(O, <<"v">>, <<"x-application/custom">>),
    ?assertEqual(<<"v">>, get_update_value(O1)),
    M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
    O2 = riakc_obj:update_metadata(O1, M1),
    ?assertEqual(M1, get_update_metadata(O2)),
    ?assertEqual(<<"x-application/custom">>, get_update_content_type(O1)).
  
update_content_type_test() ->
    O = riakc_obj:new(<<"b">>, <<"k">>),
    undefined = get_update_content_type(O),
    O1 = update_content_type(O, <<"application/json">>),
    ?assertEqual(<<"application/json">>, get_update_content_type(O1)).

-endif.
