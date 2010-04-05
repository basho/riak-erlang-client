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
         get_update_metadata/1,
         get_update_value/1]).

%% @type riakc_obj().  Opaque container for Riak objects.
-record(riakc_obj, {
          bucket :: binary(),
          key :: binary(),
          vclock :: binary(),
          contents :: [],
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

%% ====================================================================
%% object functions
%% ====================================================================

%% @spec new(Bucket::bucket(), Key::key()) -> riakc_obj()
%% @doc Constructor for new riak client objects.
new(Bucket, Key) ->
    #riakc_obj{bucket = Bucket, key = Key}.

%% @doc  INTERNAL USE ONLY.  Set the contents of riak_object to the
%%       {Metadata, Value} pairs in MVs. Normal clients should use the
%%       set_update_[value|metadata]() + apply_updates() method for changing
%%       object contents.
%% @private
new(Bucket, Key, Vclock, Contents) ->
    #riakc_obj{bucket = Bucket, key = Key, vclock = Vclock, contents = Contents}.

%% @spec bucket(riakc_obj()) -> bucket()
%% @doc Return the containing bucket for this riakc_obj.
bucket(O) ->
    O#riakc_obj.bucket.

%% @spec key(riakc_obj()) -> key()
%% @doc  Return the key for this riakc_obj.
key(O) ->
    O#riakc_obj.key.

%% @spec vclock(riakc_obj()) -> vclock:vclock()
%% @doc  Return the vector clock for this riakc_obj.
vclock(O) ->
    O#riakc_obj.vclock.
    
%% @spec value_count(riakc_obj()) -> non_neg_integer()
%% @doc  Return the number of values (siblings) of this riakc_obj.
value_count(#riakc_obj{contents=Contents}) -> length(Contents).

%% @spec get_contents(riakc_obj()) -> [{dict(), value()}]
%% @doc  Return the contents (a list of {metadata, value} tuples) for
%%       this riakc_obj.
get_contents(O) ->    
    O#riakc_obj.contents.

%% @spec get_metadata(riak_object()) -> dict()
%% @doc  Assert that this riak_object has no siblings and return its associated
%%       metadata.  This function will fail with a badmatch error if the
%%       object has siblings (value_count() > 1).
get_metadata(O=#riakc_obj{}) ->
    % this blows up intentionally (badmatch) if more than one content value!
    [{Metadata,_V}] = get_contents(O),
    Metadata.

%% @spec get_metadatas(riak_object()) -> [dict()]
%% @doc  Return a list of the metadata values for this riak_object.
get_metadatas(#riakc_obj{contents=Contents}) ->
    [M || {M,_V} <- Contents].

%% @spec get_value(riakc_obj()) -> value()
%% @doc  Assert that this riakc_obj has no siblings and return its associated
%%       value.  This function will fail with a badmatch error if the object
%%       has siblings (value_count() > 1).
get_value(#riakc_obj{contents=Contents}) ->
    [{_M,V}] = Contents,
    V.

%% @spec get_values(riakc_obj()) -> [value()]
%% @doc  Return a list of object values for this riakc_obj.
get_values(#riakc_obj{contents=Contents}) ->
    [V || {_,V} <- Contents].

%% @spec update_metadata(riakc_obj(), dict()) -> riakc_obj()
%% @doc  Set the updated metadata of an object to M.
update_metadata(Object=#riakc_obj{}, M) ->
    Object#riakc_obj{updatemetadata=dict:erase(clean, M)}.

%% @spec update_value(riakc_obj(), value()) -> riakc_obj()
%% @doc  Set the updated value of an object to V
update_value(Object=#riakc_obj{}, V) -> Object#riakc_obj{updatevalue=V}.

%% @spec get_update_metadata(riakc_obj()) -> dict()
%% @doc  Return the updated metadata of this riakc_obj.
get_update_metadata(#riakc_obj{updatemetadata=UM}) -> UM.

%% @spec get_update_value(riakc_obj()) -> value()
%% @doc  Return the updated value of this riakc_obj.
get_update_value(#riakc_obj{updatevalue=UV}) -> UV.
