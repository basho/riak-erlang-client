%% -------------------------------------------------------------------
%%
%% riakc_datatype: Behaviour for eventually-consistent data-types
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc When used with riakc_pb_socket:fetch_type() and
%% riakc_pb_socket:update_type(), modules implementing this behaviour
%% provide an internally consistent local view of the data while
%% capturing update operations for shipping back to the server.
-module(riakc_datatype).

-type maybe(T) :: T | undefined.
-type datatype() :: term().
-type context() :: binary().

%% @doc Constructs a new, empty container for the type.
-callback new() -> datatype().

%% @doc Constructs a new container for the type with the specified
%% value.
-callback new(Value::term()) -> datatype().

%% @doc Constructs a new container for the type with the specified
%% value and opaque server-side context.
-callback new(Value::term(), context()) -> datatype().

%% @doc Returns the current local-view of the container's value.
-callback value(datatype()) -> term().

%% @doc Extracts an operation from the container that can be encoded
%% into an update request. 'undefined' should be returned if the type
%% is unmodified.
-callback to_op(datatype()) -> maybe(tuple()).

%% @doc Extracts the opaque update context from the container for
%% sending along with an update request. 'undefined' should be
%% returned if no context was provided, or if it is unneeded.
-callback context(datatype()) -> maybe(context()).
