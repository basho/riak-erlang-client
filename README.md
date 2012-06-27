<p align="right">
    <img src="http://wiki.basho.com/images/riaklogo.png">
</p>
# Riak Protocol Buffers Client Introduction

[![Build Status](https://secure.travis-ci.org/basho/riak-erlang-client.png?branch=master)](http://travis-ci.org/basho/riak-erlang-client)

This document assumes that you have already started your Riak cluster.
For instructions on that prerequisite, refer to
[Installation and Setup](https://wiki.basho.com/Installation-and-Setup.html)
in the [Riak Wiki](https://wiki.basho.com). You can also view the Riak Erlang Client EDocs [here](http://basho.github.com/riak-erlang-client/).

Dependencies
=========

To build the riak-erlang-client you will need Erlang OTP R13B04 or later, and Git.

Debian
------

On a Debian based system (Debian, Ubuntu, ...) you will need to make sure that certain packages are installed:

    # apt-get install erlang-parsetools erlang-dev erlang-syntax-tools


Installing
=========

        $ git clone git://github.com/basho/riak-erlang-client.git
        $ cd riak-erlang-client
        $ make


Connecting
=======

To talk to riak, all you need is an Erlang node with the riak-erlang-client library (riakc) in its code path.

        $ erl -pa $PATH_TO_RIAKC/ebin $PATH_TO_RIAKC/deps/*/ebin


You'll know you've done this correctly if you can execute the following commands and get a path to a beam file, instead of the atom 'non_existing':

       1> code:which(riakc_pb_socket).
       ".../riak-erlang-client/ebin/riakc_pb_socket.beam"


Once you have your node running, pass your Riak server nodename to `riakc_pb_socket:start_link/2` to connect and get a client. This can be as simple as:

       1> {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087).
       {ok,<0.56.0>}

Verify connectivity with the server using `ping/1`.

       2> riakc_pb_socket:ping(Pid).
       pong


Storing New Data
=========

Each bit of data in Riak is stored in a "bucket" at a "key" that is unique to that bucket. The bucket is intended as an organizational aid, for example to help segregate data by type, but Riak doesn't care what values it stores, so choose whatever scheme suits you. Buckets, keys and values are all binaries.

Before storing your data, you must wrap it in a riakc_obj:

    3> Object = riakc_obj:new(<<"groceries">>, <<"mine">>, <<"eggs & bacon">>).
    {riakc_obj,<<"groceries">>,<<"mine">>,undefined,undefined,
    {dict,0,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],[],[],[],...}}},
    <<"eggs & bacon">>}

If you want to have the server generate you a key (similar to the REST API) pass the atom `undefined` as the second parameter to new().

The Object refers to a key `<<"mine">>` in a bucket named `<<"groceries">>` with the value `<<"eggs & bacon">>`. Using the client you opened earlier, store the object:

    5> riakc_pb_socket:put(Pid, Object).
    ok

If the return value of the last command was anything but the atom `ok` (or `{ok, Key}` when you instruct the server to generate the key), then the store failed. The return value may give you a clue as to why the store failed, but check the Troubleshooting section below if not.

The object is now stored in Riak. `put/2` uses default parameters for storing the object. There is also a `put/3` call that takes a proplist of options.

<table border="1">
    <th>Option</th>
    <th>Description</th>
    <tr>
        <td><code>{w, W}</code></td>
        <td>the minimum number of nodes that must respond with success for the write to be considered successful. The default is currently set on the server at 2</td>
    </tr>
    <tr>
        <td><code>{dw, DW}</code></td>
        <td>  the minimum number of nodes that must respond with success * *after durably storing* the object for the write to be considered successful. The default is currently set on the server at 0. </td>
    </tr>
    <tr>
        <td><code>return_body </code></td>
        <td> immediately do a get after the put and return a
        riakc_obj.</td>
    </tr>
</table>

    6> AnotherObject = riakc_obj:new(<<"my bucket">>, <<"my key">>, <<"my binary data">>).
    7> riakc_pb_socket:put(Pid, AnotherObject, [{w, 2}, {dw, 1}, return_body]).
    {ok,{riakc_obj,<<"my bucket">>,<<"my key">>,
    <<107,206,97,96,96,96,206,96,202,5,82,44,140,62,169,115,
    50,152,18,25,243,88,25,...>>,
    [{{dict,2,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],...}}},
    <<"my binary data">>}],
    {dict,0,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],[],...}}},
    undefined}}

Would make sure at least two nodes responded successfully to the put and at least one node has durably stored the value and an updated object is returned.

See [riak/doc/architecture.txt](https://github.com/basho/riak/blob/master/doc/architecture.txt) for more information about W and DW
values.


Fetching Data
==================

At some point you'll want that data back. Using the same bucket and key you used before:

    8> {ok, O} = riakc_pb_socket:get(Pid, <<"groceries">>, <<"mine">>).
    {ok,{riakc_obj,<<"groceries">>,<<"mine">>,
    <<107,206,97,96,96,96,204,96,202,5,82,44,12,143,167,115,
    103,48,37,50,230,177,50,...>>,
    [{{dict,2,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],...}}},
    <<"eggs & bacon">>}],
    {dict,0,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],[],...}}},
    undefined}}

Like `put/3`, there is a `get/4` function that takes options.

<table border="1">
    <th>Option</th>
    <th>Description</th>
    <tr>
        <td><code>{r, R}</code></td>
        <td>the minimum number of nodes that must respond with success for the read to be considered successfu2</td>
    </tr>
</table>

If the data was originally stored using the distributed erlang client (riak_client), the server
will automatically term_to_binary/1 the value before sending it, with the content
type set to application/x-erlang-binary (replacing any user-set value).  The application is
responsible for calling binary_to_term to access the content and calling term_to_binary
when modifying it.

Modifying Data
==================

Say you had the "grocery list" from the examples above, reminding you to get `<<"eggs & bacon">>`, and you want to add `<<"milk">>` to it. The easiest way is:

    9> {ok, Oa} = riakc_pb_socket:get(Pid, <<"groceries">>, <<"mine">>).
    ...
    10> Ob = riakc_obj:update_value(Oa, <<"milk, ", (riakc_obj:get_value(Oa))/binary>>).
    11> {ok, Oc} = riakc_pb_socket:put(Pid, Ob, [return_body]).
    {ok,{riakc_obj,<<"groceries">>,<<"mine">>,
    <<107,206,97,96,96,96,206,96,202,5,82,44,12,143,167,115,
    103,48,37,50,230,177,50,...>>,
    [{{dict,2,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],...}}},
    <<"milk, eggs & bacon">>}],
    {dict,0,16,16,8,80,48,
    {[],[],[],[],[],[],[],[],[],[],[],[],[],...},
    {{[],[],[],[],[],[],[],[],[],[],[],...}}},
    undefined}}


That is, fetch the object from Riak, modify its value with `riakc_obj:update_value/2`, then store the modified object back in Riak. You can get your updated object to convince yourself that your list is updated:

Deleting Data
==================

Throwing away data is quick and simple: just use the `delete/3` function.

    10> riakc_pb_socket:delete(Pid, <<"groceries">>, <<"mine">>).
    ok

As with get and put, delete can also take options

<table border="1">
    <th>Option</th>
    <th>Description</th>
    <tr>
        <td><code>{rw, RW}</code></td>
        <td>the number of nodes to wait for responses from</td>
    </tr>
</table>

Issuing a delete for an object that does not exist returns just returns ok.

Encoding
==================

The initial release of the erlang protocol buffers client treats all values as binaries. The caller needs to make sure data is serialized and deserialized correctly. The content type stored along with the object may be used to store the encoding. For example

    decode_term(Object) ->
      case riakc_obj:get_content_type(Object) of
        <<"application/x-erlang-term">> ->
          try
            {ok, binary_to_term(riakc_obj:get_value(Object))}
          catch
            _:Reason ->
              {error, Reason}
          end;
        Ctype ->
          {error, {unknown_ctype, Ctype}}
      end.

    encode_term(Object, Term) ->
      riakc_obj:update_value(Object, term_to_binary(Term, [compressed]),
      <<"application/x-erlang-term">>).


Siblings
==================

If a bucket is configured to allow conflicts (allow_mult=true) then the result object may contain more than one result. The number of values can be returned with

    1> riakc_obj:value_count(Obj).
    2

The values can be listed with

    2> riakc_obj:get_values(Obj).
    \[<<"{\"k1\":\"v1\"}">>,<<"{\"k1\":\"v1\"}">>\]

And the content types as

    3> riakc_obj:get_content_types(Obj).
    []

Siblings are resolved by calling `riakc_obj:update_value` with the winning value on an object returned by get or put with return_body.

Listing Keys
=============

Most uses of key-value stores are structured in such a way that requests know which keys they want in a bucket. Sometimes, though, it's necessary to find out what keys are available (when debugging, for example). For that, there is list_keys:

    1> riakc_pb_socket:list_keys(Pid, <<"groceries">>).
    {ok,[<<"mine">>]}

Note that keylist updates are asynchronous to the object storage primitives, and may not be updated immediately after a put or delete. This function is primarily intended as a debugging aid.

`list_keys/2` is just a convenience function around the streaming version of the call `stream_list_keys(Pid, Bucket)`.

    2> riakc_pb_socket:stream_list_keys(Pid, <<"groceries">>).
    {ok,87009603}
    3> receive Msg1 \-> Msg1 end.
    {87009603,{keys,[]}}
    4> receive Msg2 \-> Msg2 end.
    {87009603,done}

See [`riakc_pb_socket:wait_for_listkeys`](https://github.com/basho/riak-erlang-client/blob/master/src/riakc_pb_socket.erl#L1087) for an example of receiving.

Bucket Properties
==================

Bucket properties can be retrieved and modified using `get_bucket/2` and `set_bucket/3`. The bucket properties are represented as a proplist. Only a subset of the properties can be retrieved and set using the protocol buffers interface - currently only n_val and allow_mult.

Here's an example of getting/setting properties

    3> riakc_pb_socket:get_bucket(Pid, <<"groceries">>).
    {ok,[{n_val,3},{allow_mult,false}]}
    4> riakc_pb_socket:set_bucket(Pid, <<"groceries">>, [{n_val, 5}]).
    ok
    5> riakc_pb_socket:get_bucket(Pid, <<"groceries">>).
    {ok,[{n_val,5},{allow_mult,false}]}
    6> riakc_pb_socket:set_bucket(Pid, <<"groceries">>, [{n_val, 7}, {allow_mult, true}]).
    ok
    7> riakc_pb_socket:get_bucket(Pid, <<"groceries">>).
    {ok,[{n_val,7},{allow_mult,true}]}

Troubleshooting
==================

If `start/2` or `start_link/2` return `{error,econnrefused}` the client could not connect to the server - make sure the protocol buffers interface is enabled on the server and the address/port is correct.
