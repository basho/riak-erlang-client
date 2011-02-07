{application, riakc,
 [{description, "Riak Client"},
  {vsn, "1.1.0"},
  {modules, [
             riakc_pb,
             riakc_pb_socket,
             riakc_obj,
             riakclient_pb
            ]},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {registered, []},
  {env, [
         %% Set default timeout for operations.
         %% Individual operation timeouts can be supplied,
         %% e.g. get_timeout, put_timeout that will 
         %% override the default.
         {timeout, 60000}
        ]}
 ]}.

