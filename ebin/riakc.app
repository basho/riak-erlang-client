{application, riakc,
 [{description, "Riak Client"},
  {vsn, "0.2.0"},
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
  {env, [{{timeout, get}, 30000},
         {{timeout, put}, 30000},
         {timeout,        60000}]}
 ]}.

