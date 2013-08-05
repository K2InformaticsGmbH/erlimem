erlimem
=======

Erlang driver for IMEM DB

Commandline tests
=================
```erlang
{ok,S} = erlimem:open(tcp, {"127.0.0.1", 8124, imem}, {<<"admin">>,<<"change_on_install">>}). 
S:run_cmd(admin_exec, [imem_datatype,module_info,[attributes]]).

proplists:get_value(vsn,imem_datatype:module_info(attributes)).
proplists:get_value(vsn, S:run_cmd(admin_exec, [imem_datatype,module_info,[attributes]])).
S:run_cmd(admin_exec, [code,get_object_code,[imem_datatype]]).
```
