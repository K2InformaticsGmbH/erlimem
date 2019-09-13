Application erlimem
===================

Release history with new or improved features and bugfixes

Version 3.0.0 (Release Date 13.09.2019)
=======================================
* Support for cluster queires
* Backward incompatible

Version 2.0.0 (Release Date 24.08.2018)
=======================================
* [4-byte TCP payload header is replaced with socket `{packet, 4}` option](https://github.com/K2InformaticsGmbH/erlimem/pull/27)
* test cases cleaup and update
* unused macros and modules removed, code cleanups

Version 1.3.1 (Release Date 16.10.2017)
=======================================
* Fixed Erlang 20 warnings
* Removed rpc interfaces

Version 1.3.0 (Release Date 20.03.2017)
=======================================
* Migration to rebar3.

Version 1.2.8 (Release Date 09.03.2017)
=======================================
* Added support for async run_cmd allowing remote imem metric requests
* Added configuration to run eunit tests

Version 1.2.7 (Release Date 18.01.2017)
=======================================
* added 3 tuple for rest service

Version 1.2.6 (Release Date 16.09.2016)
=======================================
* TCP based connections timeout set to 5 seconds, non TCP connections 100 seconds
* Handled deleted rows event in tail mode

Version 1.0.3 (Release Date 08.08.2013)
=======================================
* increased the timeout of synchronous calls from 1 sec to 10 sec

Version 1.0.2 (Release Date 05.07.2013)
=======================================
* using lager as logging tool

Version 1.0.1 (Release Date 03.07.2013)
=======================================
* error messages of statements are not trapped by the driver
