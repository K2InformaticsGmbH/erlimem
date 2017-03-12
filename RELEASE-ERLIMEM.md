Application erlimem
===================

Release history with new or improved features and bugfixes

Version 1.3.0 (Release Date dd.mm.yyyy)
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
