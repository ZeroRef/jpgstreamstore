# jPgStreamStore

[![Build Status](https://travis-ci.org/ZeroRef/jpgstreamstore.svg)](https://travis-ci.org/ZeroRef/jpgstreamstore)
[![License](http://img.shields.io/:license-apache-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

*fancy, sophisticated persistence handling*

jpgstreamstore is a small library targeted at building scalable event-sourced applications on top of PostgreSQL. It has simple API, heavily inspired by Greg Young's Event Store.

##Features

+ Fully ACID compliant
+ Optimistic concurrency support
+ Custom stream and event properties
+ Small concise API, no (callback hell, rose futures, broken promisses)
+ Global event stream for HA replication
+ Duplicate event detection (based on identity)[todo]
+ Friendly for multi-tenant designs[todo]
+ Zero transitive dependencies

## Usage

##### Essentials
+ Writing to stream [[see](Source/Example/Scenarios/S04_Write_to_stream.cs)]
+ Reading from stream [[see](Source/Example/Scenarios/S05_Read_from_stream.cs)]
+ Optimistic concurrency [[see](Source/Example/Scenarios/S08_Concurrency_conflicts.cs)]
+ Replication [[see](Source/Example/Scenarios/S09_Handling_duplicates.cs)]

## Contribute

This is a volunteer effort. If you use it and you like it, let us know whos-using, and also help by spreading the word!

## License

Copyright 2017. Released under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0.html).