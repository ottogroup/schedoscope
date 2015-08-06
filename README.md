## News

###### 8/7/2015 - Release 0.1.1

We have released Version 0.1.1 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

This is a minor release that comprises some code cleanup and performance optimizations with regard to view initialization and Morphline transformations. We also ímplemented a [shell transformation](https://github.com/ottogroup/schedoscope/wiki/Shell-Transformations) (more documentation to come). 

# Schedoscope

## Introduction

Schedoscope is a scheduling framework for painfree agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

With Schedoscope,
* you never have to create DDL and schema migration scripts;
* you do not have to manually determine which data must be deleted and recomputed in face of retroactive changes to logic or data structures;
* you specify Hive table structures (called "views"), partitioning schemes, storage formats, dependent views, as well as transformation logic in a concise Scala DSL;
* you have a wide range of options for expressing data transformations - from file operations and MapReduce jobs to Pig scripts, Hive queries, and Oozie workflows;
* you benefit from Scala's static type system and your IDE's code completion to make less typos that hit you late during deployment or runtime;
* you can easily write unit tests for your transformation logic and run them quickly right out of your IDE;
* you schedule jobs by expressing the views you need - Schedoscope takes care that all required dependencies - and only those-  are computed as well;
* you achieve a higher utilization of your YARN cluster's resources because job launchers are not YARN applications themselves that consume cluster capacitity.

## Getting Started

Get a glance at 
- [Schedoscope's features](https://github.com/ottogroup/schedoscope/wiki/Schedoscope-at-a-Glance)

Follow the Open Street Map tutorial to install, compile, and run Schedoscope in a standard Hadoop distribution image within minutes:

- [Open Street Map Tutorial](https://github.com/ottogroup/schedoscope/wiki/Open%20Street%20Map%20Tutorial)

More documentation can be found here:
* [Schedoscope Wiki](https://github.com/ottogroup/schedoscope/wiki)

## When is Schedoscope not for you?

Schedoscope is based on the following assumptions:
* data are largely relational and meaningfully representable as Hive tables;
* there is enough cluster time and capacity to actually allow for retroactive recomputation of data;
* it is acceptable to compile table structures, dependencies, and transformation logic into what is effectively a project-specific scheduler.

Should any of those assumptions not hold in your context, you should probably look for a different scheduler.

## Origins

Schedoscope was conceived at the Business Intelligence department of [Otto Group](http://ottogroup.com/en/die-otto-group.php)

## Core Team
* [Utz Westermann](https://github.com/utzwestermann): Maintainer, DSL concept and implementation 
* [Hans-Peter Zorn](https://github.com/hpzorn): Scheduling architecture and implementation, Morphline support
* [Dominik Benz](https://github.com/dominikbenz): Test framework, deployment system, transformations, command shell
* [Annika Leveringhaus](https://github.com/aleveringhaus): Tutorial

Please help making Schedoscope better!

## Community / Forums

- Google Groups: [Schedoscope Users](https://groups.google.com/forum/#!forum/schedoscope-users)
- Twitter: @Schedoscope

## Build Status

[![Build Status](https://travis-ci.org/ottogroup/schedoscope.svg?branch=master)](https://travis-ci.org/ottogroup/schedoscope)

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
