# Schedoscope

## Introduction

Schedoscope is a scheduling framework for painfree and agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

Based on a slick Scala DSL, 

* defining a partitioned Hive table (called "view") is as simple as:

* defining its dependencies is as simple as:

* specifying its computation logic is as simple as:

* testing it is as simple as:

Running the Schedoscope shell, 

* loading the view is as simple as:

* reloading the view in case its dependencies, structure, or logic has changed is as simple as:

* monitoring what's going on is as simple as:

## Tutorials

Please follow the Open Street Map tutorial to install, compile, and run Schedoscope in a standard Hadoop distribution image within minutes:

- [Open Street Map Tutorial](Open Street Map Tutorial)

## Implementing Views
- [Setting up a Schedoscope Project](Setting up a Schedoscope Project)
- [Schedoscope View DSL](Schedoscope View DSL)
- [Storage formats](Storage Formats)
- Transformations
 - [NoOp](NoOp Transformations)
 - [File System](File System Transformations)
 - [Hive](Hive Transformations)
 - [Pig](Pig-Transformations)
 - [MapReduce](MapReduce Transformations)
 - [Morphline](Morphline Transformations)
- [Test Framework](Test Framework)

## Operating Schedoscope
- [Bundling and Deploying](Bundling and Deploying)
- [Starting](Starting Schedoscope)
- [Scheduling](Scheduling)
- [REST API](Schedoscope REST API)
- [Command Reference](Command Reference)
- [View Pattern Reference](View Pattern Reference)

## Extending Schedoscope
- [Architecture](Architecture)

## Core Team
* [Utz Westermann](https://github.com/utzwestermann) (Otto Group): Maintainer, DSL concept and implementation 
* [Hans-Peter Zorn](https://github.com/hpzorn) (Inovex GmbH): Scheduling architecture and implementation, Morphline support
* [Dominik Benz](https://github.com/dominikbenz) (Inovex GmbH): Test framework, deployment system, transformations, command shell

Please help making Schedoscope a better place!

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
