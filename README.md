# Schedoscope

## Introduction

Schedoscope is a scheduling framework for painfree agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

With Schedoscope,
* you never have to create schema DDL and migration scripts;
* you never have to manually determine which data must be deleted and recomputed in face of retroactive changes to logic or data structures;
* you specify Hive table structures (called "views"), partitioning schemes, storage formats, dependent views, as well as transformation logic in one file in a concise Scala DSL;
* you have a wide range of options for expressing data transformations - from file operations and MapReduce jobs to Pig scripts, Hive queries, and Oozie workflows;
* you benefit from your Scala's static type system and IDE's code completion to make less typos that hit you late during deployment or runtime;
* you can easily write tests for your transformation logic and run them quickly;
* you schedule jobs by expressing the views you need - Schedoscope takes care that all dependencies are computed as well;
* you achieve a higher utilization of your YARN cluster's resources because job launchers are not YARN applications themselves that consume YARN capactity.
 
## Getting Started

Please follow the Open Street Map tutorial to install, compile, and run Schedoscope in a standard Hadoop distribution image within minutes:

- [Open Street Map Tutorial](https://github.com/ottogroup/schedoscope/wiki/Open%20Street%20Map%20Tutorial)

More documentation can be found here:
* [Schedoscope Wiki](https://github.com/ottogroup/schedoscope/wiki)

## When could Schedoscope be not for you?

Schedoscope is based on the following assumptions:
* data are largely relational and meaningfully representable as Hive tables;
* there is enough cluster time and capacity to actually allow for retroactive recomputation of data;
* it is acceptable to compile table structure, dependencies, and transformation logic into what is effectively a project-specific scheduler.

Should any of those assumptions not hold in your context, you should probably look for a different scheduler.

## Core Team
* [Utz Westermann](https://github.com/utzwestermann) (Otto Group): Maintainer, DSL concept and implementation 
* [Hans-Peter Zorn](https://github.com/hpzorn) (Inovex GmbH): Scheduling architecture and implementation, Morphline support
* [Dominik Benz](https://github.com/dominikbenz) (Inovex GmbH): Test framework, deployment system, transformations, command shell

Please help making Schedoscope better!

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
