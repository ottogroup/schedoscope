# ![Schedoscope](https://raw.githubusercontent.com/wiki/ottogroup/schedoscope/images/schedoscope_logo.jpg)

## Introduction

Schedoscope is a scheduling framework for painfree agile development, testing, (re)loading, and monitoring of your datahub, datalake, or whatever you choose to call your Hadoop data warehouse these days.

Schedoscope makes the headache go away you are certainly going to get when having to frequently rollout and retroactively apply changes to computation logic and data structures in your datahub with traditional ETL job schedulers such as Oozie.

With Schedoscope,
* you never have to create DDL and schema migration scripts;
* you do not have to manually determine which data must be deleted and recomputed in face of retroactive changes to logic or data structures;
* you specify Hive table structures (called "views"), partitioning schemes, storage formats, dependent views, as well as transformation logic in a concise Scala DSL;
* you have a wide range of options for expressing data transformations - from file operations and MapReduce jobs to Pig scripts, Hive queries, and Oozie workflows;
* you benefit from Scala's static type system and your IDE's code completion to make less typos that hit you late during deployment or runtime;
* you can easily write unit tests for your transformation logic in [ScalaTest](http://www.scalatest.org/) and run them quickly right out of your IDE;
* you schedule jobs by expressing the views you need - Schedoscope takes care that all required dependencies - and only those-  are computed as well;
* you can easily  export view data in parallel to external systems such as Redis caches, JDBC, or Kafka topics;
* you have Metascope - a nice metadata management and data lineage tracing tool - at your disposal;
* you achieve a higher utilization of your YARN cluster's resources because job launchers are not YARN applications themselves that consume cluster capacitity.

## Getting Started

Get a glance at 

- [Schedoscope's features](https://github.com/ottogroup/schedoscope/wiki/Schedoscope-at-a-Glance)

Build it:

     [~]$ git clone https://github.com/ottogroup/schedoscope.git
     [~]$ cd schedoscope
     [~/schedoscope]$  MAVEN_OPTS='-XX:MaxPermSize=512m' mvn clean install
     
Follow the Open Street Map tutorial to install and run Schedoscope in a standard Hadoop distribution image:

- [Open Street Map Tutorial](https://github.com/ottogroup/schedoscope/wiki/Open%20Street%20Map%20Tutorial)

Take a look at the View DSL Primer to get more information about the capabilities of the Schedoscope DSL:

- [Schedoscope View DSL Primer](https://github.com/ottogroup/schedoscope/wiki/Schedoscope%20View%20DSL%20Primer)

Read more about how Schedoscope actually performs its scheduling work:

- [Schedoscope Scheduling](https://github.com/ottogroup/schedoscope/wiki/Scheduling)

More documentation can be found here:

- [Schedoscope Wiki](https://github.com/ottogroup/schedoscope/wiki)

Check out Metascope! It's an add-on to Schedoscope for collaborative metadata management, data discovery, exploration, and data lineage tracing:

- [Metascope Primer](https://github.com/ottogroup/schedoscope/wiki/Metascope%20Primer)

![Metascope](https://raw.githubusercontent.com/wiki/ottogroup/schedoscope/images/lineage.png)

## When is Schedoscope not for you?

Schedoscope is based on the following assumptions:
* data are largely relational and meaningfully representable as Hive tables;
* there is enough cluster time and capacity to actually allow for retroactive recomputation of data;
* it is acceptable to compile table structures, dependencies, and transformation logic into what is effectively a project-specific scheduler.

Should any of those assumptions not hold in your context, you should probably look for a different scheduler.

## Origins

Schedoscope was conceived at the Business Intelligence department of [Otto Group](http://ottogroup.com/en/die-otto-group.php)

## Contributions

The following people have contributed to the various parts of Schedoscope so far: 

[Utz Westermann](https://github.com/utzwestermann) (maintainer), [Hans-Peter Zorn](https://github.com/hpzorn), [Kassem Tohme](https://github.com/ktohme), [Christian Richter](https://github.com/christianrichter), [Dominik Benz](https://github.com/dominikbenz), [Martin Sänger](https://github.com/martinsaenger), [Alexander Kolb](https://github.com/lofifnc), [Annika Seidler](https://github.com/aleveringhaus).

We would love to get contributions from you as well. We haven't got a formalized submission process yet. If you have an idea for a contribution or even coded one already, get in touch with Utz or just send us your pull request. We will work it out from there.

Please help making Schedoscope better!

## News

###### 08/19/2016 - Release 0.6.5
We have released Version 0.6.5 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

We have factored out Oozie, Pig, and shell transformations and their drivers into separate modules and removed knowledge about which transformation types exist from `schedoscope-core`. Thus, one can now extend Schedoscope with new tranformation types without touching the core.

We have fixed a bug in the test framework where sorting results with null values yielded a null pointer exception.

###### 08/12/2016 - Release 0.6.4
We have released Version 0.6.4 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

We have added: 
* simple parallel [(S)FTP exporting of views](https://github.com/ottogroup/schedoscope/wiki/(S)FTP%20Export)
* the ability to manually assign versions to transformations with `defineVersion` in order to avoid unnecessary recomputations in complex cases where the automatic transformation logic change detection generates too many false positives.

###### 07/01/2016 - Release 0.6.3
We have released Version 0.6.3 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

We have fixed a security issue with Metascope that allowed non-admin users to edit taxonomies.

###### 06/30/2016 - Release 0.6.2
We have released Version 0.6.2 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

Hadoop dependencies have been updated to CDH-5.7.1. A critical bug that could result in no more views transforming while depending views still waiting has been fixed. Reliability of Metascope has been improved.

###### 06/23/2016 - Release 0.6.1
We have released Version 0.6.1 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

Hive transformations are no longer submitted via Hive Server 2 to the cluster but directly via the `hive-exec` library. The reason for this change are stability and resource leakage issues commonly encountered when operating Hive Server 2. Please note that Hive transformations are now issued with `hive.auto.convert.join` set to false by default to limit heap consumption in Schedoscope due to involuntary local map join operations. Refer to [Hive Transformation](https://github.com/ottogroup/schedoscope/wiki/Hive%20Transformations) for more information on how to reenable map joins for queries that need them.

Also: quite a few bug fixes, better error messages when using the CLI client, improved parallelization of JDBC exports.  

###### 05/27/2016 - Release 0.6.0
We have released Version 0.6.0 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

We have updated the checksumming algorithm for Hive transformations such that changes to comments, settings, and formatting no longer affect the checksum. This should significantly reduce operations worries. However, the checksums of all your Hive queries compared to Release 0.5.0 will change. **Take care that you issue a materialization request with [mode `RESET_TRANSFORMATION_CHECKSUMS`](https://github.com/ottogroup/schedoscope/wiki/Scheduling-Command-Reference) when switching to this version to avoid unwanted view recomputations!** Hence the switch of the minor release number.

The test framework now automatically checks whether there is an `ON` condition for each `JOIN` clause in your Hive queries. Also, it checks whether each input view you provide in `basedOn` is also declared as a dependency.

###### 05/21/2016 - Release 0.5.0
We have released Version 0.5.0 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

This is a biggie. We have added Metascope to our distribution. Metascope is a collaborative metadata management, documentation, exploration, and data lineage tracing tool that exploits the integrated specification of data structure, dependencies, and computation logic in Schedoscope views. See [the tutorial](https://github.com/ottogroup/schedoscope/wiki/Open%20Street%20Map%20Tutorial) and the [Metascope primer](https://github.com/ottogroup/schedoscope/wiki/Metascope%20Primer) for more information.


## Community / Forums

- Google Groups: [Schedoscope Users](https://groups.google.com/forum/#!forum/schedoscope-users)
- Twitter: @Schedoscope

## Build Status

[![Build Status](https://travis-ci.org/ottogroup/schedoscope.svg?branch=master)](https://travis-ci.org/ottogroup/schedoscope)

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
