# ![Schedoscope](https://raw.githubusercontent.com/wiki/ottogroup/schedoscope/images/schedoscope_logo.jpg)

## Introduction

Schedoscope is a scheduling framework for painfree agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

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
* you achieve a higher utilization of your YARN cluster's resources because job launchers are not YARN applications themselves that consume cluster capacitity.

## Getting Started

Get a glance at 

- [Schedoscope's features](https://github.com/ottogroup/schedoscope/wiki/Schedoscope-at-a-Glance)

Build it:

     [~]$ git clone https://github.com/ottogroup/schedoscope.git
     [~]$ cd schedoscope
     [~/schedoscope]$  MAVEN_OPTS='-XX:MaxPermSize=512m' mvn clean install
     
Follow the Open Street Map tutorial to install and run Schedoscope in a standard Hadoop distribution image within minutes:

- [Open Street Map Tutorial](https://github.com/ottogroup/schedoscope/wiki/Open%20Street%20Map%20Tutorial)

Take a look at the View DSL Primer to get more information about the capabilities of the Schedoscope DSL:

- [Schedoscope View DSL Primer](https://github.com/ottogroup/schedoscope/wiki/Schedoscope%20View%20DSL%20Primer)

More documentation can be found here:
* [Schedoscope Wiki](https://github.com/ottogroup/schedoscope/wiki)

Check out Metascope! It's a meta data management and data discovery tool which serves as an add-on to Schedoscope.
- [Metascope Primer](Metascope Primer)

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

[Utz Westermann](https://github.com/utzwestermann) (maintainer), [Hans-Peter Zorn](https://github.com/hpzorn), [Kassem Tohme](https://github.com/ktohme), [Christian Richter](https://github.com/christianrichter), [Dominik Benz](https://github.com/dominikbenz), [Martin Sänger](https://github.com/martinsaenger), [Annika Seidler](https://github.com/aleveringhaus).

We would love to get contributions from you as well. We haven't got a formalized submission process yet. If you have an idea for a contribution or even coded one already, get in touch with Utz or just send us your pull request. We will work it out from there.

Please help making Schedoscope better!

## News

###### 04/26/2016 - Release 0.4.3

We have released Version 0.4.3 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

This release makes `exportTo` support the `isPrivacySensitive` clause of the View DSL. Fields and partition parameters marked with `isPrivacySensitive` are hashed during export.

###### 04/25/2016 - Release 0.4.2

We have released Version 0.4.2 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

This is a bugfix release solving an issue with an overly pedantic view pattern checker in the HTTP-API sabotaging the `views` command.

###### 04/22/2016 - Release 0.4.0

We have released Version 0.4.0 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom). 

This is a big release including:

* a complete overhaul of the scheduling state machine with significant improvement of test coverage

* `exportTo` clause for simple, seamless, and parallel export of views to relational databases, Redis key-value stores, and Kafka topics (see [View DSL Primer](https://github.com/ottogroup/schedoscope/wiki/Schedoscope-View-DSL-Primer))

* new materialization modes `SET_ONLY` and `TRANSFORMATION_ONLY` for more flexible ops (see [Scheduling Command Reference](https://github.com/ottogroup/schedoscope/wiki/Scheduling-Command-Reference))

###### 01/22/2016 - Release 0.3.5

We have released Version 0.3.5 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

This release migrates Schedoscope's Hadoop dependencies to CDH-5.5.1. Furthermore, the test framework has been ported to Hive 1.1.0. Finally, Schedoscope's resilience against Metastore failures has been improved. It is able to reconnect and resume work when the Metastore has become unavailable in more error cases.

###### 11/21/2015 - Release 0.3.4

We have released Version 0.3.4 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

This release fixes a bug in Schedoscope which led to not correctly instantiating ViewActors for newly appearing dependencies such as date changes. Moreover, checksum versioning code has been cleaned up. Note that checksumming is not backwards compatible; you might want to execute your next materializations with the -m RESET_TRANSFORMATION_CHECKSUMS option.

###### 11/13/2015 - Release 0.3.3

We have released Version 0.3.3 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

This release gets some order into the logging framework mess inherited from the various libraries used. It does so by routing Java util logging and Apache commons logging through SLF4J and SLF4J to logback. By muting log4j and setting an appropriate logback-test.xml test outputs are now a lot less chatty.

###### 11/10/2015 - Release 0.3.2

We have released Version 0.3.2 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

This fixes a nasty resource leak in the Touch FileSystemTransformation


###### 11/09/2015 - Release 0.3.1

We have released Version 0.3.1 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

Fields can now be given comments as well: `val id = fieldOf[String]("An ID.")` 

###### 11/06/2015 - Release 0.3.0

We have released Version 0.3.0 as a Maven artifact to our Bintray repository (see [Setting Up A Schedoscope Project](https://github.com/ottogroup/schedoscope/wiki/Setting-up-a-Schedoscope-Project) for an example pom).

This is a _big_ release, with the following major changes:

* Migration to Scala 2.11 and Akka 2.3.14
* Support of Hive 1.1.0 in test framework
* Significant code cleanup 
* Significant round of Scaladoc documentation
* Significant performance improvements when dealing with many views / partitions

Please note that the cleanup incurred some breaking of the API. In particular, the storage format classes have been moved to a separate package `org.schedoscope.dsl.storageformats`. Moreover, the various path builders for views have been renamed in a more systematic way. See [Storage Paths](https://github.com/ottogroup/schedoscope/wiki/Storage-Formats#storage-paths).

## Community / Forums

- Google Groups: [Schedoscope Users](https://groups.google.com/forum/#!forum/schedoscope-users)
- Twitter: @Schedoscope

## Build Status

[![Build Status](https://travis-ci.org/ottogroup/schedoscope.svg?branch=master)](https://travis-ci.org/ottogroup/schedoscope)

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/schedoscope/blob/master/LICENSE)
