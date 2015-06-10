# Schedoscope

## Introduction

Schedoscope is a scheduling framework for painfree agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

With Schedoscope,
* you never have to create schema DDL and migration scripts;
* you never have to manually determine which data must be deleted and recomputed in face of retroactive changes to logic or data structures;
* you specify Hive table structures (called "views"), partitioning schemes, storage formats, dependent views, as well as the transformation logic in one file in a concise Scala DSL;
* you have a wide range of options for expressing data transformations - from file operations and MapReduce jobs to Pig scripts, Hive queries, and Oozie workflows;
* you benefit from your Scala IDE's code completion and have less typos hitting you during deployment;
* you can easily write tests for your transformation logic and run them quickly;
* you schedule jobs by expressing the views you need - Schedoscope takes care that all dependencies are computed as well;
* you achieve a higher utilization of your YARN cluster's resources because job launchers are not YARN applications themselves that consume YARN capactity.

Based on Schedoscope's DSL, 

* defining a partitioned Hive table (called "view") is as simple as:

        case class Nodes(
          year: Parameter[String],
          month: Parameter[String]) extends View
          with MonthlyParameterization
          with Id
          with PointOccurrence
          with JobMetadata {

          val version = fieldOf[Int]
          val user_id = fieldOf[Int]
          val longitude = fieldOf[Double]
          val latitude = fieldOf[Double]
          val geohash = fieldOf[String]
          val tags = fieldOf[Map[String, String]]

          comment("View of nodes partitioned by year and month with tags and geohash")

          storedAs(Parquet())
        }

* defining its dependencies on other views is as simple as:

        case class Nodes(
          year: Parameter[String],
          month: Parameter[String]) extends View
          with MonthlyParameterization
          with Id
          with PointOccurrence
          with JobMetadata {

          val version = fieldOf[Int]
          val user_id = fieldOf[Int]
          val longitude = fieldOf[Double]
          val latitude = fieldOf[Double]
          val geohash = fieldOf[String]
          val tags = fieldOf[Map[String, String]]

          dependsOn(() => NodesWithGeohash(p(year), p(month)))
          dependsOn(() => NodeTags(p(year), p(month)))

          comment("View of nodes with tags and geohash")

          storedAs(Parquet())
        }

* specifying the logic how to compute the view out of its dependencies is as simple as:

        case class Nodes(
          year: Parameter[String],
          month: Parameter[String]) extends View
          with MonthlyParameterization
          with Id
          with PointOccurrence
          with JobMetadata {

          val version = fieldOf[Int]
          val user_id = fieldOf[Int]
          val longitude = fieldOf[Double]
          val latitude = fieldOf[Double]
          val geohash = fieldOf[String]
          val tags = fieldOf[Map[String, String]]

          dependsOn(() => NodesWithGeohash(year, month))
          dependsOn(() => NodeTags(year, month))

          transformVia(() =>
            HiveTransformation(
              insertInto(
                this,
                queryFromResource("hiveql/processed/insert_nodes.sql")))          
            .configureWith(Map(
               "year" -> year.v.get,
               "month" -> month.v.get)))

         comment("View of nodes with tags and geohash")

         storedAs(Parquet())
      }

* testing the view logic is as simple as:

        "processed.Nodes" should "load correctly from processed.nodes_with_geohash and stage.node_tags" in {
            new Nodes(p("2013"), p("06")) with test {
              basedOn(nodeTags, nodes)
              then()
              numRows shouldBe 1
              row(v(id) shouldBe "122318",
              v(occurredAt) shouldBe "2013-06-17 15:49:26Z",
              v(version) shouldBe 6,
              v(user_id) shouldBe 50299,
              v(tags) shouldBe Map(
                "TMC:cid_58:tabcd_1:Direction" -> "positive",
                "TMC:cid_58:tabcd_1:LCLversion" -> "8.00",
                "TMC:cid_58:tabcd_1:LocationCode" -> "10696"))
            }
          }

Running the Schedoscope shell, 

* loading the view is as simple as:

        materialize -v schedoscope.example.osm.processed/Nodes/2013/06

* reloading the view in case its dependencies, structure, or logic have changed is as simple as (it is just the same):

        materialize -v schedoscope.example.osm.processed/Nodes/2013/06

* monitoring a view's load state is as simple as:

        views -v schedoscope.example.osm.processed/Nodes/2013/06
        
        RESULTS
        =======
        Details:
        +------------------------------------------------------------+--------------+-------+
        |                         VIEW                               |    STATUS    | PROPS |
        +------------------------------------------------------------+--------------+-------+
        |            schedoscope.example.osm.processed/Nodes/2013/06 | waiting      |       |
        | schedoscope.example.osm.processed/NodesWithGeohash/2013/06 | materialized |       |
        |             schedoscope.example.osm.stage/NodeTags/2013/06 | transforming |       |
        +------------------------------------------------------------+--------------+-------+
        Total: 3

        materialized: 1
        waiting: 1
        transforming: 1


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
