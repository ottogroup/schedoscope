# Schedoscope

## Introduction

Schedoscope is a scheduling framework for painfree and agile development, testing, (re)loading, and monitoring of your datahub, lake, or whatever you choose to call your Hadoop data warehouse these days.

Based on a concise Scala DSL, 

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

          comment("View of nodes with tags and geohash")

          storedAs(Parquet())
        }

* defining its dependencies is as simple as:

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

* specifying its computation logic is as simple as:

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

* testing it is as simple as:

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
