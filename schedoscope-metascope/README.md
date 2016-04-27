# Metascope

Metascope is a metadata management tool built at Otto Group. Metascope is able to collect technical, operational and business metadata from your Hadoop Datahub and displays the metadata to the user through a web service.

Metascope serves as an extension to [Schedoscope](https://github.com/ottogroup/schedoscope). 

Check out the [**wiki**](https://github.com/ottogroup/metascope/wiki) to learn more about Metascope.

## Features

* Overview of data: Get a complete overview of all your tables and partitions and their metadata
* Discover your data: All metadata is indexed and is ready for query
* Search and filters: Metascope will let you apply filter and facets to your query to find the datasets you need in seconds
* Data Lineage: Get dependency and lineage information on table and partition level
* At a glance: Get all important and critical metadata in one spot, including data sample, data distribution, transformations and many more
* Collaboration: No need to document your datasets in external portals like Confluence. Create inline documentation for your data and create comments on table and field level
* Business Metadata: Enrich your data with your own, individual taxonomy. Create tags and assign them to the various datasets to categorize all your data
* Webinterface and REST API: Query the metadata repository through the Metascope webservice or use the REST API to automate your applications
* Check out the [User Guide](https://github.com/ottogroup/metascope/wiki/User-Guide) for a complete list of all features and screenshots



![Metascope](https://raw.githubusercontent.com/wiki/ottogroup/metascope/images/metascope.png)



## Getting started

### Manual Build:
1. Clone this repo: `git clone https://github.com/ottogroup/metascope`.

2. Build with maven: `maven install`.

3. The .../target/metascope folder includes all necessary files to execute Metascope.

### Quick Start:
Check out [Quick Start Guide](https://github.com/ottogroup/metascope/wiki/Quick-Start-Guide)

## Origins
The project was conceived at the Business Intelligence department of Otto Group.

## Build Status

[![Build Status](https://travis-ci.org/ottogroup/metascope.svg?branch=master)](https://travis-ci.org/ottogroup/metascope)

## License
Licensed under the [Apache License 2.0](https://github.com/ottogroup/metascope/blob/master/LICENSE)
