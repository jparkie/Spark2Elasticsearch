# Spark2Elasticsearch

Spark Library for Bulk Loading into Elasticsearch

[![Build Status](https://travis-ci.org/jparkie/Spark2Elasticsearch.svg?branch=master)](https://travis-ci.org/jparkie/Spark2Elasticsearch)

## Requirements

Spark2Elasticsearch supports Spark 1.4 and above.

| Spark2Elasticsearch Version | Elasticsearch Version |
| --------------------------- | --------------------- |
| `2.0.X`                     | `2.0.X`               |

## Downloads

#### SBT
```scala
libraryDependencies += "com.github.jparkie" %% "spark2elasticsearch" % "2.0.0-SNAPSHOT"
```

Add the following resolver if needed:

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```

#### Maven
```xml
<dependency>
  <groupId>com.github.jparkie</groupId>
  <artifactId>spark2elasticsearch_2.10</artifactId>
  <version>2.0.0-SNAPSHOT</version>
</dependency>
```

It is planned for Spark2Elasticsearch to be available on the following:
- http://spark-packages.org/

## Features
- Utilizes Elasticsearch Java API with a `TransportClient` to bulk load data from a `DataFrame` into Elasticsearch.

## Usage

### Bulk Loading into Elasticsearch

```scala
// Import the following to have access to the `bulkLoadToEs()` function.
import com.github.jparkie.spark.elasticsearch.sql._

val sparkConf = new SparkConf()
val sc = SparkContext.getOrCreate(sparkConf)
val sqlContext = SQLContext.getOrCreate(sc)

val df = sqlContext.read.parquet("<PATH>")

// Specify the `index` and the `type` to write.
df.bulkLoadToEs(
  esIndex = "twitter",
  esType = "tweets"
)
```

Refer to for more: [SparkEsDataFrameFunctions.scala](https://github.com/jparkie/Spark2Elasticsearch/blob/master/src/main/scala/com/github/jparkie/spark/elasticsearch/sql/SparkEsDataFrameFunctions.scala)

## Configurations

### SparkEsMapperConf

Refer to for more: [SparkEsMapperConf.scala](https://github.com/jparkie/Spark2Elasticsearch/blob/master/src/main/scala/com/github/jparkie/spark/elasticsearch/conf/SparkEsMapperConf.scala)

| Property Name             | Default | Description |
| ------------------------- |:-------:| ------------|
| `es.mapping.id`           | None    | The document field/property name containing the document id. |
| `es.mapping.parent`       | None    | The document field/property name containing the document parent. To specify a constant, use the <CONSTANT> format. |
| `es.mapping.version`      | None    | The document field/property name containing the document version. To specify a constant, use the <CONSTANT> format. |
| `es.mapping.version.type` | None    | Indicates the type of versioning used. http://www.elastic.co/guide/en/elasticsearch/reference/2.0/docs-index_.html#_version_types If es.mapping.version is undefined (default), its value is unspecified. If es.mapping.version is specified, its value becomes external. |
| `es.mapping.routing`      | None    | The document field/property name containing the document routing. To specify a constant, use the <CONSTANT> format. |
| `es.mapping.ttl`          | None    | The document field/property name containing the document time-to-live. To specify a constant, use the <CONSTANT> format. |
| `es.mapping.timestamp`    | None    | The document field/property name containing the document timestamp. To specify a constant, use the <CONSTANT> format. |

### SparkEsTransportClientConf

Refer to for more: [SparkEsTransportClientConf.scala](https://github.com/jparkie/Spark2Elasticsearch/blob/master/src/main/scala/com/github/jparkie/spark/elasticsearch/conf/SparkEsTransportClientConf.scala)

| Property Name                                | Default    | Description |
| -------------------------------------------- |:----------:| ------------|
| `es.nodes`                                   | *Required* | The minimum set of hosts to connect to when establishing a client. Comma separated, colon separated host and port. |
| `es.port`                                    | 9300       | The port to connect when establishing a client. |
| `es.cluster.name`                            | None       | The name of the Elasticsearch cluster to connect. |
| `es.client.transport.sniff`                  | None       | If set to true, will discover other IP addresses to connect. |
| `es.client.transport.ignore_cluster_name`    | None       | Set to true to ignore cluster name validation of connected nodes. |
| `es.client.transport.ping_timeout`           | 5s         | The time to wait for a ping response from a node. |
| `es.client.transport.nodes_sampler_interval` | 5s         | How often to sample / ping the nodes listed and connected. |

### SparkEsWriteConf

Refer to for more: [SparkEsWriteConf.scala](https://github.com/jparkie/Spark2Elasticsearch/blob/master/src/main/scala/com/github/jparkie/spark/elasticsearch/conf/SparkEsWriteConf.scala)

| Property Name                 | Default | Description |
| ----------------------------- |:-------:| ------------|
| `es.batch.size.entries`       | 1000    | The number of IndexRequests to batch in one request. |
| `es.batch.size.bytes`         | 5       | The maximum size in MB of a batch. |
| `es.batch.concurrent.request` | 1       | The number of concurrent requests in flight. |
| `es.batch.flush.timeout`      | 10      | The maximum time in seconds to wait while closing a BulkProcessor. |

## Documentation

Scaladocs are currently unavailable.
