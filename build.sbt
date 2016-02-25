import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

/**
  * Organization:
  */
organization     := "com.github.jparkie"
organizationName := "jparkie"

/**
  * Library Meta:
  */
name     := "Spark2Elasticsearch"
licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

/**
  * Scala:
  */
scalaVersion       := "2.11.7"
crossScalaVersions := Seq("2.10.6", "2.11.7")
crossVersion       := CrossVersion.binary

/**
  * Library Dependencies:
  */

// Exclusion Rules:
val guavaRule              = ExclusionRule("com.google.guava", "guava")
val sparkNetworkCommonRule = ExclusionRule("org.apache.spark", "spark-network-common")

// Versions:
val SparkVersion         = "1.4.1"
val SparkTestVersion     = "1.4.1_0.3.0"
val ScalaTestVersion     = "2.2.4"
val ElasticsearchVersion = "2.2.0"
val Slf4jVersion         = "1.7.10"

// Dependencies:
val sparkCore         = "org.apache.spark"  %% "spark-core"            % SparkVersion         % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
val sparkSql          = "org.apache.spark"  %% "spark-sql"             % SparkVersion         % "provided" excludeAll(sparkNetworkCommonRule, guavaRule)
val sparkTest         = "com.holdenkarau"   %% "spark-testing-base"    % SparkTestVersion     % "test"
val scalaTest         = "org.scalatest"     %% "scalatest"             % ScalaTestVersion     % "test"
val elasticsearch     = "org.elasticsearch"  % "elasticsearch"         % ElasticsearchVersion
val slf4j             = "org.slf4j"          % "slf4j-api"             % Slf4jVersion

libraryDependencies ++= Seq(sparkCore, sparkSql, sparkTest, scalaTest, elasticsearch, slf4j)

/**
  * Tests:
  */
parallelExecution in Test := false

/**
  * Scalariform:
  */
SbtScalariform.scalariformSettings
ScalariformKeys.preferences := FormattingPreferences()
  .setPreference(RewriteArrowSymbols, false)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(SpacesAroundMultiImports, true)

/**
  * Publishing to Sonatype:
  */
publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := {
  <url>https://github.com/jparkie/Spark2Elasticsearch</url>
  <scm>
    <url>git@github.com:jparkie/Spark2Elasticsearch.git</url>
    <connection>scm:git:git@github.com:jparkie/Spark2Elasticsearch.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jparkie</id>
      <name>Jacob Park</name>
      <url>https://github.com/jparkie</url>
    </developer>
  </developers>
}

/**
  * Release:
  */
import ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)
