name := "spark-latest-1.6.1"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-yarn_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.databricks" % "spark-xml_2.10" % "0.2.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.databricks" % "spark-csv_2.10" % "1.4.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-catalyst_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.101tec" % "zkclient" % "0.8" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.2.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "com.stratio.datasource" % "spark-mongodb_2.10" % "0.11.1" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.mongodb.mongo-hadoop" % "mongo-hadoop-core" % "1.5.1" excludeAll ExclusionRule(organization = "javax.servlet")
  //"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.0-M2" -- Adding this directly as part of Build.sbt throws Guava Version incompatability issues.
  //  "org.apache.hbase" % "hbase-client" % "1.2.1",
  //  "org.apache.hbase" % "hbase" % "1.2.1",
  //  "org.apache.hadoop" % "hadoop-common" % "2.7.2",
  //  "org.apache.hadoop" % "hadoop-client" % "2.7.2",
  //  "org.apache.hbase" % "hbase-server" % "1.2.1"
)
