name := "Fit2Parquet"

version := "0.0.1-SNAPSHOT"

organization := "collectivecycling.com"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  DefaultMavenRepository,
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  Resolver.bintrayRepo("scalaz", "releases"),
  Resolver.typesafeRepo("releases"),
  Resolver.typesafeIvyRepo("releases"),
  Resolver.sonatypeRepo("public")
)

lazy val versions = new {
  val fit2thrift= "0.0.2-SNAPSHOT"
  val scalatest = "3.0.1-SNAP1"
  val scalacheck = "1.13.2"
  val spark = "2.0.1"
  val hadoop = "2.7.2"
  val log4j = "2.7"
  val fit= "20.0.0"
  val joda_time= "2.9.2"
  val nscala_time="2.14.0"
  val parquet_format="2.3.1"
  //val parquet_thrift="1.8.1"
  val parquet_thrift="1.7.0"
  val libthrift="0.9.3"
  //val libthrift="0.7.0"
  val hadoop_lzo="0.4.19"
}

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % versions.scalatest % "test",
  "org.scalacheck" %% "scalacheck" % versions.scalacheck % "test",
  "com.collectivecycling" % "fit2thrift" % versions.fit2thrift,
  "org.apache.spark" %% "spark-core" % versions.spark,
  "org.apache.spark" %% "spark-sql" % versions.spark,
  "org.apache.hadoop" % "hadoop-client" % versions.hadoop,
  //"com.hadoop.gplcompression" % "hadoop-lzo" % versions.hadoop_lzo,
  "org.apache.logging.log4j" % "log4j-api" % versions.log4j,
  "org.apache.logging.log4j" % "log4j-core" % versions.log4j,
  "org.apache.logging.log4j" % "log4j-1.2-api" % versions.log4j,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % versions.log4j,
  "com.garmin" % "fit" % versions.fit,
  "joda-time" % "joda-time" % versions.joda_time,
  "org.apache.parquet" % "parquet-format" % versions.parquet_format,
  "org.apache.parquet" % "parquet-thrift" % versions.parquet_thrift,
  "org.apache.thrift" % "libthrift" % versions.libthrift,
  "com.github.nscala-time" %% "nscala-time" % versions.nscala_time
)
//TODO what is this
initialCommands := "import example._"
