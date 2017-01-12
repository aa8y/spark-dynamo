name := "Spark Dynamo"
version := "0.0.2"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.6", "2.11.8")

libraryDependencies := Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.77",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.scalatest" %% "scalatest" % "3.0.1"
)
