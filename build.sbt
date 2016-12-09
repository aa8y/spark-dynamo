name := "Spark Dynamo"
version := "0.0.1"
scalaVersion := "2.11.8"

libraryDependencies := Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.64",
  "org.apache.spark" % "spark-core_2.11" % "2.0.2",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.2",
  "org.json4s" % "json4s-jackson_2.11" % "3.2.11",
  "org.scalatest" % "scalatest_2.11" % "2.2.6"
)
