name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1",
  "org.apache.spark" %% "spark-sql" % "2.0.1"
//  "com.databricks" %% "spark-csv" % "1.5.0"
)
