name := "tutorial"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3"
)
