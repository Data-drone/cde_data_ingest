//scalafmtOnCompile in Compile := true

organization := "com.github.datadrone"
name := "spark-cde-etl"

version := "1.0.0"
crossScalaVersions := Seq("2.12.12")
scalaVersion := "2.12.12"
val sparkVersion = "3.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.1.1.7.2.8.0-228" % "provided"
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/Data-drone/cde_data_ingest"))

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

