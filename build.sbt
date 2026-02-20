ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "pipelines_hw_03",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.1.1",
      "org.apache.spark" %% "spark-sql" % "4.1.1"
    )
  )
