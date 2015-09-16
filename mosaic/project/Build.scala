import sbt._
import sbt.Keys._
import scala.util.Properties

// sbt-assembly
import sbtassembly.AssemblyPlugin.autoImport._

object Version {
  val tiler       = "0.1.0"
  val scala       = "2.10.5"
  val spark       = "1.4.1"

  val geotrellis  = "0.10.0-SNAPSHOT"
}

object Build extends Build {
  // Default settings
  override lazy val settings =
    super.settings ++
  Seq(
    version := Version.tiler,
    scalaVersion := Version.scala,
    organization := "org.hotosm",

    scalacOptions ++=
      Seq(
        "-deprecation",
        "-feature",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-language:existentials"
        ),

    licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
  )

  // Project: root
  lazy val root =
    Project("root", file("."))
      .settings(rootSettings:_*)

  lazy val rootSettings =
    Seq(
      organization := "org.hotosm",
      name := "oam-tiler",

      scalaVersion := Version.scala,

      // raise memory limits here if necessary
      javaOptions += "-Xmx5G",

      mainClass in (Compile, run) := Some("org.hotosm.oam.Tiler"),

      fork := true,
      connectInput in run := true,

      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % Version.spark,
//        "com.github.nscala-time" %% "nscala-time" % "1.6.0",
        "com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
//        "de.javakaffee" % "kryo-serializers" % "0.33",
        "org.apache.commons" % "commons-io" % "1.3.2",
        "io.spray"        %% "spray-json"    % "1.3.1",
        "com.amazonaws" % "aws-java-sdk-s3" % "1.9.34",
        "com.azavea.geotrellis" %%  "geotrellis-testkit" % "0.10.0-SNAPSHOT" % "test",
        "org.scalatest" %%  "scalatest" % "2.2.0" % "test"
      ),

      test in assembly := {}
    )  ++ net.virtualvoid.sbt.graph.Plugin.graphSettings
}
