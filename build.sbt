import sbt.Keys.scalaVersion
import sbt.Opts.resolver.{sonatypeSnapshots, sonatypeStaging}

lazy val scala213               = "2.13.6"
lazy val scala212               = "2.12.14"
lazy val scala211               = "2.11.12"
lazy val supportedScalaVersions = Seq(scala213, scala212, scala211)

organization := "io.github.agolovenko"
homepage := Some(url("https://github.com/agolovenko/avro-json-tools"))
scmInfo := Some(ScmInfo(url("https://github.com/agolovenko/avro-json-tools"), "git@github.com:agolovenko/avro-json-tools.git"))
developers := List(Developer("agolovenko", "agolovenko", "ashotik@gmail.com", url("https://github.com/agolovenko")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

lazy val root = project
  .in(file("."))
  .enablePlugins(GitVersioning)
  .settings(
    name := "avro-json-tools",
    scalaVersion := scala212,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= new Dependencies(scalaVersion.value).all,
    publishMavenStyle := true,
    publishTo := Some(if (isSnapshot.value) sonatypeSnapshots else sonatypeStaging),
    scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard",
//      "-Ywarn-unused"
    ),
    initialize := {
      val _           = initialize.value
      val javaVersion = sys.props("java.specification.version")
      if (javaVersion != "1.8") sys.error(s"Java 1.8 is required for this project. Found $javaVersion instead.")
    }
  )
