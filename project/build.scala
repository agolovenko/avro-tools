import sbt._

class Dependencies(scalaVersion: String) {
  private def playJsonVersion = if (scalaVersion.startsWith("2.11")) "2.7.4" else "2.9.2"

  private def apacheAvro       = "org.apache.avro"        % "avro"                     % "1.10.1"
  private def collectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.1"
  private def playJson         = "com.typesafe.play"      %% "play-json"               % playJsonVersion
  private def scalaTest        = "org.scalatest"          %% "scalatest"               % "3.2.3"

  def all: Seq[ModuleID] = Seq(
    collectionCompat % Compile,
    playJson         % Compile,
    apacheAvro       % Compile,
    scalaTest        % Test
  )
}
