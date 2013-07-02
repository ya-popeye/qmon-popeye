import sbt._

object MyBuild extends Build {

  val sprayVersion = "1.1-M8"
  val akkaVersion = "2.1.4"

  lazy val kafkaCoreProject = ProjectRef(uri("git://github.com/resetius/kafka.git#0.8-scala-2.10-stbx"), "core")
  lazy val mainProject = Project("main", file(".")) dependsOn(kafkaCoreProject) dependsOn(kafkaCoreProject % "test->test")

}
