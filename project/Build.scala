import sbt._

object MyBuild extends Build {

  lazy val kafkaCoreProject = ProjectRef(uri("git://github.com/resetius/kafka.git#0.8-scala-2.10-stbx"), "core")
  lazy val mainProject = Project("main", file(".")) dependsOn(kafkaCoreProject)

}

