import sbt._
import Keys._

object MyBuild extends Build {

  val sprayVersion = "1.1-M8"
  val akkaVersion = "2.1.4"

  lazy val exclusions = List(
    ExclusionRule(organization="log4j"),
    ExclusionRule(organization="org.slf4j", name = "slf4j-simple")
  )

  lazy val kafkaCoreProject = ProjectRef(uri("git://github.com/resetius/kafka.git#0.8-scala-2.10-stbx"), "core")
  lazy val mainProject = Project("main", file("."))
        .dependsOn(kafkaCoreProject)
        .dependsOn(kafkaCoreProject % "test->test")
        .settings(
    projectDependencies ~= { deps => deps map {dep =>
      dep.excludeAll(exclusions: _*)
    }}
  )
}
