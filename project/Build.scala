import sbt._
import Keys._
import net.virtualvoid.sbt.graph.{Plugin => Dep}


object Version {
  val Scala = "2.10.1"
  val Akka = "2.1.4"
  val Spray = "1.1-M8"
  val Hadoop = "1.2.0"
  val HBase = "0.94.6"
  val ScalaTest = "1.9.1"
  val Jackson = "1.8.8"
}


object Compiler {
  val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature", "-language:postfixOps", "-language:implicitConversions"),
    javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")
  )
}

object Tests {
  val defaultSettings = Seq(
    parallelExecution in Test := false
  )
}

object PopeyeBuild extends Build {

  lazy val defaultSettings =
    Defaults.defaultSettings ++
    Compiler.defaultSettings ++
    Tests.defaultSettings ++
    Dep.graphSettings

  lazy val kafka = ProjectRef(
    build = uri("git://github.com/resetius/kafka.git#0.8-scala-2.10-stbx"),
    project = "core")

  lazy val popeye = Project(
    id = "popeye",
    base = file("."),
    settings = defaultSettings)
  .aggregate(popeyeServer)

  lazy val popeyeServer = Project(
    id="popeye-server",
    base = file("server"),
    settings = defaultSettings)
    .dependsOn(kafka)
    .dependsOn(kafka % "test->test")
    .settings(
    projectDependencies ~= {
      deps => deps map {
        dep =>
          dep.excludeAll(
            ExclusionRule(organization = "log4j"),
            ExclusionRule(organization = "org.slf4j", name = "slf4j-simple")
          )
      }
    }
  )
}
