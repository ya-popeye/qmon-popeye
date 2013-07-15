import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.{Plugin => Dep}

object Version {
  val Scala = "2.10.1"
  val Akka = "2.2.0"
  val Spray = "1.2-M8"
  val Hadoop = "1.2.0"
  val HBase = "0.94.6"
  val ScalaTest = "1.9.1"
  val Mockito = "1.9.0"
  val Jackson = "1.8.8"
  val Kafka = "0.8.0-beta1"
}


object Compiler {
  val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature",
      "-language:postfixOps", "-language:implicitConversions"),
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

  lazy val popeye = Project(
    id = "popeye",
    base = file("."),
    settings = defaultSettings)
    .aggregate(popeyeServer)

  lazy val popeyeServer = Project(
    id = "popeye-server",
    base = file("server"),
    settings = defaultSettings)
    .settings(
    managedDirectory := file("target/libs/"),
    retrieveManaged := true,
    retrievePattern := "[conf]/[artifact](-[revision])(-[classifier]).[ext]",
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "org.apache.kafka" %% "kafka" % Version.Kafka % "compile->compile;test->test"
        exclude("org.slf4j", "slf4j-simple"),
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "io.spray" % "spray-can" % Version.Spray,
      "io.spray" % "spray-io" % Version.Spray,
      "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
      "org.slf4j" % "jcl-over-slf4j" % "1.7.5",
      "org.slf4j" % "slf4j-log4j12" % "1.7.5",
      "org.hbase" % "asynchbase" % "1.4.1"
        exclude("org.slf4j", "log4j-over-slf4j")
        exclude("org.slf4j", "jcl-over-slf4j"),
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    )
  )
}

