import sbt._
import sbt.Keys._
import QMonDistPlugin._
import net.virtualvoid.sbt.graph.{Plugin => Dep}
import scala._


object Compiler {
  val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature",
      "-language:postfixOps", "-language:implicitConversions"),
    javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6"),
    resolvers ++= Seq(
      "spray repo" at "http://repo.spray.io",
      "spray repo (nightly)" at "http://nightlies.spray.io",
      "akka repo (nightly)" at "http://repo.akka.io/snapshots",
      Resolver.url("octo47 repo", url("http://octo47.github.com/repo/"))({
        val patt = Resolver.mavenStylePatterns.artifactPatterns
        new Patterns(patt, patt, true)
      })
    ),
    ivyXML :=
      <dependencies>
        <exclude org="log4j"/>
        <exclude org="junit" name="junit"/>
        <exclude org="org.slf4j" name="slf4j-log4j12"/>
      </dependencies>
  )
}

object Tests {

  val defaultSettings = Seq(
    parallelExecution in Test := false
  )
}

object Version {
  val Scala = "2.10.1"
  val Akka = "2.2-SNAPSHOT"
  val Spray = "1.2-20130710"
  val Hadoop = "1.2.0"
  val HBase = "0.94.6"
  val ScalaTest = "1.9.1"
  val Mockito = "1.9.0"
  val Jackson = "1.8.8"
  val Kafka = "0.8.0-beta1-qmon"
  val Metrics = "3.0.0"
  val Slf4j = "1.7.5"
  val Logback = "1.0.7"
  val Snappy = "1.0.4.1"
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
    settings = defaultSettings
  ).aggregate(popeyeCommon, popeyeSlicer, popeyePump, popeyeBench)

  lazy val popeyeCommon = Project(
    id = "popeye-core",
    base = file("core"),
    settings = defaultSettings)
    .settings(
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "org.apache.kafka" %% "kafka" % Version.Kafka % "compile->compile;test->test"
        exclude("org.slf4j", "slf4j-log4j")
        exclude("org.slf4j", "slf4j-simple"),
      "nl.grons" %% "metrics-scala" % Version.Metrics,
      "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
      "org.slf4j" % "jcl-over-slf4j" % Version.Slf4j,
      "org.slf4j" % "log4j-over-slf4j" % Version.Slf4j,
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "ch.qos.logback" % "logback-classic" % Version.Logback,
      "org.hbase" % "asynchbase" % "1.4.1", // TODO: move that to pump module
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    )
  )

  lazy val popeyeSlicer = Project(
    id = "popeye-slicer",
    base = file("slicer"),
    settings = defaultSettings ++ QMonDistPlugin.distSettings)
    .dependsOn(popeyeCommon % "compile->compile;test->test")
    .settings(
    distMainClass := "popeye.transport.SlicerMain",
    distJvmOptions := "-Xms4096M -Xmx4096M -Xss1M -XX:MaxPermSize=256M -XX:+UseParallelGC -XX:NewSize=3G",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % Version.Kafka % "compile->compile;test->test"
        exclude("org.slf4j", "slf4j-simple"),
      "io.spray" % "spray-can" % Version.Spray exclude("com.typesafe.akka", "akka-actor"),
      "io.spray" % "spray-io" % Version.Spray exclude("com.typesafe.akka", "akka-actor"),
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "org.xerial.snappy" % "snappy-java" % Version.Snappy,
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    )
  )

  lazy val popeyePump = Project(
    id = "popeye-pump",
    base = file("pump"),
    settings = defaultSettings ++ QMonDistPlugin.distSettings)
    .dependsOn(popeyeCommon % "compile->compile;test->test")
    .settings(
    distMainClass := "popeye.transport.PumpMain",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % Version.Kafka % "compile->compile;test->test"
        exclude("org.slf4j", "slf4j-simple"),
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    )
  )
  lazy val popeyeBench = Project(
    id = "popeye-bench",
    base = file("bench"),
    settings = defaultSettings ++ QMonDistPlugin.distSettings)
    .settings(
    distMainClass := "popeye.transport.bench.Main",
    libraryDependencies ++= Seq(
      "nl.grons" %% "metrics-scala" % Version.Metrics,
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "io.spray" % "spray-can" % Version.Spray exclude("com.typesafe.akka", "akka-actor"),
      "io.spray" % "spray-io" % Version.Spray exclude("com.typesafe.akka", "akka-actor"),
      "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
      "org.slf4j" % "jcl-over-slf4j" % Version.Slf4j,
      "org.slf4j" % "log4j-over-slf4j" % Version.Slf4j,
      "ch.qos.logback" % "logback-classic" % Version.Logback,
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
    )

  )

}

