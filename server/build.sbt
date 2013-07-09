libraryDependencies in ThisBuild ++= Seq(
    "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
    "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
)

libraryDependencies ++= Seq(
    "com.google.protobuf" % "protobuf-java" % "2.4.1",
    "com.yammer.metrics" % "metrics-core" % "2.2.0",
    "com.yammer.metrics" % "metrics-annotation" % "2.2.0",
    "com.typesafe.akka" %% "akka-kernel" % Version.Akka,
    "com.typesafe.akka" %% "akka-actor" % Version.Akka,
    "com.typesafe.akka" %% "akka-agent" % Version.Akka,
    "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
    "io.netty" % "netty" % "3.6.6.Final",
    "io.spray" % "spray-can" % Version.Spray,
    "io.spray" % "spray-io" % Version.Spray,
    "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
    "org.slf4j" % "jcl-over-slf4j" % "1.7.5",
    "org.slf4j" % "slf4j-log4j12" % "1.7.5",
    "org.hbase" % "asynchbase" % "1.4.1"
)
