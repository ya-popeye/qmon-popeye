name:="popeye-server"

organization := "ru.yandex.qmon"

version := "1.0"

scalaVersion := "2.10.1"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies ++= Seq(
    "org.scalatest"         %% "scalatest"  % "1.9.1" % "test",
    "com.google.protobuf"   % "protobuf-java"   % "2.4.1",
    "com.yammer.metrics"    % "metrics-core"    % "2.2.0",
    "com.yammer.metrics"    % "metrics-annotation" % "2.2.0",
    "io.netty"              % "netty"           % "3.6.6.Final",
    "com.typesafe.akka"     %% "akka-kernel"     % akkaVersion,
    "com.typesafe.akka"     %% "akka-actor"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-agent"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-slf4j"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-testkit"    % akkaVersion % "test",
    "io.spray"            %  "spray-can"        % sprayVersion,
    "io.spray"            %   "spray-io"        % sprayVersion,
    "io.spray"            %   "spray-routing"   % sprayVersion,
    "io.spray"            %   "spray-testkit"   % sprayVersion,
    "org.codehaus.jackson"  %   "jackson-core-asl"      % "1.9.12",
    "org.slf4j"           %   "jcl-over-slf4j"  % "1.7.5",
    "org.slf4j"           %   "slf4j-log4j12"   % "1.7.5"
)

