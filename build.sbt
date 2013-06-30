name:="qmon-parent"

organization := "ru.yandex.qmon"

version := "1.0"

scalaVersion := "2.10.+"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/"
)

libraryDependencies ++= Seq(
    "org.scalatest"         %% "scalatest"  % "2.0.M5b" % "test",
    "com.google.protobuf"   % "protobuf-java"   % "2.4.1",
    "com.yammer.metrics"    % "metrics-core"    % "2.2.0",
    "com.yammer.metrics"    % "metrics-annotation" % "2.2.0",
    "io.netty"              % "netty"           % "3.6.6.Final",
    "com.typesafe.akka"     %% "akka-kernel"     % akkaVersion,
    "com.typesafe.akka"     %% "akka-actor"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-agent"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-slf4j"      % akkaVersion,
    "com.typesafe.akka"     %% "akka-testkit"    % akkaVersion % "test",
    "io.spray"            %  "spray-can"       % sprayVersion,
    "io.spray"            %   "spray-io"        % sprayVersion,
    "io.spray"            %   "spray-routing"   % sprayVersion,
    "io.spray"            %   "spray-testkit"   % sprayVersion,
    "org.codehaus.jackson"  %   "jackson-core-asl"      % "1.9.12",
    "org.specs2"          %%  "specs2"          % "1.14" % "test",
    "junit"               %   "junit"           % "4.10" % "test",
    "com.novocode" % "junit-interface" % "0.8" % "test->default"
)



