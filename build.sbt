name:="qmon-parent"

organization := "ru.yandex.qmon"

version := "1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
    "org.scalatest"         % "scalatest_2.10"  % "2.0.M5b" % "test",
    "com.google.protobuf"   % "protobuf-java"   % "2.4.1",
    "com.yammer.metrics"    % "metrics-core"    % "2.2.0",
    "com.yammer.metrics"    % "metrics-annotation" % "2.2.0"
)



