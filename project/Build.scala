import sbt._
import sbt.ExclusionRule
import sbt.Keys._
import com.twitter.sbt._
import net.virtualvoid.sbt.graph.{Plugin => Dep}
import de.johoop.findbugs4sbt.FindBugs._
import de.johoop.findbugs4sbt.ReportType
import de.johoop.findbugs4sbt.Effort
import sbtassembly.Plugin._
import scala._

object Util {
  implicit def dependencyFilterer(deps: Seq[ModuleID]) = new Object {
    def excluding(group: String, artifactId: String) =
      deps.map(_.exclude(group, artifactId))
    def excluding(rules: ExclusionRule*) =
      deps.map(_.excludeAll(rules :_*))
  }
}

object Compiler {
  val defaultSettings = Seq(
    scalacOptions in Compile ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature",
      "-language:postfixOps", "-language:implicitConversions"),
    javacOptions in Compile ++= Seq("-source", "1.7", "-target", "1.7"),
    resolvers ++= Seq(
      "spray repo" at "http://repo.spray.io",
      "cdh" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      Resolver.url("octo47 repo", url("http://octo47.github.io/repo/"))({
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

object FindBugs {
  val settings = findbugsSettings ++ Seq(
    findbugsReportType := ReportType.Xml,
    findbugsIncludeFilters := Some(
      <FindBugsFilter>
        <Match>
          <Package name="~popeye.*"/>
        </Match>
      </FindBugsFilter>
    ),
    findbugsExcludeFilters := Some(
      <FindBugsFilter>
        <Match>
          <Package name="~.*"/>
        </Match>
      </FindBugsFilter>
    ),
    findbugsEffort := Effort.Low
  )
}

object Version {
  val Scala = "2.10.4"
  val Akka = "2.2.3"
  val Spray = "1.2-RC2"
  val ScalaTest = "2.0"
  val Mockito = "1.9.0"
  val Jackson = "1.8.8"
  val Kafka = "0.8.1"
  val Metrics = "3.0.0"
  val Slf4j = "1.7.7"
  val Logback = "1.0.7"
  val Snappy = "1.0.5"
  val Guava = "11.0.2"
  val FakeHBase = "0.1.2"
  val Scopt = "3.1.0"
  val Avro = "1.7.5"
  val Hadoop = "2.3.0-cdh5.0.1"
  val HadoopTest = "2.3.0-mr1-cdh5.0.1"
  val HBase = "0.96.1.1-cdh5.0.1"

  val slf4jDependencies: Seq[ModuleID] = Seq(
    "org.slf4j" % "jcl-over-slf4j" % Version.Slf4j,
    "org.slf4j" % "log4j-over-slf4j" % Version.Slf4j,
    "ch.qos.logback" % "logback-classic" % Version.Logback
  )

  val commonExclusions = Seq(
    ExclusionRule(name = "jline"),
    ExclusionRule(name = "junit")
  )

  val slf4jExclusions = Seq(
    ExclusionRule(name = "slf4j-log4j12"),
    ExclusionRule(name = "slf4j-simple")
  )
}

object HBase {

  import Util._

  val settings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.hbase" % "hbase-common" % Version.HBase,
      "org.apache.hbase" % "hbase-client" % Version.HBase,
      "org.apache.hbase" % "hbase-server" % Version.HBase,
      "org.apache.hbase" % "hbase-hadoop-compat" % Version.HBase
    ).excluding(
      ExclusionRule(name = "commons-daemon"),
      ExclusionRule(name = "commons-cli"),
      ExclusionRule(name = "commons-logging"),
      ExclusionRule(name = "jsp-api"),
      ExclusionRule(name = "servlet-api"),
      ExclusionRule(name = "kfs"),
      ExclusionRule(name = "mockito-all"),
      ExclusionRule(organization = "org.jruby"),
      ExclusionRule(organization = "tomcat"),
      ExclusionRule(organization = "org.apache.thrift"),
      ExclusionRule(organization = "com.jcraft"),
      ExclusionRule(organization = "org.mortbay.jetty"),
      ExclusionRule(organization = "com.sun.jersey")
    ).excluding(Version.slf4jExclusions :_*)
     .excluding(Version.commonExclusions :_*)
  )
}

object Publish {

  lazy val settings = Seq(
//    credentials += Credentials(Path.userHome / ".ivy2" / "credentials.txt"),
    publishTo <<= version { v=>
      if (v.endsWith("SNAPSHOT"))
        Some("yandex_common_snapshots"
          at "http://maven.yandex.net/nexus/content/repositories/yandex_common_snapshots/")
      else
        Some("yandex_common_releases"
          at "http://maven.yandex.net/nexus/content/repositories/yandex_common_releases/")
    }
  )
}

object PopeyeBuild extends Build {

  import Util._
  import PackageDist._

  val zipArtifact =
    SettingKey[Artifact]("package-dist-zip-artifact", "Artifact definition for built dist");

  lazy val releaseSettings = Seq(
    Defaults.defaultSettings,
    DefaultRepos.newSettings,
    ArtifactoryPublisher.newSettings,
    GitProject.gitSettings,
    VersionManagement.newSettings,
    ReleaseManagement.newSettings
  ).foldLeft(Seq[Setting[_]]()) { (s, a)  => s ++ a} ++ Seq(
    exportJars := false
  )

  lazy val distSettings = Seq(
    BuildProperties.newSettings,
    PublishSourcesAndJavadocs.newSettings,
    PackageDist.newSettings
  ).foldLeft(Seq[Setting[_]]()) { (s, a) => s ++ a}

  lazy val defaultSettings =
    Defaults.defaultSettings ++
      Compiler.defaultSettings ++
      Tests.defaultSettings ++
      Dep.graphSettings ++
      Publish.settings ++
      PopeyeBuild.releaseSettings ++
      Seq(exportJars := true)

  lazy val popeyeCore = Project(
    id = "popeye-core",
    base = file("core"),
    settings = defaultSettings ++ FindBugs.settings ++ HBase.settings)
    .settings(
      libraryDependencies ++= Version.slf4jDependencies ++ Seq(
        "com.typesafe" % "config" % "1.0.2",
        "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
        "org.apache.hadoop" % "hadoop-common" % Version.Hadoop % "test"
      ).excluding(Version.slf4jExclusions: _*)
        .excluding(Version.commonExclusions: _*)
    )

  lazy val popeyeApp = Project(
    id = "popeye-app",
    base = file("app"),
    settings = defaultSettings ++ FindBugs.settings ++ HBase.settings)
    .settings(
    libraryDependencies ++= Version.slf4jDependencies ++ Seq(
      "org.apache.hadoop" % "hadoop-common" % Version.Hadoop,
      "org.apache.hadoop" % "hadoop-client" % Version.Hadoop,
      "com.github.scopt" %% "scopt" % Version.Scopt,
      "com.google.protobuf" % "protobuf-java" % "2.5.0",
      "org.apache.kafka" %% "kafka" % Version.Kafka,
      "nl.grons" %% "metrics-scala" % Version.Metrics,
      "com.codahale.metrics" % "metrics-jvm" % "3.0.0",
      "org.codehaus.jackson" % "jackson-core-asl" % Version.Jackson,
      "com.typesafe.akka" %% "akka-actor" % Version.Akka,
      "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
      "org.xerial.snappy" % "snappy-java" % Version.Snappy,
      "com.github.bigtoast" %% "async-zk-client" % "0.2.3",
      "com.google.guava" % "guava" % Version.Guava % "test",
      "io.spray" % "spray-can" % Version.Spray,
      "io.spray" % "spray-io" % Version.Spray,
      "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4",
      "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
      "org.mockito" % "mockito-core" % Version.Mockito % "test",
      "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test",
      "org.apache.avro" % "avro" % Version.Avro % "test"
    ).excluding(Version.slf4jExclusions :_*)
     .excluding(Version.commonExclusions :_*)
    ).dependsOn(popeyeCore, popeyeHadoopJar)

  lazy val popeyeBench = Project(
    id = "popeye-bench",
    base = file("bench"),
    settings = defaultSettings).dependsOn(popeyeApp % "compile->compile;test->test", popeyeHadoopJar)
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % Version.Hadoop,
        "org.apache.hadoop" % "hadoop-common" % Version.Hadoop classifier "tests",
        "org.apache.hadoop" % "hadoop-hdfs" % Version.Hadoop,
        "org.apache.hadoop" % "hadoop-hdfs" % Version.Hadoop classifier "tests",
        "org.apache.hadoop" % "hadoop-test" % Version.HadoopTest,
        "org.apache.hbase" % "hbase-server" % Version.HBase,
        "org.apache.hbase" % "hbase-server" % Version.HBase classifier "tests",
        "org.apache.hbase" % "hbase-common" % Version.HBase,
        "org.apache.hbase" % "hbase-common" % Version.HBase classifier "tests",
        "org.apache.hbase" % "hbase-hadoop-compat" % Version.HBase,
        "org.apache.hbase" % "hbase-hadoop-compat" % Version.HBase classifier "tests",
        "org.apache.hbase" % "hbase-hadoop2-compat" % Version.HBase,
        "org.apache.hbase" % "hbase-hadoop2-compat" % Version.HBase classifier "tests",
        "nl.grons" %% "metrics-scala" % Version.Metrics,
        "com.typesafe.akka" %% "akka-actor" % Version.Akka,
        "com.typesafe.akka" %% "akka-slf4j" % Version.Akka,
        "org.apache.kafka" %% "kafka" % Version.Kafka,
        "org.scalatest" %% "scalatest" % Version.ScalaTest % "test",
        "org.mockito" % "mockito-core" % Version.Mockito % "test",
        "com.typesafe.akka" %% "akka-testkit" % Version.Akka % "test"
      ).excluding(Version.commonExclusions: _*)
        .excluding(Version.slf4jExclusions: _*),
      fork in test := true,
      javaOptions in test ++= Seq("-Xmx2g", "-Xms1g", "-XX:MaxPermSize=512m")
    ).dependsOn(popeyeCore)

  lazy val popeyeHadoopJar = Project(
    id = "popeye-hadoop-jar",
    base = file("hadoop-jar"),
    settings = defaultSettings ++ HBase.settings ++ assemblySettings).dependsOn(popeyeCore % "compile->compile;test->test")
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % Version.Hadoop % "provided"
      ).excluding(Version.commonExclusions: _*)
        .excluding(Version.slf4jExclusions: _*)
    )

  lazy val popeye = Project(
    id = "popeye",
    base = file("."),
    settings = defaultSettings ++ distSettings ++ Seq(
      packageDistScriptsOutputPath <<= packageDistDir { b => Some(b / "bin")},
      packageDistScriptsPath <<= baseDirectory { b => Some(b / "src/main/bin")},
      packageDistConfigPath <<= baseDirectory { b => Some(b / "src/main/etc")},

      // make sbt-package-dist happy
      packageDistCopyJars <<= packageDistDir map { pd =>
        val file = pd / ".build"
        IO.touch(file, setModified = true)
        Set[File](file)
      },
      cleanFiles <+= baseDirectory { b => b / "dist"},
      zipArtifact <<= name(Artifact(_, "zip", "zip"))
    ) ++ addArtifact(zipArtifact, packageDist).settings)
    .aggregate(popeyeApp, popeyeBench, popeyeHadoopJar)
    .dependsOn(popeyeApp, popeyeBench, popeyeHadoopJar)


}

