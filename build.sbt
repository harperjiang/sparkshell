name := "SparkShell"

version := "0.1.0"

scalaVersion := "2.13.15"

// Main class for easy running
Compile / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / assemblyJarName := "sparkshell.jar"

// Assembly merge strategy
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ => MergeStrategy.concat
      case "versions" :: _ => MergeStrategy.first
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".properties") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// JVM options for Java 9+ compatibility with Spark
fork := true
run / fork := true
run / connectInput := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)

libraryDependencies ++= Seq(
  // Spark SQL
  "org.apache.spark" %% "spark-sql" % "4.0.0",

  // Cloud Storage Support (S3, Azure, GCS)
  "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
  "software.amazon.awssdk" % "bundle" % "2.23.19",

  "org.apache.iceberg" % "iceberg-spark-runtime-4.0_2.13" % "1.10.0",
  "org.apache.iceberg" % "iceberg-aws-bundle" % "1.10.0",

  // REST API
  "com.sparkjava" % "spark-core" % "2.9.4",
  "com.google.code.gson" % "gson" % "2.10.1"
)
