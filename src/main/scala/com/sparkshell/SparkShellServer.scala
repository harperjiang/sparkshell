package com.sparkshell

import org.apache.spark.sql.SparkSession
import org.apache.spark.deploy.SparkSubmitUtils


class SparkShellServer(spark: SparkSession, port: Int) {
  private var restApi: RestApi = _

  def start(): Unit = {
    restApi = new RestApi(spark, port)
    restApi.start()

    sys.addShutdownHook {
      System.err.println("Shutting down REST server...")
      stop()
      spark.stop()
      System.err.println("Server shut down.")
    }
  }

  def stop(): Unit = {
    if (restApi != null) {
      restApi.stop()
    }
  }

  def blockUntilShutdown(): Unit = {
    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        println("Server interrupted, shutting down...")
    }
  }
}

object SparkShellServer {
  private val DEFAULT_PORT = 8080

  /**
   * Resolve and download Maven packages specified in spark.jars.packages.
   * Returns the list of resolved JAR paths.
   */
  private def resolvePackages(packages: String, repositories: Option[String]): Seq[String] = {
    println(s"[SparkShell] Resolving Maven packages: $packages")
    
    try {
      val repos = repositories.getOrElse("https://repo1.maven.org/maven2/")
      val resolved = SparkSubmitUtils.resolveMavenCoordinates(
        packages,
        repos,
        None,  // ivySettings
        exclusions = Nil,
        isTest = false
      )
      
      println(s"[SparkShell] Resolved ${resolved.split(",").length} JARs from Maven")
      resolved.split(",").toSeq
    } catch {
      case e: Exception =>
        println(s"[SparkShell] Warning: Failed to resolve packages: ${e.getMessage}")
        Seq.empty
    }
  }

  def main(args: Array[String]): Unit = {
    // Parse arguments: port [key1=value1 key2=value2 ...]
    val port = if (args.length > 0) args(0).toInt else DEFAULT_PORT
    val sparkConfigs = if (args.length > 1) {
      args.drop(1).map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) Some((parts(0), parts(1))) else None
      }.flatten.toMap
    } else {
      Map.empty[String, String]
    }

    // Handle spark.jars.packages - resolve and download Maven packages
    val resolvedJars = sparkConfigs.get("spark.jars.packages").map { packages =>
      val repos = sparkConfigs.get("spark.jars.repositories")
      resolvePackages(packages, repos)
    }.getOrElse(Seq.empty)

    // Initialize Spark Session with cloud storage support
    val builder = SparkSession.builder()
      .appName("Spark SQL REST Server")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      // Cloud storage configurations (enable S3, Azure, GCS filesystems)
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.azure.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    
    // Add resolved JARs to spark.jars config
    val builderWithJars = if (resolvedJars.nonEmpty) {
      val jarsStr = resolvedJars.mkString(",")
      println(s"[SparkShell] Adding resolved JARs to classpath")
      builder.config("spark.jars", jarsStr)
    } else {
      builder
    }
    
    // Apply custom Spark configurations (excluding spark.jars.packages which was already handled)
    val builderWithConfigs = sparkConfigs
      .filterKeys(k => k != "spark.jars.packages" && k != "spark.jars.repositories")
      .foldLeft(builderWithJars) { case (b, (key, value)) =>
        println(s"Applying custom Spark config: $key = $value")
        b.config(key, value)
      }
    
    val spark = builderWithConfigs.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Print version information
    println("=" * 60)
    println("Spark SQL REST Server")
    println(s"  Spark version: ${spark.version}")
    println(s"  Port: $port")
    println("=" * 60)

    // Eagerly initialize Spark internals to avoid lazy loading issues
    try {
      spark.sql("SELECT 1").collect()
      println("Spark initialized successfully")
    } catch {
      case e: Exception =>
        println(s"Warning during Spark initialization: ${e.getMessage}")
    }

    // Start REST API Server
    val server = new SparkShellServer(spark, port)
    server.start()
    server.blockUntilShutdown()
  }
}
