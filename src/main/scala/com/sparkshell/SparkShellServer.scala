package com.sparkshell

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfigBuilder {
  /**
   * Build SparkConf from custom configurations.
   */
  def buildSparkConf(customConfigs: Map[String, String]): SparkConf = {
    println("=" * 60)
    println("Building Spark Configuration")
    
    // Create SparkConf with base settings
    val conf = new SparkConf()
      .setAppName("Spark SQL REST Server")
      .setMaster("local[*]")
    
    // Base configuration
    val baseConfigs = Map(
      "spark.sql.warehouse.dir" -> "/tmp/spark-warehouse",
      // Cloud storage configurations (enable S3, Azure, GCS filesystems)
      "spark.hadoop.fs.s3.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3n.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.azure.impl" -> "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
      "spark.hadoop.fs.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    
    // Merge base configs with custom configs (custom configs override base)
    val allConfigs = baseConfigs ++ customConfigs
    
    // Apply all configurations
    allConfigs.foreach { case (key, value) =>
      println(s"  Config: $key = $value")
      conf.set(key, value)
    }

    println("=" * 60)
    conf
  }
  
  /**
   * Build SparkSession from SparkConf.
   */
  def buildSparkSession(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    
    // Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    println(s"Spark version: ${spark.version}")
    
    // Eagerly initialize Spark internals
    try {
      spark.sql("SELECT 1").collect()
      println("Spark initialized successfully")
    } catch {
      case e: Exception =>
        println(s"Warning during Spark initialization: ${e.getMessage}")
    }
    
    spark
  }
}


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

  def main(args: Array[String]): Unit = {
    // Parse arguments: port [key1=value1 key2=value2 ...]
    val port = if (args.length > 0) args(0).toInt else DEFAULT_PORT
    val customConfigs = if (args.length > 1) {
      args.drop(1).map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) Some((parts(0), parts(1))) else None
      }.flatten.toMap
    } else {
      Map.empty[String, String]
    }

    // Build SparkConf from command-line arguments
    val sparkConf = SparkConfigBuilder.buildSparkConf(customConfigs)
    
    // Build SparkSession from SparkConf
    val spark = SparkConfigBuilder.buildSparkSession(sparkConf)

    // Start REST API Server
    val server = new SparkShellServer(spark, port)
    server.start()
    server.blockUntilShutdown()
  }
}
