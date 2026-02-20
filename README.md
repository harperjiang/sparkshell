# SparkShell - Pure Spark SQL REST Server Wrapper

A lightweight REST API server that wraps Apache Spark SQL, allowing you to execute SQL commands via HTTP requests. Perfect for Python notebooks and programmatic Spark SQL access.

## Features

- **Pure Spark SQL** - No additional dependencies, just Apache Spark 4.0
- **REST API** - Simple JSON request/response format
- **Cloud Storage Support** - Built-in support for S3, Azure Blob Storage, and Google Cloud Storage
- **Python Wrapper** - Easy-to-use Python client with context manager support
- **Configurable** - Pass custom Spark configurations via Python dataclass

## Project Structure

```
sparkshell/
├── bin/                      # Management scripts
│   ├── start.sh             # Start server in background
│   ├── stop.sh              # Stop server
│   ├── restart.sh           # Restart server
│   └── status.sh            # Check server status
├── build/                    # Self-contained SBT installation
├── src/main/scala/com/sparkshell/
│   ├── RestApi.scala         # REST API implementation
│   ├── SparkShellServer.scala # Server entry point
│   └── SparkSqlExecutor.scala # SQL execution logic
├── spark_shell.py            # Python wrapper
├── build.sbt                 # Build configuration
└── README.md
```

## Building the Application

This project uses its own SBT installation:

```bash
cd sparkshell
build/sbt assembly
```

This will create `target/scala-2.13/sparkshell.jar`.

## Usage

### Option 1: Python Wrapper (Recommended for Notebooks)

The Python wrapper automatically builds the JAR if it doesn't exist, so you can start using it immediately!

```python
from spark_shell import SparkShell, SparkConfig

# Basic usage - automatically builds JAR if needed
with SparkShell(source=".", port=8080) as shell:
    result = shell.execute_sql("SELECT 1 as id, 'Alice' as name")
    print(result)

# With custom Spark configurations
spark_config = SparkConfig(configs={
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.adaptive.enabled": "true"
})

with SparkShell(source=".", port=8080, spark_config=spark_config) as shell:
    # Create a table
    shell.execute_sql("CREATE TABLE users (id INT, name STRING, age INT)")
    
    # Insert data
    shell.execute_sql("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
    
    # Query data
    result = shell.execute_sql("SELECT * FROM users WHERE age > 25")
    print(result)
```

#### Cloud Storage Configuration

**AWS S3:**
```python
from spark_shell import SparkShell, SparkConfig

spark_config = SparkConfig(configs={
    "spark.hadoop.fs.s3a.access.key": "your-access-key",
    "spark.hadoop.fs.s3a.secret.key": "your-secret-key",
    "spark.hadoop.fs.s3a.endpoint": "s3.us-west-2.amazonaws.com"
})

with SparkShell(source=".", spark_config=spark_config) as shell:
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        LOCATION 's3a://my-bucket/path/to/table'
    """)
```

**Azure Blob Storage:**
```python
spark_config = SparkConfig(configs={
    "spark.hadoop.fs.azure.account.key.mystorageaccount.dfs.core.windows.net": "your-storage-key"
})

with SparkShell(source=".", spark_config=spark_config) as shell:
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        LOCATION 'abfss://container@mystorageaccount.dfs.core.windows.net/path/to/table'
    """)
```

**Google Cloud Storage:**
```python
spark_config = SparkConfig(configs={
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/path/to/keyfile.json",
    "spark.hadoop.fs.gs.project.id": "your-project-id"
})

with SparkShell(source=".", spark_config=spark_config) as shell:
    shell.execute_sql("""
        CREATE TABLE my_table (id INT, name STRING) 
        LOCATION 'gs://my-bucket/path/to/table'
    """)
```

### Option 2: Background Mode (Daemon)

Use the convenience scripts to manage the server as a background process:

**Start the server:**
```bash
bin/start.sh [port]          # Default port: 8080
bin/start.sh 3000             # Custom port: 3000
```

**Stop the server:**
```bash
bin/stop.sh
```

**Check status:**
```bash
bin/status.sh
```

**View logs:**
```bash
tail -f sparkshell.log
```

### Option 3: Interactive Mode (SBT)

Run the server in the foreground:

```bash
build/sbt run                 # Default port: 8080
build/sbt "run 3000"          # Custom port: 3000
```

### Option 4: Run JAR Directly

```bash
java -jar target/scala-2.13/sparkshell.jar 8080 \
  spark.executor.memory=2g \
  spark.driver.memory=1g
```

## REST API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "ok",
  "message": "SparkShell server is running"
}
```

### Server Info
```bash
curl http://localhost:8080/info
```

Response:
```json
{
  "sparkVersion": "4.0.0",
  "port": "8080",
  "endpoints": {
    "health": "GET /health",
    "execute": "POST /sql",
    "info": "GET /info"
  }
}
```

### Execute SQL
```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as id, '\''Alice'\'' as name"}'
```

Response:
```json
{
  "success": true,
  "result": "id | name\n-----------\n1 | Alice\n\nTotal rows: 1",
  "error": null
}
```

## Python API Reference

### SparkConfig

```python
from spark_shell import SparkConfig

spark_config = SparkConfig(configs={
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "10"
})
```

### OpConfig

```python
from spark_shell import OpConfig

op_config = OpConfig(
    verbose=True,           # Print detailed logs
    auto_start=True,        # Auto-start in context manager
    cleanup_on_exit=False,  # Don't cleanup temp files on exit
    startup_timeout=60      # Server startup timeout in seconds
)
```

### SparkShell

```python
from spark_shell import SparkShell

shell = SparkShell(
    source=".",                    # Path to sparkshell directory
    port=8080,                     # Server port
    spark_config=spark_config,     # Optional SparkConfig
    op_config=op_config            # Optional OpConfig
)

# Context manager (recommended)
with SparkShell(source=".") as shell:
    result = shell.execute_sql("SELECT 1")
    print(result)

# Manual lifecycle
shell.start()
result = shell.execute_sql("SELECT 1")
shell.shutdown()

# Check health
is_healthy = shell.is_healthy()

# Get server info
info = shell.get_server_info()
print(f"Spark Version: {info['sparkVersion']}")
```

## Requirements

- **Java 17** - Required for Spark 4.0
- **Python 3.7+** - For the Python wrapper
- **Scala 2.13** - Included in SBT build
- **Apache Spark 4.0** - Included as dependency

## Notes

- The server runs Spark in local mode (`local[*]`)
- Log level is set to WARN to reduce noise
- The server gracefully shuts down Spark when terminated
- Warehouse directory: `/tmp/spark-warehouse`
- Supports CORS for cross-origin requests

## License

This project is a pure wrapper around Apache Spark SQL for educational and development purposes.
