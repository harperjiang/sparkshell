# Quick Start Guide

## Prerequisites

- Java 17
- Python 3.7+

## Setup (One Time)

1. **Build the project:**
   ```bash
   cd /Users/hao.jiang/sparkshell
   build/sbt assembly
   ```
   This will create `target/scala-2.13/sparkshell.jar`

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage in Python Notebook

```python
from spark_shell import SparkShell, SparkConfig

# Basic usage
with SparkShell(source="/Users/hao.jiang/sparkshell", port=8080) as shell:
    result = shell.execute_sql("SELECT 1 as id, 'test' as name")
    print(result)

# With custom Spark configurations
spark_config = SparkConfig(configs={
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g"
})

with SparkShell(source="/Users/hao.jiang/sparkshell", 
                port=8080, 
                spark_config=spark_config) as shell:
    
    # Create table
    shell.execute_sql("CREATE TABLE test (id INT, name STRING)")
    
    # Insert data
    shell.execute_sql("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
    
    # Query
    result = shell.execute_sql("SELECT * FROM test")
    print(result)
    
    # Cleanup
    shell.execute_sql("DROP TABLE test")
```

## Testing the Setup

Run the example script:
```bash
python example.py
```

This will run several examples demonstrating the functionality.

## Common Issues

### Port Already in Use
If you get "Port already in use" error, either:
- Use a different port: `SparkShell(source=".", port=8081)`
- Stop the existing server: `bin/stop.sh`

### JAR Not Found
If you get "Assembly JAR not found" error:
- Run `build/sbt assembly` first to build the project

### Java Version
Spark 4.0 requires Java 17. Check your version:
```bash
java -version
```

If needed, set JAVA_HOME:
```bash
export JAVA_HOME=/path/to/java-17
```
