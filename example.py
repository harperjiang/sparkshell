#!/usr/bin/env python3
"""
Example usage of SparkShell wrapper.

Before running:
1. Build the project: build/sbt assembly
2. Install requirements: pip install -r requirements.txt
"""

from spark_shell import SparkShell, SparkConfig, OpConfig


def basic_example():
    """Basic usage example."""
    print("=" * 60)
    print("Basic Example: Simple Query")
    print("=" * 60)
    
    with SparkShell(source=".") as shell:
        # Simple query
        result = shell.execute_sql("SELECT 1 as id, 'Alice' as name, 30 as age")
        print(result)
        print()


def table_operations_example():
    """Example with table creation and queries."""
    print("=" * 60)
    print("Table Operations Example")
    print("=" * 60)
    
    with SparkShell(source=".", port=8080) as shell:
        # Create table
        print("Creating table...")
        shell.execute_sql("CREATE TABLE users (id INT, name STRING, age INT)")
        
        # Insert data
        print("Inserting data...")
        shell.execute_sql("""
            INSERT INTO users VALUES 
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28)
        """)
        
        # Query all
        print("\nAll users:")
        result = shell.execute_sql("SELECT * FROM users ORDER BY id")
        print(result)
        
        # Query with filter
        print("\nUsers over 28:")
        result = shell.execute_sql("SELECT * FROM users WHERE age > 28 ORDER BY age")
        print(result)
        
        # Aggregation
        print("\nAverage age:")
        result = shell.execute_sql("SELECT AVG(age) as avg_age FROM users")
        print(result)
        
        # Drop table
        print("\nCleaning up...")
        shell.execute_sql("DROP TABLE users")
        print()


def custom_config_example():
    """Example with custom Spark configuration."""
    print("=" * 60)
    print("Custom Configuration Example")
    print("=" * 60)
    
    # Configure Spark settings
    spark_config = SparkConfig(configs={
        "spark.executor.memory": "1g",
        "spark.driver.memory": "1g",
        "spark.sql.shuffle.partitions": "4"
    })
    
    # Configure operational settings
    op_config = OpConfig(
        verbose=True,
        startup_timeout=60
    )
    
    with SparkShell(source=".", port=8081, 
                    spark_config=spark_config, 
                    op_config=op_config) as shell:
        
        # Get server info
        info = shell.get_server_info()
        print(f"\nServer Info:")
        print(f"  Spark Version: {info['sparkVersion']}")
        print(f"  Port: {info['port']}")
        
        # Run query
        result = shell.execute_sql("""
            SELECT 
                'configured' as status,
                current_timestamp() as timestamp
        """)
        print(f"\nQuery Result:")
        print(result)
        print()


def error_handling_example():
    """Example with error handling."""
    print("=" * 60)
    print("Error Handling Example")
    print("=" * 60)
    
    with SparkShell(source=".", port=8082) as shell:
        # Valid query
        try:
            result = shell.execute_sql("SELECT 1 as valid_query")
            print("Valid query succeeded:")
            print(result)
            print()
        except RuntimeError as e:
            print(f"Error: {e}")
        
        # Invalid query (should fail)
        try:
            result = shell.execute_sql("SELECT * FROM nonexistent_table")
            print(result)
        except RuntimeError as e:
            print(f"Expected error for invalid query: {e}")
            print()


if __name__ == "__main__":
    # Run all examples
    basic_example()
    table_operations_example()
    custom_config_example()
    error_handling_example()
    
    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)
