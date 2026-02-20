#!/usr/bin/env python3
"""
SparkShell - Pure Spark SQL REST Server Wrapper

A simple Python wrapper to start a Spark SQL server and execute SQL commands.

Usage:
    from spark_shell import SparkShell, SparkConfig

    # Basic usage
    with SparkShell(source=".", port=8080) as shell:
        result = shell.execute_sql("SELECT 1 as id")
        print(result)

    # With custom Spark configurations
    spark_config = SparkConfig(configs={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.sql.shuffle.partitions": "10"
    })
    
    with SparkShell(source=".", port=8080, spark_config=spark_config) as shell:
        result = shell.execute_sql("SELECT * FROM table")
        print(result)
"""

import os
import sys
import time
import subprocess
import requests
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class SparkConfig:
    """Spark configuration settings."""
    configs: dict = field(default_factory=dict)


@dataclass
class OpConfig:
    """Operational configuration for SparkShell lifecycle."""
    verbose: bool = True
    auto_start: bool = True
    cleanup_on_exit: bool = False
    startup_timeout: int = 60


class SparkShell:
    """
    A simple wrapper to manage Spark SQL REST server lifecycle.
    
    Features:
    - Start/stop the Spark SQL server
    - Execute SQL commands via REST API
    - Context manager support for automatic cleanup
    """
    
    def __init__(
        self,
        source: str,
        port: int = 8080,
        spark_config: Optional[SparkConfig] = None,
        op_config: Optional[OpConfig] = None
    ):
        """
        Initialize SparkShell.

        Args:
            source: Local directory path containing SparkShell code
            port: Port for the server (default: 8080)
            spark_config: Spark configuration (SparkConfig object)
            op_config: Operational configuration (OpConfig object)
        """
        self.source = Path(source).expanduser().resolve()
        self.port = port

        # Initialize configurations
        self.spark_config = spark_config or SparkConfig()
        self.op_config = op_config or OpConfig()
        
        # Verify source directory
        if not self.source.exists():
            raise FileNotFoundError(f"Source directory not found: {self.source}")
        
        # Verify required files
        self.jar_path = self.source / "target" / "scala-2.13" / "sparkshell.jar"
        if not self.jar_path.exists():
            raise FileNotFoundError(
                f"Assembly JAR not found at: {self.jar_path}\n"
                f"Please build the project first: cd {self.source} && build/sbt assembly"
            )

        # Runtime state
        self.process: Optional[subprocess.Popen] = None
        self.is_ready = False
        self.base_url = f"http://localhost:{self.port}"

        if self.op_config.verbose:
            print(f"[SparkShell] Initialized")
            print(f"  Source: {self.source}")
            print(f"  Port: {self.port}")
            print(f"  JAR: {self.jar_path}")

    def __enter__(self):
        """Context manager entry - start server."""
        if self.op_config.auto_start:
            self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup."""
        self.shutdown()
        return False
    
    def start(self):
        """Start the Spark SQL REST server."""
        if self.op_config.verbose:
        print(f"[SparkShell] Starting server on port {self.port}...")

        # Check if port is already in use
        if self._is_port_in_use():
            raise RuntimeError(f"Port {self.port} is already in use")
        
        # Build command with port and Spark configs
        java_home = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
        java_cmd = os.path.join(java_home, "bin", "java")
        cmd = [java_cmd, "-jar", str(self.jar_path), str(self.port)]

        # Add Spark configurations as key=value arguments
        if self.spark_config.configs:
            for key, value in self.spark_config.configs.items():
                cmd.append(f"{key}={value}")
                if self.op_config.verbose:
                print(f"[SparkShell] Setting Spark config: {key}={value}")

        if self.op_config.verbose:
            print(f"[SparkShell] Running: {' '.join(cmd)}")

        # Start the server process
        log_file = self.source / "sparkshell.log"
        
        with open(log_file, "w") as log:
                self.process = subprocess.Popen(
                    cmd,
                cwd=self.source,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid if sys.platform != "win32" else None
                )

        # Wait for server to be ready
        if self.op_config.verbose:
        print("[SparkShell] Waiting for server to start...")

        start_time = time.time()
        while time.time() - start_time < self.op_config.startup_timeout:
            if self._check_health():
                self.is_ready = True
                if self.op_config.verbose:
                print(f"[SparkShell] Server ready at {self.base_url}")
                return

            # Check if process died
            if self.process.poll() is not None:
                with open(log_file) as f:
                    log_contents = f.read()
                raise RuntimeError(f"Server process died. Log:\n{log_contents}")

            time.sleep(1)

        raise RuntimeError(f"Server failed to start within {self.op_config.startup_timeout} seconds")
    
    def _is_port_in_use(self) -> bool:
        """Check if the port is already in use."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def _check_health(self) -> bool:
        """Check if server is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=2)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
    
    def execute_sql(self, sql: str, output_path: Optional[str] = None) -> str:
        """
        Execute SQL command and return result.

        Args:
            sql: SQL command to execute
            output_path: Optional path to write results as Parquet files

        Returns:
            str: Query result as formatted string

        Raises:
            RuntimeError: If server is not ready or SQL execution fails
        """
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")

        try:
            # Build request payload
            payload = {"sql": sql}
            if output_path:
                payload["outputPath"] = output_path
            
            response = requests.post(
                f"{self.base_url}/sql",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=300  # 5 minutes timeout for long queries
            )

            if response.status_code != 200:
                raise RuntimeError(f"HTTP error {response.status_code}: {response.text}")

            data = response.json()

            if not data.get("success", False):
                error_msg = data.get("error", "Unknown error")
                raise RuntimeError(f"SQL execution failed: {error_msg}")

            return data.get("result", "")

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to execute SQL: {str(e)}")
    
    def get_server_info(self) -> dict:
        """Get server information."""
        if not self.is_ready:
            raise RuntimeError("Server is not ready. Call start() first.")
        
        try:
            response = requests.get(f"{self.base_url}/info", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to get server info: {str(e)}")
    
    def is_healthy(self) -> bool:
        """Check if server is healthy."""
        return self._check_health()
    
    def shutdown(self):
        """Shutdown the server gracefully."""
        if self.process is None:
            return
        
        if self.op_config.verbose:
        print("[SparkShell] Shutting down server...")
        
        try:
            # Try graceful shutdown first
            self.process.terminate()
            
            # Wait up to 10 seconds for graceful shutdown
            try:
                self.process.wait(timeout=10)
                if self.op_config.verbose:
                print("[SparkShell] Server shutdown complete")
            except subprocess.TimeoutExpired:
                if self.op_config.verbose:
                print("[SparkShell] Forcing server shutdown...")
                self.process.kill()
                self.process.wait()
                if self.op_config.verbose:
                print("[SparkShell] Server killed")
        except Exception as e:
            if self.op_config.verbose:
            print(f"[SparkShell] Error during shutdown: {e}")
        finally:
            self.process = None
            self.is_ready = False
    
    def __del__(self):
        """Destructor - ensure cleanup."""
        if hasattr(self, 'process') and self.process:
            self.shutdown()
