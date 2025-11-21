"""
DuckDB Client for Remote Access

A client library for interacting with a remote DuckDB database via REST API.
Supports both read operations (queries) and write operations (execute).
"""

import requests
import pandas as pd


class DuckDBClient:
    def __init__(self, base_url: str, token: str):
        """
        Initialize client

        Args:
            base_url: The public URL of the DuckDB server
            token: Your API token
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def health_check(self) -> dict:
        """Check if server is healthy"""
        response = requests.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

    def list_tables(self) -> pd.DataFrame:
        """List all available tables"""
        response = requests.get(
            f"{self.base_url}/tables",
            headers=self.headers
        )
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data["tables"])

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SELECT query (READ operation)

        Args:
            sql: SQL query string

        Returns:
            pandas DataFrame
        """
        response = requests.post(
            f"{self.base_url}/query",
            json={"query": sql},
            headers=self.headers
        )
        response.raise_for_status()
        data = response.json()

        if data['rows'] > 0:
            print(f"Query returned {data['rows']} rows")
            return pd.DataFrame(data["data"])
        else:
            print("Query executed successfully (0 rows)")
            return pd.DataFrame()

    def execute(self, sql: str) -> dict:
        """
        Execute INSERT, UPDATE, DELETE, CREATE, DROP (WRITE operations)

        Args:
            sql: SQL statement to execute

        Returns:
            dict with execution result
        """
        response = requests.post(
            f"{self.base_url}/execute",
            json={"query": sql},
            headers=self.headers
        )
        response.raise_for_status()
        result = response.json()
        print(f"✓ {result['message']}")
        return result

    def get_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get schema for a specific table

        Args:
            table_name: Name of the table

        Returns:
            pandas DataFrame with schema information
        """
        response = requests.get(
            f"{self.base_url}/schema/{table_name}",
            headers=self.headers
        )
        response.raise_for_status()
        data = response.json()

        print(f"Table: {data['table']}")
        print(f"Rows: {data['row_count']:,}")
        print("\nSchema:")
        return pd.DataFrame(data["schema"])

    def get_info(self) -> dict:
        """
        Get overall database information

        Returns:
            dict with database info including table count and table list
        """
        response = requests.get(
            f"{self.base_url}/info",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
