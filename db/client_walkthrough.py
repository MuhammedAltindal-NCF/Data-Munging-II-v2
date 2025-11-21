"""
Simple DuckDB Client for Remote Access
"""

from duckdb_client import DuckDBClient

BASE_URL = "https://launched-pavilion-msie-repairs.trycloudflare.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ"
# token-7tP9tNX9cJ_KeQmF1tNnSA
# token-I5WbqPySFXbAnjCZPqzNsw
# token-bxbzJjnUW6-g1PrQnTylsw

if __name__ == "__main__":
    # Initialize client with configured values
    client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)

    print("=" * 60)
    print("DuckDB Remote Database Client")
    print("=" * 60)
    print()

    # Test 1: Health check
    print("1. Health Check")
    print("-" * 60)
    health = client.health_check()
    print(f"Status: {health}")
    print()

    # Test 2: Database info
    print("2. Database Info")
    print("-" * 60)
    info = client.get_info()
    print(f"Total tables: {info['table_count']}")
    print("\nAvailable tables:")
    for table in info['tables']:
        print(f"  - {table['table']:20s} {table['rows']:>10,} rows")
    print()

    # Test 3: List tables
    print("3. Available Tables (DataFrame)")
    print("-" * 60)
    tables = client.list_tables()
    print(tables)
    print()

    # Test 4: Query data summary
    print("4. Data Summary by Zone and Year")
    print("-" * 60)
    summary = client.query("SELECT * FROM data_summary ORDER BY zone, year")
    print(summary)
    print()

    # Test 5: Query hourly data for a specific zone with complete data
    print("5. Sample Hourly Data for Florida (US-FLA-FPL) - Complete Data!")
    print("-" * 60)
    fl_data = client.query("""
        SELECT
            datetime_utc,
            zone,
            carbon_direct,
            carbon_lifecycle,
            cfe_pct,
            re_pct
        FROM hourly_data
        WHERE zone = 'US-FLA-FPL' AND year = 2024
        LIMIT 10
    """)
    print(fl_data)
    print()

    # Test 6: Aggregation query with complete data
    print("6. Average Carbon Metrics by Zone (2024)")
    print("-" * 60)
    avg_carbon = client.query("""
        SELECT
            zone,
            COUNT(*) as record_count,
            ROUND(AVG(carbon_direct), 2) as avg_carbon_direct,
            ROUND(AVG(carbon_lifecycle), 2) as avg_carbon_lifecycle,
            ROUND(AVG(cfe_pct), 2) as avg_cfe_pct,
            ROUND(AVG(re_pct), 2) as avg_re_pct
        FROM hourly_data
        WHERE year = 2024
        GROUP BY zone
        ORDER BY zone
    """)
    print(avg_carbon)
    print()

    # Test 7: Compare zones with complete vs partial data
    print("7. Data Completeness Comparison")
    print("-" * 60)
    print("Zones with COMPLETE data (all columns):")
    complete = client.query("""
        SELECT zone, COUNT(*) as records
        FROM hourly_data
        WHERE cfe_pct IS NOT NULL
        GROUP BY zone
        ORDER BY zone
    """)
    print(complete)
    print()

    print("Zones with PARTIAL data (carbon_direct only):")
    partial = client.query("""
        SELECT zone, COUNT(*) as records
        FROM hourly_data
        WHERE cfe_pct IS NULL
        GROUP BY zone
        ORDER BY zone
    """)
    print(partial)
    print()

    print("=" * 60)
    print("✅ All tests completed successfully!")
    print("=" * 60)
