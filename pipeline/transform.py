import os
import glob
import duckdb

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
DB_PATH = "db/fleet.duckdb"

SYSTEMS_CSV = os.path.join(RAW_DIR, "systems_20250729.csv")
SYSTEMS_COLUMNS = [
    "system_id", "system_public_name", "site_location", "dc_capacity_kW",
    "first_timestamp", "last_timestamp", "years", "qa_status",
]


def load_tables(con: duckdb.DuckDBPyConnection) -> None:
    # systems
    cols = ", ".join(SYSTEMS_COLUMNS)
    con.execute(f"""
        CREATE OR REPLACE TABLE systems AS
        SELECT {cols}
        FROM read_csv_auto('{SYSTEMS_CSV}')
    """)
    print(f"  systems:      {con.execute('SELECT COUNT(*) FROM systems').fetchone()[0]:>8,} rows")

    # contracts
    con.execute(f"""
        CREATE OR REPLACE TABLE contracts AS
        SELECT * FROM read_parquet('{PROCESSED_DIR}/contracts.parquet')
    """)
    print(f"  contracts:    {con.execute('SELECT COUNT(*) FROM contracts').fetchone()[0]:>8,} rows")

    # incidents
    con.execute(f"""
        CREATE OR REPLACE TABLE incidents AS
        SELECT * FROM read_parquet('{PROCESSED_DIR}/incidents.parquet')
    """)
    print(f"  incidents:    {con.execute('SELECT COUNT(*) FROM incidents').fetchone()[0]:>8,} rows")

    # performance — union all 14 per-system parquet files
    parquet_files = sorted(glob.glob(os.path.join(PROCESSED_DIR, "system_id=*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(f"No system_id=*.parquet files found in {PROCESSED_DIR}")
    file_list = ", ".join(f"'{f}'" for f in parquet_files)
    con.execute(f"""
        CREATE OR REPLACE TABLE performance AS
        SELECT * FROM read_parquet([{file_list}], union_by_name=true)
    """)
    print(f"  performance:  {con.execute('SELECT COUNT(*) FROM performance').fetchone()[0]:>8,} rows")


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE OR REPLACE VIEW monthly_performance AS
        SELECT
            system_id,
            YEAR(measured_on)                              AS year,
            MONTH(measured_on)                             AS month,
            SUM(ac_power / 4.0)                            AS total_ac_energy_kwh,
            AVG(poa_irradiance)                            AS avg_irradiance,
            COUNT(*)                                       AS total_intervals,
            COUNT(*) FILTER (WHERE status = 'online')      AS online_intervals,
            COUNT(*) FILTER (WHERE status = 'online')
                / COUNT(*)::DOUBLE                         AS uptime_pct
        FROM performance
        GROUP BY system_id, YEAR(measured_on), MONTH(measured_on)
    """)
    print("  view monthly_performance: created")

    con.execute("""
        CREATE OR REPLACE VIEW compliance_summary AS
        SELECT
            mp.system_id,
            mp.year,
            mp.month,
            mp.uptime_pct,
            CASE
                WHEN mp.uptime_pct < c.uptime_sla_pct              THEN 'BREACH'
                WHEN mp.uptime_pct < c.uptime_sla_pct + 0.02       THEN 'AT_RISK'
                ELSE 'COMPLIANT'
            END                                            AS uptime_status,
            mp.total_ac_energy_kwh                         AS energy_actual_kwh,
            c.min_monthly_kwh                              AS energy_expected_kwh,
            CASE
                WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh            THEN 'BREACH'
                WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh * 1.05     THEN 'AT_RISK'
                ELSE 'COMPLIANT'
            END                                            AS energy_status,
            CASE
                WHEN 'BREACH' IN (
                    CASE WHEN mp.uptime_pct < c.uptime_sla_pct THEN 'BREACH'
                         WHEN mp.uptime_pct < c.uptime_sla_pct + 0.02 THEN 'AT_RISK'
                         ELSE 'COMPLIANT' END,
                    CASE WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh THEN 'BREACH'
                         WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh * 1.05 THEN 'AT_RISK'
                         ELSE 'COMPLIANT' END
                ) THEN 'BREACH'
                WHEN 'AT_RISK' IN (
                    CASE WHEN mp.uptime_pct < c.uptime_sla_pct THEN 'BREACH'
                         WHEN mp.uptime_pct < c.uptime_sla_pct + 0.02 THEN 'AT_RISK'
                         ELSE 'COMPLIANT' END,
                    CASE WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh THEN 'BREACH'
                         WHEN mp.total_ac_energy_kwh < c.min_monthly_kwh * 1.05 THEN 'AT_RISK'
                         ELSE 'COMPLIANT' END
                ) THEN 'AT_RISK'
                ELSE 'COMPLIANT'
            END                                            AS overall_status
        FROM monthly_performance mp
        JOIN contracts c ON mp.system_id = c.system_id
    """)
    print("  view compliance_summary:  created")


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    con = duckdb.connect(DB_PATH)
    try:
        print("Loading tables...")
        load_tables(con)
        print("Creating views...")
        create_views(con)
        print(f"\nDone. Database written to {DB_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
