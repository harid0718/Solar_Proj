import os
import duckdb
import pandas as pd

DB_PATH = "db/fleet.duckdb"
OUTPUT_PATH = "data/processed/compliance_report.parquet"


QUERY = """
WITH ticket_metrics AS (
    SELECT
        i.system_id,
        YEAR(i.created_at)  AS year,
        MONTH(i.created_at) AS month,
        COUNT(*)          AS total_incidents,
        COUNT(*) FILTER (WHERE i.status = 'open') AS open_incidents,
        AVG(
            CASE WHEN (responded_at - created_at) > INTERVAL (c.response_time_hrs) HOUR
                 THEN 1.0 ELSE 0.0 END
        ) AS response_breach_rate,
        AVG(
            CASE WHEN (resolved_at - created_at) > INTERVAL (c.resolution_time_hrs) HOUR
                 THEN 1.0 ELSE 0.0 END
        ) AS resolution_breach_rate
    FROM incidents i
    JOIN contracts c ON i.system_id = c.system_id
    GROUP BY i.system_id, YEAR(created_at), MONTH(created_at)
),
enriched AS (
    SELECT
        cs.system_id,
        cs.year,
        cs.month,
        s.system_public_name   AS system_name,
        s.site_location,
        s.dc_capacity_kW,
        cs.uptime_pct,
        cs.uptime_status,
        cs.energy_actual_kwh,
        cs.energy_expected_kwh,
        cs.energy_status,
        cs.overall_status,
        c.uptime_sla_pct,
        c.response_time_hrs,
        c.resolution_time_hrs,
        COALESCE(tm.total_incidents,       0)    AS total_incidents,
        COALESCE(tm.open_incidents,        0)    AS open_incidents,
        COALESCE(tm.response_breach_rate,  0.0)  AS response_breach_rate,
        COALESCE(tm.resolution_breach_rate, 0.0) AS resolution_breach_rate
    FROM compliance_summary cs
    JOIN systems   s  ON cs.system_id = s.system_id
    JOIN contracts c  ON cs.system_id = c.system_id
    LEFT JOIN ticket_metrics tm
           ON cs.system_id = tm.system_id
          AND cs.year       = tm.year
          AND cs.month      = tm.month
)
SELECT
    *,
    GREATEST(0,
        100
        - CASE uptime_status
            WHEN 'BREACH'   THEN 30
            WHEN 'AT_RISK'  THEN 15
            ELSE 0 END
        - CASE energy_status
            WHEN 'BREACH'   THEN 30
            WHEN 'AT_RISK'  THEN 15
            ELSE 0 END
        - CASE WHEN response_breach_rate   > 0.2 THEN 10 ELSE 0 END
        - CASE WHEN resolution_breach_rate > 0.2 THEN 10 ELSE 0 END
    ) AS fleet_risk_score
FROM enriched
ORDER BY system_id, year, month
"""


def print_summary(df: pd.DataFrame) -> None:
    total = len(df)
    print(f"\nTotal rows: {total:,}")

    status_pct = (
        df["overall_status"]
        .value_counts(normalize=True)
        .mul(100)
        .round(1)
        .rename("pct")
    )
    print("\nOverall status breakdown:")
    for status in ["COMPLIANT", "AT_RISK", "BREACH"]:
        pct = status_pct.get(status, 0.0)
        print(f"  {status:<12} {pct:>5.1f}%")

    worst = (
        df.groupby(["system_id", "system_name"])["fleet_risk_score"]
        .mean()
        .reset_index()
        .sort_values("fleet_risk_score")
        .head(5)
    )
    print("\nTop 5 worst systems by avg fleet_risk_score:")
    for _, row in worst.iterrows():
        print(f"  system {int(row['system_id']):>4}  {row['system_name']:<35}  avg score: {row['fleet_risk_score']:.1f}")


def main():
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute(QUERY).df()
    finally:
        con.close()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Written to {OUTPUT_PATH}")

    print_summary(df)


if __name__ == "__main__":
    main()
