import os
import numpy as np
import pandas as pd

rng = np.random.default_rng(42)

PROCESSED_DIR = "data/processed"

SYSTEM_IDS = [4, 10, 33, 34, 35, 50, 51, 1199, 1200, 1201, 1202, 1203, 1208, 1239]

DC_CAPACITY = {
    4: 1.0, 10: 1.12, 33: 2.4, 34: 146.64, 35: 121.68,
    50: 6.0, 51: 6.0, 1199: 52.92, 1200: 51.84, 1201: 140.14,
    1202: 51.84, 1203: 197.47, 1208: 2.71, 1239: 20.16,
}


def generate_contracts() -> pd.DataFrame:
    rows = []
    for sid in SYSTEM_IDS:
        cap = DC_CAPACITY[sid]
        large = cap > 50
        rows.append({
            "contract_id": f"CONTRACT-{sid}",
            "system_id": sid,
            "effective_date": pd.Timestamp("2015-01-01"),
            "expiry_date": pd.Timestamp("2025-12-31"),
            "uptime_sla_pct": round(rng.uniform(0.95, 0.99), 2),
            "min_monthly_kwh": cap * 80,
            "response_time_hrs": 4 if large else 8,
            "resolution_time_hrs": 24 if large else 48,
        })
    return pd.DataFrame(rows)


def generate_incidents(n: int = 500) -> pd.DataFrame:
    capacities = np.array([DC_CAPACITY[s] for s in SYSTEM_IDS], dtype=float)
    weights = capacities / capacities.sum()

    system_ids = rng.choice(SYSTEM_IDS, size=n, p=weights)

    epoch_start = pd.Timestamp("2015-01-01").value // 10**9
    epoch_end = pd.Timestamp("2020-12-31").value // 10**9
    created_ts = rng.integers(epoch_start, epoch_end, size=n)
    created_at = pd.to_datetime(created_ts, unit="s")

    # Response delays: base within SLA, ~20% breach doubles the SLA
    response_base = np.array(
        [4 if DC_CAPACITY[s] > 50 else 8 for s in system_ids], dtype=float
    )
    response_breach = rng.random(n) < 0.20
    response_multiplier = rng.uniform(0.1, 1.0, size=n)
    response_hours = np.where(
        response_breach,
        response_base * rng.uniform(1.01, 3.0, size=n),
        response_base * response_multiplier,
    )
    responded_at = created_at + pd.to_timedelta(response_hours, unit="h")

    # Resolution delays: base within SLA, ~15% breach
    resolution_base = np.array(
        [24 if DC_CAPACITY[s] > 50 else 48 for s in system_ids], dtype=float
    )
    resolution_breach = rng.random(n) < 0.15
    resolution_multiplier = rng.uniform(0.1, 1.0, size=n)
    resolution_hours = np.where(
        resolution_breach,
        resolution_base * rng.uniform(1.01, 4.0, size=n),
        resolution_base * resolution_multiplier,
    )
    resolved_at = responded_at + pd.to_timedelta(resolution_hours, unit="h")

    severity = rng.choice(["P1", "P2", "P3"], size=n, p=[0.10, 0.40, 0.50])
    categories = ["inverter_fault", "communication_loss", "degraded_output", "sensor_error", "grid_fault"]
    category = rng.choice(categories, size=n)

    return pd.DataFrame({
        "ticket_id": [f"TKT-{i:05d}" for i in range(1, n + 1)],
        "system_id": system_ids,
        "created_at": created_at,
        "responded_at": responded_at,
        "resolved_at": resolved_at,
        "severity": severity,
        "category": category,
        "status": "resolved",
    })


def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    contracts = generate_contracts()
    contracts_path = os.path.join(PROCESSED_DIR, "contracts.parquet")
    contracts.to_parquet(contracts_path, index=False)
    print(f"Written {len(contracts)} contracts to {contracts_path}")

    incidents = generate_incidents(500)
    incidents_path = os.path.join(PROCESSED_DIR, "incidents.parquet")
    incidents.to_parquet(incidents_path, index=False)
    print(f"Written {len(incidents)} incidents to {incidents_path}")


if __name__ == "__main__":
    main()
