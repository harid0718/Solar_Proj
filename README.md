# Solar Compliance Engine

## Overview

A fleet compliance monitoring system that ingests real solar PV performance data from NREL PVDAQ, evaluates each site against contractual SLAs, cross-references service incidents, and surfaces breaches and risk signals in a live dashboard. Built to simulate the workflows of a commercial energy data analyst role.

---

## Architecture

```
NREL PVDAQ (S3) → ingest.py → data/processed/system_id=*.parquet
                                             ↓
             generate_synthetic.py → contracts.parquet + incidents.parquet
                                             ↓
                                   transform.py → db/fleet.duckdb
                                             ↓
                                   compliance.py → compliance_report.parquet
                                             ↓
                                   dashboard/app.py (Streamlit)
```

---

## Dataset

- **Source:** NREL PVDAQ public dataset via OEDI ([data.openei.org/submissions/4568](https://data.openei.org/submissions/4568))
- **Coverage:** 14 real solar systems across CO, NV, MD, NJ, DE, ME, UT
- **Scale:** ~47 million rows of 15-minute interval performance data (2010–2020)
- **Key columns:** `measured_on`, `ac_power_hw`, `poa_irradiance`, `inverter_error_code`, `status`, `system_id`
- **Synthetic data:** Contracts and incidents generated to simulate real SLA and ticketing workflows

---

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd solar-compliance-engine
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Download NREL data via AWS CLI

The PVDAQ dataset is hosted in a public S3 bucket. Use the following PowerShell loop to download all 14 systems:

```powershell
$systems = @(4, 10, 33, 34, 35, 50, 51, 1199, 1200, 1201, 1202, 1203, 1208, 1239)

foreach ($id in $systems) {
    aws s3 cp `
        s3://oedi-data-lake/pvdaq/pvdaq-4568/system_id=$id/ `
        data/raw/system_id=$id/ `
        --recursive --no-sign-request
}
```

### 4. Run the pipeline

Execute scripts in order from the project root:

```bash
python pipeline/ingest.py            # ingest raw CSVs → per-system parquet
python pipeline/generate_synthetic.py # generate contracts + incidents
python pipeline/transform.py         # load DuckDB, build views
python pipeline/compliance.py        # generate compliance report
```

### 5. Launch the dashboard

```bash
python -m streamlit run dashboard/app.py
```

---

## Compliance Model

### Uptime SLA
Monthly uptime is calculated as the fraction of 15-minute intervals where the site was online. Each system has a contractual threshold between 95–99%. A site-month is flagged **AT_RISK** if uptime falls within 2 percentage points of the threshold, and **BREACH** if it falls below.

### Energy SLA
Monthly energy output (kWh) is compared against a contractual floor of `dc_capacity_kW × 80`. A site-month is **AT_RISK** if actual output is within 5% of the floor, and **BREACH** if below.

### Ticket SLA
Response and resolution times for each incident are compared against per-system contract windows (4 or 8 hours for response; 24 or 48 hours for resolution, based on system capacity). Breach rates are aggregated per system per month.

### Fleet Risk Score
A composite 0–100 score computed per site-month. Deductions are applied for each compliance dimension:

| Condition                        | Deduction |
|----------------------------------|-----------|
| Uptime BREACH                    | −30       |
| Uptime AT_RISK                   | −15       |
| Energy BREACH                    | −30       |
| Energy AT_RISK                   | −15       |
| Response breach rate > 20%       | −10       |
| Resolution breach rate > 20%     | −10       |

Score is clamped to a minimum of 0. Used to rank and prioritize systems needing attention.

---

## Key Findings

- **80.5%** of site-months are fully compliant across the fleet
- **Andre Agassi Preparatory Academy** (Las Vegas, NV) shows recurring uptime breaches — likely driven by high-heat environment degrading inverter performance
- **Distributed Sun - EJ DeSeta** (Wilmington, DE, 197 kW) is the highest-risk site by average fleet risk score
- Energy compliance is near-perfect fleet-wide — uptime is the primary driver of SLA breaches
- NREL research sites (Golden, CO) maintain consistent 100% compliance throughout the monitoring period

---

## Project Structure

```
solar-compliance-engine/
├── dashboard/
│   └── app.py                         # Streamlit dashboard (5 tabs)
├── data/
│   ├── raw/
│   │   ├── system_id=4/               # Raw PVDAQ CSVs partitioned by system/year/month/day
│   │   ├── system_id=10/
│   │   └── ...                        # (14 systems total)
│   └── processed/
│       ├── system_id=4.parquet        # Cleaned per-system performance data
│       ├── system_id=10.parquet
│       ├── ...                        # (14 files)
│       ├── contracts.parquet          # Synthetic SLA contracts
│       ├── incidents.parquet          # Synthetic incident tickets
│       └── compliance_report.parquet  # Final enriched compliance output
├── db/
│   └── fleet.duckdb                   # DuckDB database with tables + views
├── pipeline/
│   ├── ingest.py                      # Raw CSV → parquet ingestion
│   ├── generate_synthetic.py          # Contract + incident data generation
│   ├── transform.py                   # DuckDB load + view creation
│   └── compliance.py                  # Compliance scoring + report export
└── README.md
```

---

## Tech Stack

| Layer        | Technology                          |
|--------------|-------------------------------------|
| Ingestion    | Python, Pandas                      |
| Storage      | Parquet, DuckDB                     |
| Transform    | DuckDB SQL                          |
| Dashboard    | Streamlit, Plotly                   |
| Data source  | AWS CLI (S3), NREL PVDAQ / OEDI     |

---

## What I'd Add with Production Data

- **Real contract data** pulled from CRM/ERP systems to replace synthetic SLAs
- **Live telemetry ingestion** via REST API or MQTT broker for near-real-time monitoring
- **Automated alerting** triggered when sites breach SLA thresholds (email, PagerDuty, Slack)
- **ML-based anomaly detection** for early degradation signals ahead of full SLA breaches
- **Role-based access control** on the dashboard to separate operator, analyst, and executive views
