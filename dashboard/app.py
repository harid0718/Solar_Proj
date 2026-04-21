import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Solar Fleet Compliance",
    page_icon="☀️",
    layout="wide",
)

COMPLIANCE_PATH = "data/processed/compliance_report.parquet"
INCIDENTS_PATH  = "data/processed/incidents.parquet"

STATUS_COLORS = {"COMPLIANT": "#2ecc71", "AT_RISK": "#f39c12", "BREACH": "#e74c3c"}
SEVERITY_COLORS = {"P1": "#e74c3c", "P2": "#f39c12", "P3": "#f1c40f"}


# ── data loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_compliance() -> pd.DataFrame:
    df = pd.read_parquet(COMPLIANCE_PATH)
    df["period"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


@st.cache_data
def load_incidents() -> pd.DataFrame:
    df = pd.read_parquet(INCIDENTS_PATH)
    df["created_at"]   = pd.to_datetime(df["created_at"])
    df["responded_at"] = pd.to_datetime(df["responded_at"])
    df["resolved_at"]  = pd.to_datetime(df["resolved_at"])
    df["response_hrs"]   = (df["responded_at"] - df["created_at"]).dt.total_seconds() / 3600
    df["resolution_hrs"] = (df["resolved_at"]  - df["created_at"]).dt.total_seconds() / 3600
    df["year"]  = df["created_at"].dt.year
    df["month"] = df["created_at"].dt.month
    return df


try:
    df_c = load_compliance()
    df_i = load_incidents()
except FileNotFoundError as e:
    st.error(f"Data not found: {e}\n\nRun the pipeline scripts first.")
    st.stop()

# latest month snapshot per system (for fleet overview table)
latest = (
    df_c.sort_values("period")
    .groupby("system_id", as_index=False)
    .last()
)

# ── tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Fleet Overview",
    "Compliance Trends",
    "Site Drilldown",
    "Incident Board",
    "Risk Heatmap",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Fleet Overview
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.header("Fleet Overview")

    total_systems   = latest["system_id"].nunique()
    pct_compliant   = (latest["overall_status"] == "COMPLIANT").mean() * 100
    active_breaches = (latest["overall_status"] == "BREACH").sum()
    avg_risk        = latest["fleet_risk_score"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Systems",        f"{total_systems}")
    c2.metric("Fleet Compliant",      f"{pct_compliant:.1f}%")
    c3.metric("Active Breaches",      f"{active_breaches}")
    c4.metric("Avg Fleet Risk Score", f"{avg_risk:.1f}")

    st.divider()

    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("Status Breakdown")
        counts = (
            latest["overall_status"]
            .value_counts()
            .reindex(["COMPLIANT", "AT_RISK", "BREACH"], fill_value=0)
            .reset_index()
        )
        counts.columns = ["Status", "Count"]
        fig_bar = px.bar(
            counts, x="Status", y="Count",
            color="Status",
            color_discrete_map=STATUS_COLORS,
            text="Count",
        )
        fig_bar.update_traces(textposition="outside")
        fig_bar.update_layout(showlegend=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        st.subheader("System Status (latest month)")
        tbl = (
            latest[["system_name", "site_location", "overall_status", "fleet_risk_score"]]
            .sort_values("fleet_risk_score")
            .rename(columns={
                "system_name":    "System",
                "site_location":  "Location",
                "overall_status": "Status",
                "fleet_risk_score": "Risk Score",
            })
        )
        st.dataframe(
            tbl.style.apply(
                lambda col: col.map(lambda v: f"color: {STATUS_COLORS.get(v, 'inherit')}")
                if col.name == "Status" else col.map(lambda _: ""),
                axis=0,
            ),
            use_container_width=True,
            hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Compliance Trends
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.header("Compliance Trends")

    min_date = df_c["period"].min().date()
    max_date = df_c["period"].max().date()

    with st.sidebar:
        st.markdown("### Date Range")
        date_from, date_to = st.date_input(
            "Filter period",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="trend_dates",
        )

    mask = (df_c["period"].dt.date >= date_from) & (df_c["period"].dt.date <= date_to)
    df_filtered = df_c[mask]

    monthly = (
        df_filtered.groupby("period")
        .agg(
            overall_compliant=("overall_status",  lambda s: (s == "COMPLIANT").mean()),
            uptime_compliant= ("uptime_status",   lambda s: (s == "COMPLIANT").mean()),
            energy_compliant= ("energy_status",   lambda s: (s == "COMPLIANT").mean()),
        )
        .reset_index()
        .melt(id_vars="period", var_name="Metric", value_name="Compliance Rate")
    )
    monthly["Compliance Rate"] *= 100
    monthly["Metric"] = monthly["Metric"].map({
        "overall_compliant": "Overall",
        "uptime_compliant":  "Uptime",
        "energy_compliant":  "Energy",
    })

    fig_trend = px.line(
        monthly, x="period", y="Compliance Rate",
        color="Metric",
        markers=True,
        labels={"period": "Month", "Compliance Rate": "% Compliant"},
        color_discrete_map={"Overall": "#3498db", "Uptime": "#9b59b6", "Energy": "#e67e22"},
    )
    fig_trend.update_layout(yaxis_range=[0, 105], margin=dict(t=20))
    st.plotly_chart(fig_trend, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Site Drilldown
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.header("Site Drilldown")

    system_options = (
        df_c[["system_id", "system_name"]]
        .drop_duplicates()
        .sort_values("system_name")
    )
    name_to_id = dict(zip(system_options["system_name"], system_options["system_id"]))

    selected_name = st.selectbox("Select system", list(name_to_id.keys()))
    selected_id   = name_to_id[selected_name]

    sys_row = df_c[df_c["system_id"] == selected_id].iloc[0]

    i1, i2, i3, i4, i5 = st.columns(5)
    i1.metric("Location",         sys_row["site_location"])
    i2.metric("DC Capacity (kW)", f"{sys_row['dc_capacity_kW']:.2f}")
    i3.metric("Uptime SLA",       f"{sys_row['uptime_sla_pct']*100:.0f}%")
    i4.metric("Response SLA",     f"{sys_row['response_time_hrs']:.0f} hrs")
    i5.metric("Resolution SLA",   f"{sys_row['resolution_time_hrs']:.0f} hrs")

    st.divider()
    sys_df = df_c[df_c["system_id"] == selected_id].sort_values("period")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Uptime vs SLA")
        fig_up = go.Figure()
        fig_up.add_trace(go.Scatter(
            x=sys_df["period"], y=sys_df["uptime_pct"] * 100,
            name="Actual Uptime %", mode="lines+markers", line=dict(color="#3498db"),
        ))
        fig_up.add_trace(go.Scatter(
            x=sys_df["period"], y=sys_df["uptime_sla_pct"] * 100,
            name="SLA Threshold", mode="lines",
            line=dict(color="#e74c3c", dash="dash"),
        ))
        fig_up.update_layout(yaxis_title="Uptime %", margin=dict(t=20))
        st.plotly_chart(fig_up, use_container_width=True)

    with col_b:
        st.subheader("Energy: Actual vs Expected")
        fig_en = go.Figure()
        fig_en.add_trace(go.Scatter(
            x=sys_df["period"], y=sys_df["energy_actual_kwh"],
            name="Actual kWh", mode="lines+markers", line=dict(color="#2ecc71"),
        ))
        fig_en.add_trace(go.Scatter(
            x=sys_df["period"], y=sys_df["energy_expected_kwh"],
            name="Expected kWh", mode="lines",
            line=dict(color="#e74c3c", dash="dash"),
        ))
        fig_en.update_layout(yaxis_title="kWh", margin=dict(t=20))
        st.plotly_chart(fig_en, use_container_width=True)

    st.subheader("Incident History")
    sys_incidents = df_i[df_i["system_id"] == selected_id].copy()
    if sys_incidents.empty:
        st.info("No incidents recorded for this system.")
    else:
        sys_incidents = sys_incidents.sort_values("created_at", ascending=False)
        st.dataframe(
            sys_incidents[[
                "ticket_id", "created_at", "severity", "category",
                "status", "response_hrs", "resolution_hrs",
            ]].rename(columns={
                "ticket_id":      "Ticket",
                "created_at":     "Created",
                "severity":       "Severity",
                "category":       "Category",
                "status":         "Status",
                "response_hrs":   "Response (hrs)",
                "resolution_hrs": "Resolution (hrs)",
            }),
            use_container_width=True,
            hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Incident Board
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.header("Incident Board")

    # sidebar filters
    with st.sidebar:
        st.markdown("### Incident Filters")
        sev_filter = st.multiselect(
            "Severity", ["P1", "P2", "P3"], default=["P1", "P2", "P3"],
            key="inc_sev",
        )
        sys_filter = st.multiselect(
            "System ID", sorted(df_i["system_id"].unique()),
            key="inc_sys",
        )

    df_inc = df_i.copy()
    if sev_filter:
        df_inc = df_inc[df_inc["severity"].isin(sev_filter)]
    if sys_filter:
        df_inc = df_inc[df_inc["system_id"].isin(sys_filter)]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Incidents",      f"{len(df_inc):,}")
    m2.metric("Open Incidents",       f"{(df_inc['status'] == 'open').sum():,}")
    m3.metric("Avg Response (hrs)",   f"{df_inc['response_hrs'].mean():.1f}")
    m4.metric("Avg Resolution (hrs)", f"{df_inc['resolution_hrs'].mean():.1f}")

    st.divider()

    def color_severity(val):
        return f"background-color: {SEVERITY_COLORS.get(val, 'inherit')}; color: #1a1a1a"

    display_inc = (
        df_inc.sort_values("created_at", ascending=False)[[
            "ticket_id", "system_id", "created_at", "severity",
            "category", "status", "response_hrs", "resolution_hrs",
        ]]
        .rename(columns={
            "ticket_id":      "Ticket",
            "system_id":      "System",
            "created_at":     "Created",
            "severity":       "Severity",
            "category":       "Category",
            "status":         "Status",
            "response_hrs":   "Response (hrs)",
            "resolution_hrs": "Resolution (hrs)",
        })
    )
    display_inc["Response (hrs)"]   = display_inc["Response (hrs)"].round(1)
    display_inc["Resolution (hrs)"] = display_inc["Resolution (hrs)"].round(1)

    st.dataframe(
        display_inc.style.applymap(color_severity, subset=["Severity"]),
        use_container_width=True,
        hide_index=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — Risk Heatmap
# ══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.header("Risk Heatmap")
    st.caption("Fleet risk score by system and month — green = healthy, red = at risk")

    pivot = (
        df_c.assign(period_label=df_c["period"].dt.strftime("%Y-%m"))
        .pivot_table(
            index="system_name",
            columns="period_label",
            values="fleet_risk_score",
            aggfunc="mean",
        )
        .sort_index()
    )

    # sort columns chronologically
    pivot = pivot[sorted(pivot.columns)]

    fig_heat = px.imshow(
        pivot,
        color_continuous_scale=[
            [0.0,  "#e74c3c"],
            [0.5,  "#f39c12"],
            [1.0,  "#2ecc71"],
        ],
        zmin=0, zmax=100,
        aspect="auto",
        labels=dict(x="Month", y="System", color="Risk Score"),
    )
    fig_heat.update_layout(
        margin=dict(t=30, l=180),
        coloraxis_colorbar=dict(title="Risk Score"),
        xaxis=dict(tickangle=-45),
    )
    st.plotly_chart(fig_heat, use_container_width=True)
