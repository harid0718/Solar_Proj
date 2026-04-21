import re
import glob
import os
import pandas as pd

RAW_ROOT = "data/raw"
PROCESSED_DIR = "data/processed"


def strip_suffix(col: str) -> str:
    return re.sub(r"__\d+$", "", col)


def collect_files_by_system() -> dict[str, list[str]]:
    pattern = os.path.join(RAW_ROOT, "system_id=*", "year=*", "**", "*.csv")
    files = glob.glob(pattern, recursive=True)
    by_system: dict[str, list[str]] = {}
    for f in sorted(files):
        sys_id = re.search(r"system_id=([^/\\]+)", f).group(1)
        by_system.setdefault(sys_id, []).append(f)
    return by_system


def process_file(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [strip_suffix(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df["measured_on"] = pd.to_datetime(df["measured_on"])
    if "inverter_error_code" in df.columns:
        df["status"] = df["inverter_error_code"].apply(
            lambda x: "offline" if x != 0 else "online"
        )
    else:
        df["status"] = "online"
    return df


def main():
    by_system = collect_files_by_system()
    if not by_system:
        print(f"No CSV files found under {RAW_ROOT}")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    total_systems = len(by_system)

    for i, (sys_id, files) in enumerate(by_system.items(), start=1):
        print(f"[{i}/{total_systems}] Processing system_id={sys_id} ({len(files)} files)...")
        df = pd.concat([process_file(f) for f in files], ignore_index=True)
        out_path = os.path.join(PROCESSED_DIR, f"system_id={sys_id}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  -> {len(df):,} rows written to {out_path}")


if __name__ == "__main__":
    main()
