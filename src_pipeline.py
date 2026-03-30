import argparse
from pathlib import Path

import pandas as pd

try:
    from tableauhyperapi import (
        HyperProcess,
        Connection,
        Telemetry,
        CreateMode,
        SqlType,
        TableDefinition,
        Inserter,
    )
except Exception:  # allows CSV-only runs if Hyper API isn't installed
    HyperProcess = None


def read_inputs(input_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    finance_name = "data_sample_revenue_cost_sample.csv"
    invest_name = "data_sample_investments_sample.csv"

    # Support either a provided input folder or CSVs placed at the workspace root.
    finance_candidates = [input_dir / finance_name, Path(finance_name)]
    invest_candidates = [input_dir / invest_name, Path(invest_name)]

    finance_path = next((p for p in finance_candidates if p.exists()), finance_candidates[0])
    invest_path = next((p for p in invest_candidates if p.exists()), invest_candidates[0])

    finance = pd.read_csv(finance_path, parse_dates=["week_end_date"])
    investments = pd.read_csv(invest_path, parse_dates=["week_end_date"])
    return finance, investments


def build_finance_mart(finance: pd.DataFrame) -> pd.DataFrame:
    df = finance.copy()
    df["gross_profit"] = df["revenue"] - df["cogs"]
    df["net_contribution"] = df["revenue"] - df["cogs"] - df["opex"]
    df["gross_margin_pct"] = df["gross_profit"] / df["revenue"].where(df["revenue"] != 0, pd.NA)
    return df


def build_investments_mart(inv: pd.DataFrame) -> pd.DataFrame:
    df = inv.copy()
    df["unrealized_pnl"] = df["ending_mv"] - df["cost_basis"]
    denom = (df["starting_mv"] + df["net_contributions"]).where(
        (df["starting_mv"] + df["net_contributions"]) != 0, pd.NA
    )
    df["total_return_pct"] = (df["ending_mv"] - df["starting_mv"] - df["net_contributions"]) / denom
    return df


def write_csvs(out_dir: Path, finance_mart: pd.DataFrame, invest_mart: pd.DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    finance_mart.to_csv(out_dir / "mart_finance_weekly.csv", index=False)
    invest_mart.to_csv(out_dir / "mart_investments_weekly.csv", index=False)


def write_hyper(out_dir: Path, finance_mart: pd.DataFrame, invest_mart: pd.DataFrame) -> Path | None:
    if HyperProcess is None:
        return None

    out_path = out_dir / "finance_marts.hyper"

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=str(out_path), create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_schema("Extract")

            finance_table = TableDefinition(
                table_name=("Extract", "mart_finance_weekly"),
                columns=[
                    TableDefinition.Column("week_end_date", SqlType.date()),
                    TableDefinition.Column("cost_center", SqlType.text()),
                    TableDefinition.Column("region", SqlType.text()),
                    TableDefinition.Column("product", SqlType.text()),
                    TableDefinition.Column("revenue", SqlType.double()),
                    TableDefinition.Column("cogs", SqlType.double()),
                    TableDefinition.Column("opex", SqlType.double()),
                    TableDefinition.Column("gross_profit", SqlType.double()),
                    TableDefinition.Column("net_contribution", SqlType.double()),
                    TableDefinition.Column("gross_margin_pct", SqlType.double()),
                ],
            )

            invest_table = TableDefinition(
                table_name=("Extract", "mart_investments_weekly"),
                columns=[
                    TableDefinition.Column("week_end_date", SqlType.date()),
                    TableDefinition.Column("portfolio", SqlType.text()),
                    TableDefinition.Column("asset_class", SqlType.text()),
                    TableDefinition.Column("instrument", SqlType.text()),
                    TableDefinition.Column("starting_mv", SqlType.double()),
                    TableDefinition.Column("ending_mv", SqlType.double()),
                    TableDefinition.Column("net_contributions", SqlType.double()),
                    TableDefinition.Column("cost_basis", SqlType.double()),
                    TableDefinition.Column("unrealized_pnl", SqlType.double()),
                    TableDefinition.Column("total_return_pct", SqlType.double()),
                ],
            )

            conn.catalog.create_table(finance_table)
            conn.catalog.create_table(invest_table)

            finance_rows = [
                (
                    r.week_end_date.date(),
                    str(r.cost_center),
                    str(r.region),
                    str(r.product),
                    float(r.revenue),
                    float(r.cogs),
                    float(r.opex),
                    float(r.gross_profit),
                    float(r.net_contribution),
                    float(r.gross_margin_pct) if pd.notna(r.gross_margin_pct) else None,
                )
                for r in finance_mart.itertuples(index=False)
            ]

            invest_rows = [
                (
                    r.week_end_date.date(),
                    str(r.portfolio),
                    str(r.asset_class),
                    str(r.instrument),
                    float(r.starting_mv),
                    float(r.ending_mv),
                    float(r.net_contributions),
                    float(r.cost_basis),
                    float(r.unrealized_pnl),
                    float(r.total_return_pct) if pd.notna(r.total_return_pct) else None,
                )
                for r in invest_mart.itertuples(index=False)
            ]

            with Inserter(conn, finance_table) as ins:
                ins.add_rows(finance_rows)
                ins.execute()

            with Inserter(conn, invest_table) as ins:
                ins.add_rows(invest_rows)
                ins.execute()

    return out_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=".", help="Input folder containing CSVs")
    parser.add_argument("--out", type=str, default="data/curated", help="Output folder for curated marts")
    args = parser.parse_args()

    input_dir = Path(args.input)
    out_dir = Path(args.out)

    finance, investments = read_inputs(input_dir)
    finance_mart = build_finance_mart(finance)
    invest_mart = build_investments_mart(investments)

    write_csvs(out_dir, finance_mart, invest_mart)
    hyper_path = write_hyper(out_dir, finance_mart, invest_mart)

    print(f"Wrote curated CSVs to: {out_dir}")
    if hyper_path:
        print(f"Wrote Tableau Hyper extract to: {hyper_path}")
    else:
        print("Hyper extract not generated (tableauhyperapi not installed).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())