import pandas as pd

RET = "/mnt/data/kronos_data/processed/nbeats_global_daily.parquet"
FAC = "/mnt/data/kronos_data/interim/fundamentals_factors_v1.parquet"
AUX = "/mnt/data/kronos_data/interim/aux_features_daily.parquet"
OUT = "/mnt/data/kronos_data/processed/tft_daily.parquet"


def main() -> None:
    returns = pd.read_parquet(RET)
    factors = pd.read_parquet(FAC)
    aux = pd.read_parquet(AUX)

    factors = factors.rename(columns={"symbol": "unique_id"})
    factors["ds"] = pd.to_datetime(factors["ds"])

    covariates = [
        "zn_bm",
        "zn_ey",
        "zn_ebit_ev",
        "zn_gm",
        "zn_om",
        "zn_nm",
        "zn_roa",
        "zn_roe",
        "zn_dta",
        "zn_nd_ebitda",
        "zn_ic",
        "zn_accr",
        "zn_cta",
        "zn_rev",
        "zn_log_mktcap",
    ]

    df = returns.merge(
        factors[["unique_id", "ds", *covariates]],
        on=["unique_id", "ds"],
        how="left",
    )
    df = df.merge(aux, on=["unique_id", "ds"], how="left")

    for column in covariates + ["news_count", "tweet_count"]:
        if column in df.columns:
            df[column] = df[column].fillna(0.0)

    df = df.sort_values(["unique_id", "ds"]).reset_index(drop=True)
    df.to_parquet(OUT, index=False)
    print(f"Wrote {OUT} rows={len(df)}")


if __name__ == "__main__":
    main()
