"""Config-driven regional ingestion into one common staging model.

Onboarding a new region = adding a JSON file to configs/regions/ that says
which source column means what, plus its date format, delimiter and currency.
No code changes.
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

COMMON = ["region", "order_id", "order_date", "store_code", "product_code", "quantity",
          "amount_local", "currency", "fulfilment_days"]
REQUIRED = {"order_id", "order_date", "store_code", "product_code", "quantity", "amount_local"}


def load_configs(folder: str) -> list[dict]:
    configs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(Path(folder).glob("*.json"))]
    for c in configs:
        missing = REQUIRED - set(c["columns"])
        if missing:
            raise ValueError(f"region {c['region']}: mapping missing {sorted(missing)}")
    return configs


def read_source(spark: SparkSession, raw_dir: str, cfg: dict) -> DataFrame:
    # read everything as string: parsing is explicit and driven by the config
    return (spark.read.option("header", True).option("sep", cfg["delimiter"])
            .csv(f"{raw_dir}/{cfg['file']}"))


def to_common(src: DataFrame, cfg: dict) -> DataFrame:
    m = cfg["columns"]
    amount = F.col(m["amount_local"])
    if cfg.get("decimal_comma"):
        amount = F.regexp_replace(F.regexp_replace(amount, r"\.", ""), ",", ".")
    fd = F.col(m["fulfilment_days"]).cast("int") if "fulfilment_days" in m else F.lit(None).cast("int")
    return src.select(
        F.lit(cfg["region"]).alias("region"),
        F.concat(F.lit(cfg["region"] + ":"), F.col(m["order_id"])).alias("order_id"),
        F.to_date(F.col(m["order_date"]), cfg["date_format"]).alias("order_date"),
        F.col(m["store_code"]).alias("store_code"),
        F.col(m["product_code"]).alias("product_code"),
        F.col(m["quantity"]).cast("int").alias("quantity"),
        amount.cast("decimal(18,2)").alias("amount_local"),
        F.lit(cfg["currency"]).alias("currency"),
        fd.alias("fulfilment_days"),
    )


def stage_all(spark: SparkSession, raw_dir: str, configs: list[dict]) -> tuple[DataFrame, dict]:
    """Returns the unioned staging table and per-region source stats for reconciliation."""
    staged, stats = None, {}
    for cfg in configs:
        src = read_source(spark, raw_dir, cfg)
        amt = F.col(cfg["columns"]["amount_local"])
        if cfg.get("decimal_comma"):
            amt = F.regexp_replace(F.regexp_replace(amt, r"\.", ""), ",", ".")
        row = src.agg(F.count("*").alias("n"), F.sum(amt.cast("double")).alias("amt")).collect()[0]
        stats[cfg["region"]] = (row.n, float(row.amt or 0))
        df = to_common(src, cfg).select(COMMON)
        staged = df if staged is None else staged.unionByName(df)
    return staged, stats
