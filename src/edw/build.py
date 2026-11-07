"""Build the global EDW from regional extracts.

    PYTHONPATH=src python -m edw.build --raw data/raw --configs configs/regions --out output/edw
"""
import argparse
import logging
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .ingest import load_configs, stage_all
from .model import build_dim_date, build_dim_product, build_fact_sales, convert_currency, scd2_store

log = logging.getLogger("edw")


def reconcile(staged, source_stats: dict) -> bool:
    got = {r.region: (r.n, float(r.amt or 0)) for r in
           staged.groupBy("region").agg(F.count("*").alias("n"), F.sum("amount_local").alias("amt")).collect()}
    ok = True
    for region, (n, amt) in source_stats.items():
        gn, gamt = got.get(region, (0, 0.0))
        good = n == gn and abs(amt - gamt) < 0.01
        ok &= good
        (log.info if good else log.error)("recon %s: rows %d/%d, local amount %.2f/%.2f", region, gn, n, gamt, amt)
    return ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--configs", default="configs/regions")
    p.add_argument("--out", default="output/edw")
    p.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    a = p.parse_args()

    spark = SparkSession.builder.appName("global-edw").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    csv = lambda name: spark.read.option("header", True).csv(f"{a.raw}/{name}")  # noqa: E731

    staged, stats = stage_all(spark, a.raw, load_configs(a.configs))
    staged = staged.cache()
    if not reconcile(staged, stats):
        log.error("regional reconciliation failed, stopping")
        return 1

    dim_store = scd2_store(None, csv("stores.csv"), date(2025, 1, 1))
    dim_store = scd2_store(dim_store, csv("stores_update.csv"), date(2025, 7, 1))
    dim_product = build_dim_product(csv("products.csv"))
    fact = build_fact_sales(convert_currency(staged, csv("fx_rates.csv")), dim_store, dim_product)

    tables = {"dim_date": build_dim_date(spark, "2025-01-01", "2025-12-31"), "dim_store": dim_store,
              "dim_product": dim_product, "fact_sales": fact}
    for name, df in tables.items():
        df.write.mode("overwrite").format(a.format).save(f"{a.out}/{name}")
        log.info("%-12s %6d rows", name, df.count())
    return 0


if __name__ == "__main__":
    sys.exit(main())
