from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from edw.build import reconcile
from edw.ingest import load_configs, stage_all, to_common
from edw.model import HIGH_DATE, build_dim_product, build_fact_sales, convert_currency, scd2_store


def test_config_maps_italian_source(spark, root):
    cfg = next(c for c in load_configs(str(root / "configs" / "regions")) if c["region"] == "IT")
    src = spark.createDataFrame([("IT000001", "23/06/2025", "IT-01", "P-1002", "3", "1.152,40", "5")],
                                "ordine string, data string, negozio string, articolo string, "
                                "quantita string, importo_eur string, giorni_consegna string")
    r = to_common(src, cfg).collect()[0]
    assert r.order_id == "IT:IT000001"
    assert r.order_date == date(2025, 6, 23)
    assert r.amount_local == Decimal("1152.40")
    assert (r.currency, r.quantity, r.fulfilment_days) == ("EUR", 3, 5)


def test_all_regions_stage_and_reconcile(spark, root):
    staged, stats = stage_all(spark, str(root / "data" / "raw"), load_configs(str(root / "configs" / "regions")))
    assert set(stats) == {"IN", "JP", "IT"}
    assert reconcile(staged, stats)
    assert staged.filter("order_date IS NULL OR amount_local IS NULL OR quantity IS NULL").count() == 0


def test_currency_conversion(spark):
    staged = spark.createDataFrame(
        [("JP", "JP:1", date(2025, 3, 10), "JP-01", "P-1001", 1, Decimal("10000.00"), "JPY", 3)],
        "region string, order_id string, order_date date, store_code string, product_code string, "
        "quantity int, amount_local decimal(18,2), currency string, fulfilment_days int")
    fx = spark.createDataFrame([("2025-03", "JPY", 0.0067), ("2025-03", "INR", 0.012)],
                               "month string, currency string, usd_per_unit double")
    r = convert_currency(staged, fx).collect()[0]
    assert r.amount_usd == Decimal("67.00")
    assert r.amount_inr == Decimal("5583.33")
    assert r.fx_missing is False


def test_scd2_and_fact_store_versioning(spark):
    cols = "store_code string, region string, country string, city string, format string"
    v1 = spark.createDataFrame([("IT-01", "IT", "Italy", "Milan", "Studio"),
                                ("JP-01", "JP", "Japan", "Tokyo", "Flagship")], cols)
    v2 = spark.createDataFrame([("IT-01", "IT", "Italy", "Milan", "Flagship"),
                                ("JP-01", "JP", "Japan", "Tokyo", "Flagship")], cols)
    dim = scd2_store(scd2_store(None, v1, date(2025, 1, 1)), v2, date(2025, 7, 1))
    it = sorted((r for r in dim.collect() if r.store_code == "IT-01"), key=lambda r: r.store_key)
    assert [(r.format, r.is_current) for r in it] == [("Studio", False), ("Flagship", True)]
    assert it[0].valid_to == date(2025, 6, 30) and it[1].valid_to == HIGH_DATE

    prod = build_dim_product(spark.createDataFrame([("P-1001", "Throw", "Textiles")],
                                                   "product_code string, product_name string, category string"))
    conv = spark.createDataFrame(
        [("IT", "IT:1", date(2025, 3, 1), "IT-01", "P-1001", 1, Decimal("10"), "EUR", 2, Decimal("1"), Decimal("1"), False),
         ("IT", "IT:2", date(2025, 9, 1), "IT-01", "P-9999", 1, Decimal("10"), "EUR", 2, Decimal("1"), Decimal("1"), False)],
        "region string, order_id string, order_date date, store_code string, product_code string, quantity int, "
        "amount_local decimal(18,2), currency string, fulfilment_days int, amount_usd decimal(18,2), "
        "amount_inr decimal(18,2), fx_missing boolean")
    fact = {r.order_id: r for r in build_fact_sales(conv, dim, prod).collect()}
    assert fact["IT:1"].store_key == it[0].store_key   # before the format change
    assert fact["IT:2"].store_key == it[1].store_key   # after the format change
    assert fact["IT:2"].product_key == -1              # unknown product kept, not dropped


def test_full_pipeline_has_no_missing_fx(spark, root):
    raw = str(root / "data" / "raw")
    staged, _ = stage_all(spark, raw, load_configs(str(root / "configs" / "regions")))
    fx = spark.read.option("header", True).csv(f"{raw}/fx_rates.csv")
    conv = convert_currency(staged, fx)
    assert conv.filter("fx_missing").count() == 0
    by_region = {r.region: r.usd for r in conv.groupBy("region").agg(F.sum("amount_usd").alias("usd")).collect()}
    # same product mix in every region, so USD totals land in the same ballpark
    assert max(by_region.values()) / min(by_region.values()) < 1.5
