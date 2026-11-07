"""Synthetic regional extracts, each in its local "house style".

    python scripts/make_sample_data.py --out data/raw

  india_sales.csv   INR, dd-MM-yyyy, comma-separated
  japan_sales.csv   JPY (no decimals), yyyy/MM/dd, different column names
  italy_sales.csv   EUR with decimal comma, dd/MM/yyyy, semicolon-separated, Italian headers
  stores.csv, stores_update.csv (an Italian store changes format -> SCD2), products.csv,
  fx_rates.csv (monthly USD value of 1 unit of each currency)
"""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

PRODUCTS = [("P-1001", "Hand-loomed Throw", "Textiles"), ("P-1002", "Ceramic Vase", "Decor"),
            ("P-1003", "Brass Table Lamp", "Lighting"), ("P-1004", "Woven Basket", "Decor"),
            ("P-1005", "Linen Cushion Cover", "Textiles"), ("P-1006", "Pendant Light", "Lighting"),
            ("P-1007", "Stoneware Bowl Set", "Tableware"), ("P-1008", "Wall Art Print", "Decor")]
BASE_USD = {"P-1001": 85, "P-1002": 60, "P-1003": 140, "P-1004": 35,
            "P-1005": 25, "P-1006": 190, "P-1007": 70, "P-1008": 110}
REGIONS = {
    "IN": ("India", "INR", ["Bengaluru", "Mumbai", "Delhi"]),
    "JP": ("Japan", "JPY", ["Tokyo", "Osaka"]),
    "IT": ("Italy", "EUR", ["Milan", "Florence"]),
}
USD_PER = {"INR": 0.0120, "JPY": 0.0067, "EUR": 1.08, "USD": 1.0}


def write(path: Path, rows: list[dict], delimiter=","):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter=delimiter)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--orders-per-region", type=int, default=400)
    ap.add_argument("--seed", type=int, default=21)
    a = ap.parse_args()
    rng, out = random.Random(a.seed), Path(a.out)

    stores = []
    for code, (country, _, cities) in REGIONS.items():
        for i, city in enumerate(cities, 1):
            stores.append({"store_code": f"{code}-{i:02d}", "region": code, "country": country,
                           "city": city, "format": rng.choice(["Flagship", "Studio", "Shop-in-shop"])})
    write(out / "stores.csv", stores)
    # mid-year snapshot: the first Italian store is converted to a different format
    upd = [dict(s) for s in stores]
    milan = next(s for s in upd if s["region"] == "IT")
    milan["format"] = "Studio" if milan["format"] == "Flagship" else "Flagship"
    write(out / "stores_update.csv", upd)
    write(out / "products.csv", [{"product_code": c, "product_name": n, "category": cat} for c, n, cat in PRODUCTS])

    months = [date(2025, m, 1) for m in range(1, 13)]
    fx = []
    for m in months:
        for cur, usd in USD_PER.items():
            fx.append({"month": m.strftime("%Y-%m"), "currency": cur,
                       "usd_per_unit": round(usd * rng.uniform(0.97, 1.03), 6) if cur != "USD" else 1.0})
    write(out / "fx_rates.csv", fx)

    def orders(region):
        rs = [s for s in stores if s["region"] == region]
        for o in range(1, a.orders_per_region + 1):
            d = date(2025, 1, 1) + timedelta(days=rng.randint(0, 364))
            pc = rng.choice(PRODUCTS)[0]
            qty = rng.randint(1, 4)
            local = BASE_USD[pc] * qty / USD_PER[REGIONS[region][1]] * rng.uniform(0.9, 1.1)
            yield o, d, rng.choice(rs)["store_code"], pc, qty, local, rng.randint(1, 12)

    write(out / "india_sales.csv", [
        {"invoice_no": f"INV{o:06d}", "invoice_date": d.strftime("%d-%m-%Y"), "store_code": s,
         "item_code": p, "qty": q, "amount_inr": f"{amt:.2f}", "fulfilment_days": fd}
        for o, d, s, p, q, amt, fd in orders("IN")])
    write(out / "japan_sales.csv", [
        {"slip_id": f"JP-{o:07d}", "sale_date": d.strftime("%Y/%m/%d"), "shop_cd": s,
         "product_cd": p, "units": q, "total_jpy": int(round(amt)), "lead_days": fd}
        for o, d, s, p, q, amt, fd in orders("JP")])
    write(out / "italy_sales.csv", [
        {"ordine": f"IT{o:06d}", "data": d.strftime("%d/%m/%Y"), "negozio": s, "articolo": p,
         "quantita": q, "importo_eur": f"{amt:.2f}".replace(".", ","), "giorni_consegna": fd}
        for o, d, s, p, q, amt, fd in orders("IT")], delimiter=";")
    print(f"wrote regional extracts to {out}/")


if __name__ == "__main__":
    main()
