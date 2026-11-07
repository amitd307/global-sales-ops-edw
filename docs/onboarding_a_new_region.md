# Onboarding a new region

Example: adding a **UK** sales team.

1. **Get a sample extract** and list its columns, date format, delimiter, decimal separator and currency.
2. **Write the mapping** in `configs/regions/uk.json`:

```json
{
  "region": "UK",
  "file": "uk_sales.csv",
  "delimiter": ",",
  "decimal_comma": false,
  "currency": "GBP",
  "date_format": "dd/MM/yyyy",
  "columns": {
    "order_id": "OrderRef",
    "order_date": "OrderDate",
    "store_code": "Branch",
    "product_code": "SKU",
    "quantity": "Qty",
    "amount_local": "NetGBP",
    "fulfilment_days": "LeadTime"
  }
}
```

3. **Add GBP** to `fx_rates.csv` and the UK stores to `stores.csv`.
4. **Run the build.** The regional reconciliation must show matching rows and local-currency totals before anything is published.
5. **Validate with the business:** compare one month of UK sales in the EDW with the team's own report, and resolve differences (returns, tax, cancelled orders) in the mapping, not in reports.

No Python changes are needed. A missing required column fails fast with a clear error from `load_configs`.
