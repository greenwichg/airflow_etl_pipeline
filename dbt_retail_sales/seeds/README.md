# Seeds — Static Reference Data

## What is a dbt Seed?

Seeds are small CSV files that dbt loads into your data warehouse as tables.
Run `dbt seed` to create/refresh the table from the CSV.

## When to Use Seeds

- Small reference/lookup data (< 1000 rows)
- Static mappings that rarely change
- Data that doesn't exist in any source system
- Test fixtures for development

## When NOT to Use Seeds

- Large datasets (use a source table instead)
- Frequently changing data (use a source with freshness checks)
- Sensitive data (seeds are committed to Git!)

## Files

| File | Table Created | Purpose |
|------|--------------|---------|
| `payment_methods.csv` | `payment_methods` | Maps payment codes to display names |

## How to Reference in a Model

```sql
SELECT s.*, p.display_name, p.is_digital
FROM {{ ref('fct_sales') }} s
LEFT JOIN {{ ref('payment_methods') }} p
    ON s.payment_method = p.payment_method
```

## Commands

```bash
dbt seed                           # Load all CSV seeds
dbt seed -s payment_methods        # Load specific seed
dbt seed --full-refresh            # Drop and reload seed tables
```
