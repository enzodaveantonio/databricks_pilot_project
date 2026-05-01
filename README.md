# 🛒 Ecommerce Data Lakehouse
### Azure Databricks · Delta Lake · PySpark · REST API · Lakeview Dashboard

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Platform](https://img.shields.io/badge/Platform-Azure%20Databricks-orange)
![Format](https://img.shields.io/badge/Format-Delta%20Lake-green)
![Language](https://img.shields.io/badge/Language-PySpark-yellow)

---

## 📌 Overview

An end-to-end data lakehouse built on **Azure Databricks** following the **medallion architecture** (Bronze → Silver → Gold). The pipeline ingests raw dirty ecommerce data across five dimension tables and 90 daily transaction files, applies rigorous data quality checks, converts multi-currency revenues to PHP via a live REST API, and serves a fully denormalized analytical layer to a Lakeview BI dashboard.

> **Special thanks to Dhaval Patel, whose Databricks tutorials provided the foundational architecture this project is built upon.

---

## Architecture

```
Source Data (CSV / REST API)
          │
          ▼
┌─────────────────────┐
│   BRONZE LAYER      │  Raw ingestion — all strings, no type casting
│   brz_*            │  Audit columns: _source_file, ingested_at
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   SILVER LAYER      │  Cleaned, typed, validated, quarantined
│   slv_*_clean      │  Referential integrity enforced
│   slv_*_quarantine │  Bad rows isolated with rejection_reason
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   GOLD LAYER        │  Denormalized, FX-converted, aggregated
│   gld_*            │  Power BI / Dashboard ready
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   DASHBOARD         │  Databricks Lakeview BI
│   Sales Insights   │  KPIs · Trends · Heatmap · Rankings
└─────────────────────┘
```

---

## Project Structure

```
ecommerce-lakehouse/
│
├── 0_data/                          # Raw source CSV files
│   ├── dimensions/
│   │   ├── brands.csv
│   │   ├── categories.csv
│   │   ├── customers.csv
│   │   ├── countries.csv
│   │   ├── date.csv
│   │   └── products.csv
│   └── order_items/                 # 90 daily transaction files
│       ├── order_items_2025-01-01.csv
│       ├── order_items_2025-01-02.csv
│       └── ... (90 files)
│
├── 2_medallion_processing_dim/
│   ├── bronze/
│   │   └── 1_dim_bronze.ipynb
│   ├── gold/
│   │   ├── 1_dim_gold_products.ipynb
│   │   ├── 2_dim_gold_customers.ipynb
│   │   └── 3_dim_gold_date.ipynb
│   └── silver/
│       ├── 1_dim_silver_categories.ipynb
│       ├── 2_dim_silver_brands.ipynb
│       ├── 3_dim_silver_products.ipynb
│       ├── 4_dim_silver_countries.ipynb
│       ├── 5_dim_silver_customers.ipynb
│       └── 6_dim_silver_dates.ipynb
│
├── 3_medallion_processing_fact/
│   ├── 1_fact_bronze.ipynb
│   ├── 2_fact_silver.ipynb
│   └── 3_fact_gold.ipynb
│
├── 4_bi_dashboard/                  # Dashboard config & SQL queries
│   └── sales_insights.lvdash.json  # incl. denorm view as SQL query
│
└── LICENSE
```

---

## Data Model

### Dimensions
| Table | Description | Key Columns |
|---|---|---|
| `dim_brands` | Product brands | `brand_code`, `brand_name`, `category_code` |
| `dim_categories` | Product categories | `category_code`, `category_name` |
| `dim_countries` | Country reference | `country_code`, `country_name`, `region`, `currency_code` |
| `dim_customers` | Customer master | `customer_id`, `country_code`, `state` |
| `dim_dates` | Date dimension | `date_id`, `year`, `quarter`, `week_of_year`, `day_name` |
| `dim_products` | Product catalog | `product_id`, `sku`, `brand_code`, `category_code` |

### Fact
| Table | Description | Grain |
|---|---|---|
| `fact_order_items` | Transaction line items | One row per order line item (`order_id` + `item_seq`) |

### Schema Type
**Snowflake in Silver** → **Star in Gold**

Silver maintains normalized relationships for referential integrity. Gold collapses the snowflake into a flat denormalized view — eliminating joins for BI consumers.

---

## ⚙️ Pipeline Details

### Bronze Layer
- Lands all data **as raw strings** — no type casting, no assumptions
- Adds audit columns: `_source_file`, `ingested_at`
- Uses `mergeSchema = true` to absorb new columns from source
- Never crashes on dirty data — Bronze survives everything

### Silver Layer — Data Quality Framework

Every dimension and fact table follows this pattern:

```
Bronze read
    ↓
Transformation  (type casting, standardization, normalization)
    ↓
Validation      (nulls, format, referential integrity via LEFT ANTI JOIN)
    ↓
Quarantine split
    ├── slv_*_clean      → passes all quality checks
    └── slv_*_quarantine → fails with specific rejection_reason
```

#### Quarantine Pattern
Bad rows are never silently dropped. Every rejected record includes:
- `rejection_reason` — specific column and rule that failed
- `_source_file` — which file the bad row came from
- `ingested_at` — when it was ingested

This is critical for **audit and finance teams** — data issues are traceable, explainable, and correctable without reprocessing the entire pipeline.

#### Referential Integrity Checks
```python
# LEFT ANTI JOIN — finds fact rows with no matching dimension
df_fact.join(df_customers, on="customer_id", how="left_anti")
    .withColumn("rejection_reason", F.lit("customer_id not in slv_customers_clean"))
```

### Gold Layer
- Joins all Silver clean tables into a **flat denormalized view**
- Analysts connect directly — no joins required in Power BI or Databricks
- All monetary columns converted to **PHP** via live FX API
- Derived metrics: `gross_amount`, `discount_amount`, `net_amount`, `final_amount`

---

## Live FX Rate API Integration

Currency conversion uses the **ExchangeRate API** (no API key required for v4 endpoint):

```python
import requests

response = requests.get("https://api.exchangerate-api.com/v4/latest/PHP")
rates = response.json()["rates"]
```

**Features:**
- Fetches live daily rates at pipeline run time
- Fallback to last known rates from `gld_fx_rates` Delta table if API is unavailable
- All rates persisted to Delta with `rate_date`, `rate_source`, `fetched_at` for full auditability
- Converts: `gross_amount`, `discount_amount`, `net_amount`, `tax_amount`, `final_amount` → PHP

---

## ACID Properties & Why They Matter

Delta Lake provides full ACID guarantees — essential for finance, risk, and compliance workloads:

| Property | Guarantee | Finance Relevance |
|---|---|---|
| **Atomicity** | Pipeline fully succeeds or fully fails — no partial writes | Prevents half-loaded GL batches |
| **Consistency** | Schema and constraints always enforced | Type violations caught at write time |
| **Isolation** | Concurrent pipelines don't corrupt each other | Safe parallel dimension loads |
| **Durability** | Committed data survives cluster crashes | Transaction log on persistent storage |

> When pipelines fail mid-run in Excel or raw Parquet, there's no way to know which records were loaded. Delta's transaction log makes every write auditable and recoverable.

---

## Dashboard — Sales Insights

Built on **Databricks Lakeview Dashboard** connected directly to Gold Delta tables.

### KPI Tiles
| Metric | Value |
|---|---|
| Gross Revenue (PHP) | 1.2B |
| Net Revenue (PHP) | 1.07B |
| Total Orders | 492 |
| Total Units Sold | 4.61K |
| Avg Discount % | 10.93% |
| Avg Order Value (PHP) | 3M |

### Visualizations
- 📈 **Daily Gross Revenue** — area chart with full 90-day granularity
- 📉 **Monthly Sales Trend** — month-over-month comparison
- 🌍 **Revenue by Country** — geographic revenue breakdown
- 📦 **Units Sold by Category** — horizontal bar chart
- 🏆 **Top 10 Products by Revenue** — ranked table with quantity and avg price
- ⚡ **Channel Performance** — web vs app: revenue, AOV, units
- 🎟️ **Coupon Performance** — coupon vs non-coupon order comparison
- 🔥 **Sales Heatmap** — day of week × hour of day for net revenue

### Filters
`Country` · `Region` · `State` · `Brand` · `Category` · `Channel` · `Order Date` · `Quarter` · `Coupon Flag`

---

## Tech Stack

| Layer | Technology |
|---|---|
| Platform | Azure Databricks (Free Edition) |
| Storage Format | Delta Lake |
| Language | PySpark (Python) |
| Orchestration | Databricks Workflows |
| FX Data | ExchangeRate API (REST) |
| Dashboard | Databricks Lakeview |
| Version Control | GitHub |

---

---

## 📌 Key Learnings

- **Medallion Architecture** — separation of concerns across Bronze/Silver/Gold enables reliable, maintainable pipelines
- **Quarantine over silent drops** — rejected data is preserved, traceable, and actionable
- **Snowflake → Star** — normalize in Silver for integrity, denormalize in Gold for consumption
- **API integration** — live FX rates with fallback pattern for pipeline resilience
- **ACID at scale** — Delta Lake makes data lakes audit-ready without sacrificing performance

---

## 🔜 Next Steps

- [ ] Incremental ingestion using Databricks Auto Loader
- [ ] MERGE INTO pattern for Silver and Gold fact tables
- [ ] Databricks Workflows with dependency management
- [ ] Personal Health Lakehouse — daily log pipeline with API data sources

---

## 🙏 Acknowledgements

**[Dhaval Patel](https://www.youtube.com/@codebasics)** — Databricks tutorials that provided the foundational architecture and Spark fundamentals this project follows.

---

*Built by Enzo | Finance Data Engineer*
