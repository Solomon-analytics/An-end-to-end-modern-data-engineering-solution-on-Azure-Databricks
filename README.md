# Project Cascade

**An end-to-end order-to-cash lakehouse on Azure Databricks**

---

Order-to-cash pipeline on Databricks. Monthly files land in a volume, pass through bronze, silver and gold, and end as a star schema for reporting. It runs on a schedule and tracks its own progress.

---

## 1. The business

Kestrel Global Trading is a London-based B2B distributor. Five thousand trade customers across 40 cities and five regions, moving 1,200 SKUs. It holds and ships stock but does not manufacture, and fulfilment runs through five contracted carriers.

Three things about the business show up directly in the data:

Invoices are raised in five currencies. Group reporting is in sterling, so every value converts at the rate for its invoice month.
The ERP was replaced in January 2025. Orders before that date use different column names and status codes, so two years of trading cannot be read without reconciling them.
A digital sales channel launched in January 2026, adding a column the earlier extracts do not have.

Order, invoice, payment and shipment data arrives as monthly file drops from systems that have never spoken to each other. Analysts rebuild the picture in a spreadsheet each month. It takes a week and breaks when someone is on leave.

---

## 2. The business problem

Kestrel has plenty of data and no ability to use it. Specifically:

**There is no single source of truth.** Order data sits in the ERP, invoices in the finance system, payments in the treasury export, shipments in carrier reports, and reference data in a set of spreadsheets that individual teams maintain. Every one of them drops files monthly. Nobody has ever joined them end to end.

**Reporting is manual and it does not scale.** Two analysts spend the first week of every month stitching those files together in Excel. The workbook is now large enough to crash, the formulas are undocumented, and when one of the analysts is on leave the month-end pack is late. Two years of trading is roughly 1.4 million transaction rows, which is well past what a spreadsheet should be asked to hold.

**The ERP migration broke historical continuity.** The legacy system named its columns one way, the new system names them another, and the 2026 extract added a field neither of the others has. Any attempt to look at trading across the full two years falls over at the boundary. The business currently cannot answer "how did this customer perform last year versus this year" without manual mapping.

**Data arrives late and out of order.** Payments in particular land months after the order they settle. Because the analysts filter by order date when they refresh, late payments are silently missed. Numbers reported in one month quietly change the next, and nobody can explain why.

**There is no history and no audit trail.** Reference files are overwritten each month. When a customer moves between regions, last year's regional numbers change retrospectively. There is no record of what the data looked like when a decision was made, which is now a problem for the annual audit.

I have been engaged as a data engineer to design and build a platform that solves these problems: one governed, automated, auditable pipeline that takes the monthly file drops and produces a reporting model the business can trust.

---

## 3. The data

### Simulation note

The data used in this project is **synthetically generated** to simulate Kestrel's real operating environment. It was produced by a generator script that models a genuine order-to-cash process and then deliberately introduces the kinds of defects found in production systems: inconsistent naming between legacy and current platforms, late-arriving records, duplicate master data, schema changes between extracts, and missing values.

### How the data arrives

Source systems export on a monthly cycle. Each export produces a dated batch, and batches accumulate in the landing area rather than overwriting.

**Coverage:** August 2024 to July 2026, 24 months, 151 batch files.

### Transactional feeds, batched monthly

| Dataset | Format | Batches | Rows | Partitioned by |
|---|---|---|---|---|
| `orders_legacy` | Parquet | 5 | ~85,000 | Order month |
| `orders_2025` | Parquet | 12 | ~200,000 | Order month |
| `orders_2026` | Parquet | 7 | ~115,000 | Order month |
| `order_line_items` | Parquet | 24 | 1,000,000 | Order month |
| `invoices` | Parquet | 25 | 352,388 | Invoice month |
| `invoice_lines` | Parquet | 25 | 880,946 | Invoice month |
| `payments` | Parquet | 28 | 302,349 | Payment month |
| `shipments` | Parquet | 25 | 405,246 | Shipment month |

### Reference data, delivered as flat files

| Dataset | Format | Rows | Content |
|---|---|---|---|
| `CUST_MASTER` | CSV | 5,100 | Customer master from the legacy platform |
| `customer_contacts` | CSV | 5,000 | Contact names, emails, phone numbers |
| `Address` | CSV | 10,000 | Billing and shipping addresses |
| `cities` | CSV | 40 | City to region mapping |
| `regions` | CSV | 5 | Region reference |
| `products` | CSV | 1,200 | Product master with cost and list price |
| `subcategories` | CSV | 18 | Product hierarchy |
| `channels` | CSV | 4 | Sales channel reference |
| `CAMPAIGN_LOG` | CSV | 654 | Denormalised daily marketing log |
| `campaign_skus` | CSV | 377 | Campaign to SKU mapping |
| `sales_targets` | CSV | 115 | Monthly revenue target by region |
| `exchange_rates` | CSV | 144 | Monthly conversion rate to sterling |


### The data quality and standardisation issues:

**Different formats.** Transactional data arrives as Parquet with typed schemas. Reference data arrives as CSV, produced by whoever owns the spreadsheet. Two file formats, two levels of reliability, one pipeline.

**Different naming conventions.** Legacy tables use uppercase with abbreviations: `ORDER_NO`, `ORD_DT`, `CUST_ID`, `PRIORITY_CD`. Current tables use lowercase with full words: `order_id`, `order_date`, `customer_id`, `priority`. `CUST_MASTER` and `CAMPAIGN_LOG` still carry legacy naming despite being current files.

**Different vocabularies for the same thing.** The legacy order status is `DELIVERED`; the current one is `Delivered`. Legacy priority is `STD` and `EXP`; current is `Standard` and `Express`. Union them without normalising and every priority-level report silently splits into six categories instead of three.

**Schema drift between extracts.** The 2026 order file carries an `order_source` column that the 2025 and legacy files do not. Any pipeline assuming a fixed schema breaks at the January 2026 boundary.

**Different grains in the same chain.** Orders are one row per order. Line items are one row per line. Shipments and payments both fan out, so a single order can have several of each. Joining the chain inflates row counts by roughly a quarter.

**Different event clocks.** Each feed partitions by its own event date, not by order date. A January order can be invoiced in February and paid in May, so its three records sit in three batches spread across five months.

**Genuine defects.** Duplicate customer IDs in the master file. Null SKUs on order lines. Order headers with no lines attached. Missing invoice totals. Phone numbers exported from Excel with a leading apostrophe. A missing month in the targets file.

---

## 4. The solution

## 4.1. Requirements

## What this project sets out to do

| # | Aim | Delivered by |
|---|---|---|
| 1 | **Data ingestion** | Landing → Bronze, parameterised by batch |
| 2 | **Data transformation** | Bronze → Silver, cleaned and conformed |
| 3 | **Reporting and analytics** | Silver → Gold, dimensional model |
| 4 | **Automated orchestration** | Control table and Databricks Workflow |
| 5 | **Data dictionary** | 


---

## 4.2. Environment setup

### Storage and governance

| Step | Detail |
|---|---|
| Platform | Databricks Free Edition, serverless compute |
| Governance | Unity Catalog, one metastore assigned to the workspace |
| Storage | Databricks-managed. No external cloud storage account is used |
| Catalog | `kestrel_data_eng_prj`, created without an explicit managed location so it inherits the metastore default |
| Landing volume | Managed volume `landing.files`, created without a location clause |
| File delivery | Batch folders uploaded to the volume through Catalog Explorer |

### Catalog structure

```
kestrel_data_eng_prj
├── landing     managed volume holding the raw file drop
├── bronze      raw ingest, one table per source
├── silver      cleaned and conformed
├── gold        dimensional model
└── control     batch control and audit tables
```

Created once, in `00-setup`:

```sql
CREATE CATALOG IF NOT EXISTS kestrel_data_eng_prj;
USE CATALOG kestrel_data_eng_prj;

CREATE SCHEMA IF NOT EXISTS landing;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;

CREATE VOLUME IF NOT EXISTS kestrel_data_eng_prj.landing.files;
```

No `MANAGED LOCATION` clause is given on the catalog and no `LOCATION` on the volume, so both use the metastore's default Databricks-managed storage.

### Notebook structure

```
/kestrel_data_eng_prj/
├── 00-common/
│   ├── 01.environment-configuration
│   ├── 02.bronze-helpers
│   └── 03.silver-helpers
    └── 04.gold-helpers
├── 01-Environment-setup/
├── 02-bronze-dimension/
├── 03-bronze-incremental-fact/
├── 04-bronze-to-silver transformation/
├── 05-gold/
└── 06-orchestration/
```

Folder names mirror the schemas, so the shape of the pipeline is visible from the workspace tree without reading any code.

---

## Shared components

Rather than repeating configuration and write logic in every notebook, three shared notebooks sit in `00-common` and are pulled in with `%run` wherever they are needed. A change to the write pattern happens in one place and applies everywhere.

| Notebook | Holds | Used by |
|---|---|---|
| `01.environment-configuration` | Catalog and schema names, the landing volume path, a path variable per source dataset. Configuration only, no logic | Every notebook |
| `02.bronze-helpers` | `add_ingestion_metadata`, `write_to_bronze` | Bronze notebooks |
| `03.silver-helpers` | `trim_whitespaces`, `remove_nulls`, `remove_duplicates`, `write_to_silver` | Silver notebooks |
| `04.gold-helpers` | `write_to_gold` | gold notebooks |

**What the bronze helpers do.** `add_ingestion_metadata` attaches an ingestion timestamp and the originating file path to every row, using Spark's built-in `_metadata` column so provenance does not have to be assembled by hand for each reader. `write_to_bronze` stamps the batch identifier onto the data and writes it as a Delta table partitioned by `batch_id`, using `replaceWhere` so that reprocessing a batch replaces it in place rather than appending a second copy.

**What the silver helpers do.** `trim_whitespaces` normalises leading, trailing and repeated internal whitespace across string columns only, leaving typed columns untouched. `remove_nulls` and `remove_duplicates` handle business key integrity. `write_to_silver` creates the Delta table on first run and merges on the business key thereafter, guarded so that an older batch cannot overwrite newer data, and preserving the original creation timestamp when a row is updated.

**What the gold helpers do.** `write_to_gold` creates the Delta table on first run and merges on the surrogate key thereafter, so a rebuild updates rows in place rather than duplicating them. `created_timestamp` is deliberately excluded from the update map, so it records when a row first entered the warehouse while only `updated_timestamp` moves

> Source: `00-common/01.environment-configuration`, `02.bronze-helpers`, `03.silver-helpers`

---

## 1. Landing → Bronze

Raw ingest with provenance attached. No cleaning, no business logic.

**Requirements**
- Ingest every source as delivered
- Explicit schema on read, so a source change fails loudly
- Attach provenance to every row
- Reference data reloads in full; transactional data loads one batch at a time
- Reprocessing a batch must replace it, never duplicate it

**Steps**
- `p_batch_id` widget on every notebook - one code path for backfill and incremental
- Shared configuration and helper functions pulled in with `%run`
- Declared `StructType` per CSV source with `FAILFAST`; Parquet read on its own schema
- `add_ingestion_metadata()` stamps ingestion timestamp and source file path
- `write_to_bronze()` writes Delta partitioned by `batch_id`, using `replaceWhere` for idempotency

📁 [`02-bronze-dimension/`](02-bronze-dimension) — 13 reference tables, full load
📁 [`03-bronze-incremental-fact/`](03-bronze-incremental-fact) — 6 transactional tables, incremental load

---

## 2. Bronze → Silver

Cleaned, conformed and merged on the business key.

**Requirements**
- Business-meaningful names in snake_case
- Trim and normalise values; drop columns not needed downstream
- Flag data quality issues rather than silently dropping rows
- Preserve source business keys for traceability
- Log row counts at every stage

**Steps**
- Read bronze filtered to the current batch
- Rename and project only what analytics needs
- Trim and normalise whitespace across string columns
- Apply quality flags; remove rows only where the business key is null
- Deduplicate deterministically on the declared business key
- `write_to_silver()` merges on the business key, guarded so an older batch cannot overwrite newer data

📁 [`04-bronze_to_silver_transformation/`](04-bronze_to_silver_transformation) — 19 tables

---

## 3. Silver → Gold

Star schema built for analysis.

**Requirements**
- Conformed dimensions, one fact per business event at its own grain
- Deterministic surrogate keys that survive a rebuild
- Row count in equals row count out — no fan-out
- Derived business measures live here, not in silver

**Steps**
- Explore each silver source: grain, business key, uniqueness, nulls
- Validate the driver table before building
- Generate surrogate keys with `xxhash64` on the business key
- Pre-aggregate one-to-many sources before joining, so facts hold their grain
- Left joins throughout — no fact row lost to a missing dimension
- Add derived columns: net line value, GBP conversion, cycle times, date keys, status flags
- Validate output, then `write_to_gold()` merges on the surrogate key
- 
**Model:** 4 dimensions · 4 facts · 1 bridge

📁 [`05_gold/`](05_gold)


---

## 4. Orchestration

Scheduled, unattended, recoverable.

**Requirements**
- Discover the next unprocessed batch automatically, oldest first
- Exit cleanly when there is nothing to do
- One batch identifier resolved once and passed to every task
- Failed batches become eligible again rather than blocking the pipeline

**Steps**
- `control.batch_control` tracks every batch and its status
- Batch discovery compares the landing volume against tracked batches
- Task values pass `p_batch_id` and a `has_batch` flag downstream
- Condition task routes the run, or ends it green when no batch is waiting
- Batch marked `in-progress` on claim, merged to `completed` on success
- Failure task runs on any upstream failure and marks the batch `failed`, so it retries next run

📁 [`06-orchestration/`](06-orchestration)


---

## Key decisions

| Decision | Why |
|---|---|
| `replaceWhere` on the batch partition | Reprocessing replaces rather than duplicates |
| Merge on the business key, not append | Handles corrections and lifecycle updates from later batches |
| Batch guard on the silver merge | Feeds run on different event clocks — an order can be invoiced and paid months later |
| Flag quality issues, never drop | Consumers see what is wrong instead of rows quietly disappearing |
| Hashed surrogate keys | Deterministic, so a full rebuild orphans nothing |
| Pre-aggregate before joining | A fact can only join to things at or above its own grain |
| Shipment kept as its own fact | Split orders often use two carriers, so carrier cannot collapse to order grain |

---

## Stack

Databricks (Free Edition) · PySpark · Delta Lake · Unity Catalog · Databricks Workflows · Power BI

---

## Notes and limitations

- Landing is a Unity Catalog managed volume. In production this would be an ADLS Gen2 external location with a managed identity credential — the pipeline code is unchanged either way.
- Layer notebooks are chained with `%run` inside a driver notebook. Free Edition permits one concurrent run, so nested job runs are not available; on a paid workspace each notebook would be its own job task.
- One batch processes per run. A backlog clears over successive runs.


---

