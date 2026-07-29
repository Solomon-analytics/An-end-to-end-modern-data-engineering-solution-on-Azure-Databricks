# Project Cascade

**An end-to-end order-to-cash lakehouse on Azure Databricks**

---

## 1. The company

**Kestrel Global Trading Ltd** is a B2B distributor of branded consumer and industrial goods, headquartered in London and trading across five regions. The business sells to roughly 5,000 trade customers spread over 40 cities, moving around 1,200 SKUs from a portfolio of suppliers.

Kestrel does not manufacture and does not run its own fleet. It buys, holds and distributes, and everything ships through five contracted carriers. Customers buy through four channels: direct field sales, telesales, distributor partners, and since early 2026 a digital channel split between web ordering and EDI integration with the larger accounts.

Because the customer base is genuinely international, Kestrel invoices in local currency. Five currencies sit in the ledger: US dollars, Singapore dollars, Brazilian real, euros and UAE dirhams. Group reporting is in sterling.

The business has grown quickly and not always tidily. A legacy ERP ran the order book until the end of 2024, when Kestrel migrated to a new platform. The migration went live in January 2025 and the digital channel was bolted on in January 2026. Each of those events left its mark on the data, and nobody was given the time to go back and reconcile what came before.

Marketing runs SKU-level promotional campaigns. Finance sets monthly revenue targets by region. Warehouse teams take a monthly stock count. All of it lives in separate systems that have never spoken to each other.

---

## 2. The business problem

Kestrel has plenty of data and no ability to use it. Specifically:

**There is no single source of truth.** Order data sits in the ERP, invoices in the finance system, payments in the treasury export, shipments in carrier reports, and reference data in a set of spreadsheets that individual teams maintain. Every one of them drops files monthly. Nobody has ever joined them end to end.

**Reporting is manual and it does not scale.** Two analysts spend the first week of every month stitching those files together in Excel. The workbook is now large enough to crash, the formulas are undocumented, and when one of the analysts is on leave the month-end pack is late. Two years of trading is roughly 1.4 million transaction rows, which is well past what a spreadsheet should be asked to hold.

**The ERP migration broke historical continuity.** The legacy system named its columns one way, the new system names them another, and the 2026 extract added a field neither of the others has. Any attempt to look at trading across the full two years falls over at the boundary. The business currently cannot answer "how did this customer perform last year versus this year" without manual mapping.

**Data arrives late and out of order.** Payments in particular land months after the order they settle. Because the analysts filter by order date when they refresh, late payments are silently missed. Numbers reported in one month quietly change the next, and nobody can explain why.

**There is no history and no audit trail.** Reference files are overwritten each month. When a customer moves between regions, last year's regional numbers change retrospectively. There is no record of what the data looked like when a decision was made, which is now a problem for the annual audit.

**Nobody can prove where a number came from.** When a figure in the board pack is challenged, the honest answer is that it came out of a spreadsheet and the working has been overwritten.

I have been engaged as a data engineer to design and build a platform that solves these problems: one governed, automated, auditable pipeline that takes the monthly file drops and produces a reporting model the business can trust.

---

## 3. The data

### Simulation note

The data used in this project is **synthetically generated** to simulate Kestrel's real operating environment. It was produced by a generator script that models a genuine order-to-cash process and then deliberately introduces the kinds of defects found in production systems: inconsistent naming between legacy and current platforms, late-arriving records, duplicate master data, schema changes between extracts, and missing values.

The generator is committed to this repository so the data can be reproduced and so reviewers can see that the imperfections are designed rather than accidental.

### How the data arrives

Source systems export on a monthly cycle. Each export produces a dated batch, and batches accumulate in the landing area rather than overwriting. This mirrors how most mid-sized businesses actually receive data: not a live feed, but a scheduled drop.

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
| `inventory` | CSV | 11,520 | Monthly stock-on-hand snapshot |
| `CAMPAIGN_LOG` | CSV | 654 | Denormalised daily marketing log |
| `campaign_skus` | CSV | 377 | Campaign to SKU mapping |
| `sales_targets` | CSV | 115 | Monthly revenue target by region |
| `exchange_rates` | CSV | 144 | Monthly conversion rate to sterling |
| `user_details` | CSV | 6 | User to region mapping for access control |

A `_manifest.csv` accompanies the batches, recording table, partition, row count, load timestamp and file size.

### The data is disparate and it is not clean

This is the heart of the engineering problem.

**Different formats.** Transactional data arrives as Parquet with typed schemas. Reference data arrives as CSV, produced by whoever owns the spreadsheet. Two file formats, two levels of reliability, one pipeline.

**Different naming conventions.** Legacy tables use uppercase with abbreviations: `ORDER_NO`, `ORD_DT`, `CUST_ID`, `PRIORITY_CD`. Current tables use lowercase with full words: `order_id`, `order_date`, `customer_id`, `priority`. `CUST_MASTER` and `CAMPAIGN_LOG` still carry legacy naming despite being current files.

**Different vocabularies for the same thing.** The legacy order status is `DELIVERED`; the current one is `Delivered`. Legacy priority is `STD` and `EXP`; current is `Standard` and `Express`. Union them without normalising and every priority-level report silently splits into six categories instead of three.

**Schema drift between extracts.** The 2026 order file carries an `order_source` column that the 2025 and legacy files do not. Any pipeline assuming a fixed schema breaks at the January 2026 boundary.

**Different grains in the same chain.** Orders are one row per order. Line items are one row per line. Shipments and payments both fan out, so a single order can have several of each. Joining the chain naively inflates row counts by roughly a quarter.

**Different event clocks.** Each feed partitions by its own event date, not by order date. A January order can be invoiced in February and paid in May, so its three records sit in three batches spread across five months.

**Genuine defects.** Duplicate customer IDs in the master file. Null SKUs on order lines. Order headers with no lines attached. Missing invoice totals. Phone numbers exported from Excel with a leading apostrophe. A missing month in the targets file.

None of this is unusual. It is what production data looks like, and handling it is the job.

---

## 4. The solution

# Defining project requirements

1. Data ingestion requirements
2. Data transformation requirements
3.  Reporting and analytical requirements
4.  Non-functional requirements

1. Data ingestion requirements:
   - ingest all dataset into the Date Lakehouse
   - Apply correct schema (columns and datatypes)
   - Add audit columns (ingestion timestamp, source file)
   - Data must in Delta format
   - Preserve Data integrity and Reliability
   - For static data - full load will be applied
   - For batch data - incremental load will be applied
     
2. Data transformation requirements
   - clean and standardise data
   - Apply consistent naming convention and reshape structure
   - Remove unnecessary columns and handle basic data quality checks
   - Apply data quality flags that ensure data meets business requirement
   - Preserve business keys across layers
   - Prepare dataset for Gold-layer analytics
     
3.  Reporting and analytical requirements
   - Optimised for reporting and analytical queries
   - Support recent and historical analysis
   - Optimised for reporting an
     
4.  Non-functional requirements
   - Scheduled and reliable pipeline execution
   - Monitoring, recovery, and alerting
   - Time travel, rollback, and Delta reliability


## 4.1 Setting up the Project Environment
- Setup Data Lake environment
- Configure unity catalog
- set up ADLS
- set managed location (ADLS C=container) in Databricks
- set up Catalog in Databricks
- set up schemas (landing, bronze, silver and gold)
- set up external volume in the landing layer

## 4.2 Ingesting from landing to bronze - setting up parameter and environmental functions and variables
 - Dynamically setting up an environment configuration: Create a new folder in the project folder (00-common). in this folder create a notebook(environment-configuration)
 - environment configiration should have the folowing variable (catalog_name, bronze_schema, silver_schema, gold_schema, control_schema and landing_folder_path)
 - create the second notebook in (00-comkon) folder - "02.bronze-helpers" - in this notebook, create two functions, add_ingestion_metadata and write_to_bronze

## 4.2 Ingesting ALL static/sct type tables from from landing to bronze Notebook
  - considering our fact files will be arriving in batches, we set a parameter "p_batch_id" for each notebooks
  - Call each of the notebooks(environment-configuration and 02.bronze-helpers) : "%run ../00-common/01.environment-configuration" and "%run ../00-common/02.bronze-helpers"
  - Create a variable for source file and target table(bronze)
  - create a schema for table
  - read All table to its defined schema
  - Add ingestion metadata
  - Write to bronze, matching on batch_id

## 4.2 Ingesting ALL fact datasets from landing to bronze
 - considering our fact files will be arriving in batches, we set a parameter "p_batch_id" for each notebooks
 - in the storage account, we create a nested folder for each dataset using the parameter "p_batch_id". example 2024-01 --> sales order --> 2024-01 --> file.parquet
 - in the (00-commom folder --> 01.environment-configuration notebook, add variable for each of fact dataset
 - Call each of the notebooks(environment-configuration and 02.bronze-helpers) : "%run ../00-common/01.environment-configuration" and "%run ../00-common/02.bronze-helpers"
 - create a schema for table
 - read All table to its defined schema
 - Add ingestion metadata
 - Write to bronze, matching on batch_id

## 4.2 Transforming and ingesting tables from bronze-silver: Requirement
 - Read all files using spark dataframe reader API
 - filter column, batch_id == v_batch_id
 - Keep only the columns required for analytics
 - Standardise all column headers using snake_case
 - Rename columns to business meaningful names
 - setup loggings for audit purpose
 - remove nulls from business keys
 - Remove duplicates
 - Transform values in string columns to title_case
 - Apply business transformation rules
 - Write transformed data to silver table


## 4.2 Ingesting ALL static/scd tables from bronze-silver - setting up
 - in the 00-commoin folder, create a new notebook (silver-helpers) - this will hold the basic_transformation and write_to_silver functions
 - Create a write to function that picks up the latest data using (source.batch_id >= target.batch_id)

















### 4.1 Storage: the enterprise data landing zone

Kestrel's source systems export to a central cloud storage account rather than pushing directly into any analytics tool. This is the standard enterprise pattern: storage is cheap, source teams own their exports, and the analytics platform reads rather than receives.

The landing zone is implemented as an **Azure Data Lake Storage Gen2** account with hierarchical namespace enabled, organised into containers by purpose:

```
kestrelstorage/
├── landing/          incoming batch files, immutable
│   ├── orders_2025/
│   │   ├── order_month=2025-01/
│   │   └── order_month=2025-02/
│   ├── order_line_items/
│   ├── invoices/
│   ├── payments/
│   └── _static/      reference CSVs
├── archive/          processed batches, retained
└── quarantine/       rejected records
```

Files landed here are never modified. They are the system of record for what the source systems actually sent, which is what makes the pipeline auditable.

### 4.2 Platform: Azure Databricks

The solution is built on **Azure Databricks**, chosen because it handles the full workload in one place: file ingestion at volume, transformation in PySpark, ACID guarantees through Delta Lake, governance through Unity Catalog, and scheduling through Workflows. There is no separate ingestion tool, no separate orchestrator and no separate catalogue to keep in sync.

### 4.3 Governance: Unity Catalog

Everything is registered in **Unity Catalog** before any data is loaded. Governance first, not retrofitted.

**Catalog**

```
kestrel_analytics
```

**Schemas, one per layer**

| Schema | Purpose |
|---|---|
| `landing` | Volumes pointing at raw files |
| `bronze` | Raw ingested tables, one per source |
| `silver` | Cleaned, conformed, quality-checked |
| `gold` | Dimensional model |
| `analytics` | Reporting views and aggregates |
| `control` | Batch control and audit tables |

**Workspace folder structure, mirroring the schemas**

```
/Workspace/kestrel_cascade/
├── 00_setup/            catalog, schemas, external locations, volumes
├── 01_landing/          file discovery and batch identification
├── 02_bronze/           landing to bronze notebooks
├── 03_silver/           bronze to silver notebooks
├── 04_gold/             silver to gold notebooks
├── 05_analytics/        reporting views
├── 06_orchestration/    control table and job task notebooks
└── 99_utils/            shared functions
```

The mapping between folder, schema and job task is deliberate. Anyone opening the workspace can see the shape of the pipeline without reading any code.

### 4.4 Connecting the lake: external location and landing volume

Databricks reaches the storage account through governed Unity Catalog objects rather than mounted paths or access keys.

**Storage credential.** An Azure Databricks Access Connector provides a managed identity, granted **Storage Blob Data Contributor** on the storage account. No secrets, no keys in notebooks.

**External location.** Registered against the container, bound to that credential:

```sql
CREATE EXTERNAL LOCATION kestrel_landing
URL 'abfss://landing@kestrelstorage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL kestrel_managed_identity);
```

**External volume.** Files are exposed to notebooks as a governed volume path:

```sql
CREATE EXTERNAL VOLUME kestrel_analytics.landing.raw_files
LOCATION 'abfss://landing@kestrelstorage.dfs.core.windows.net/';
```

Notebooks then read `/Volumes/kestrel_analytics/landing/raw_files/...` as an ordinary path. Access is governed by Unity Catalog permissions, and every read is captured in lineage.

### 4.5 Landing to bronze

Bronze holds the source data exactly as delivered, with provenance attached. No business logic, no filtering, no cleaning.

Each batch is ingested with a **`p_batch_id` parameter** passed in from the orchestration job, which identifies precisely which batch this run is processing.

**Metadata columns added to every bronze table:**

| Column | Purpose |
|---|---|
| `p_batch_id` | The batch this row belongs to |
| `_source_file` | Full path of the file it came from |
| `_source_system` | Originating system |
| `_ingested_at` | Ingestion timestamp |
| `_ingested_by` | Job run identifier |

These five columns give row-level traceability. Any figure in the final report can be traced back to the file that produced it, which directly addresses the auditability problem in section 2.

**Load pattern.** Delete by `p_batch_id`, then insert. Reprocessing a batch replaces it cleanly rather than duplicating it, so the load is idempotent by construction.

**Schema evolution** is enabled so a new source column, such as `order_source` appearing in 2026, lands in bronze rather than failing the job. Whether it is used downstream is a silver decision, not an ingestion one.

### 4.6 Bronze to silver

Silver is where the data becomes usable. One table per business entity, cleaned, conformed and quality-checked.

**Structural conformance**

- The three order extracts are unioned into a single `silver.orders`, with legacy columns renamed to the current standard
- Value vocabularies are normalised: `DELIVERED` and `Delivered` become one value, `STD` and `Standard` become one value
- Columns present in only some extracts are handled explicitly, not by accident

**Cleaning**

- Types cast and enforced
- Strings trimmed, casing standardised
- Excel artefacts removed, such as the leading apostrophe on phone numbers
- Duplicate master records resolved on a documented survivorship rule

**Data quality flags**

Rather than dropping suspect rows, silver **flags** them. Every record carries its quality assessment and continues downstream, so the business can see what is wrong rather than having it quietly removed.

| Column | Meaning |
|---|---|
| `dq_status` | `PASS`, `WARN` or `FAIL` |
| `dq_rules_failed` | Array of rule identifiers that failed |
| `dq_checked_at` | When the assessment ran |

Rules are catalogued with a severity. Critical failures, such as a null business key or a broken parent relationship, route the record to `quarantine` with its rule identifier attached. Warnings, such as an out-of-range date, are flagged and passed through.

Nothing is ever silently dropped. If a row does not reach gold, there is a record explaining why.

**Merge logic**

Silver loads use a Delta `MERGE` on the business key: update where the key exists and the record has changed, insert where it does not. This handles both corrections to previously loaded records and the late-arriving records described in section 2.

**Audit logging**

Every silver load writes to `control.audit_log`:

| Column | Purpose |
|---|---|
| `audit_id` | Unique run identifier |
| `p_batch_id` | Batch processed |
| `table_name` | Target table |
| `layer` | Pipeline layer |
| `rows_read` | Input count |
| `rows_inserted` | New records |
| `rows_updated` | Changed records |
| `rows_quarantined` | Rejected records |
| `started_at` / `ended_at` | Timing |
| `status` | Outcome |
| `error_message` | Failure detail |

Combined with Delta time travel, this gives a complete record of what changed, when, and as a result of which batch.

### 4.7 Silver to gold

Gold restructures conformed data into a dimensional model built for analysis rather than for processing.

**Fact table**

| Table | Grain |
|---|---|
| `fct_sales` | One row per order line |

Order line measures, with invoice and payment status carried down from the parent order. Shipments and payments are collapsed to one row per order before joining, so the fan-out does not inflate the fact.

**Dimensions**

| Table | Type |
|---|---|
| `dim_customer` | Slowly changing, Type 2 |
| `dim_product` | Slowly changing, Type 2 |
| `dim_date` | Generated, full calendar |
| `dim_channel` | Type 1 |
| `dim_geography` | Type 1, role-playing for bill-to and ship-to |

**Business columns added at this layer:** net line value with discount correctly applied, margin against product cost, sterling equivalent using the rate for the invoice month, order-to-invoice and invoice-to-payment day counts, and derived status flags.

Dimensions carry surrogate keys. Facts join on the surrogate valid at the transaction date, so historical reporting stays correct when a customer moves region. This directly addresses the retrospective-change problem in section 2.

Referential integrity is enforced before load: no fact row reaches gold without a valid dimension key, and unmatched keys resolve to an explicit unknown member rather than being dropped.

### 4.8 Analytics layer

The `analytics` schema exposes the model to reporting through views, so consumers never query gold tables directly and the physical model can change without breaking reports.

| View | Purpose |
|---|---|
| `vw_sales_performance` | Revenue, margin and volume by customer, product and month |
| `vw_order_to_cash` | Ordered, invoiced and collected value with the gap at each step |
| `vw_customer_360` | Customer profile with trading history and payment behaviour |
| `vw_product_performance` | SKU performance against stock position |
| `vw_target_attainment` | Regional actuals against regional targets |
| `vw_data_quality_summary` | Quality flags and quarantine counts by batch |

That final view matters. Exposing data quality to the business, rather than hiding it, is what makes the platform trustworthy.

A **Power BI** semantic model sits on the analytics layer, connected through the Databricks SQL connector.

### 4.9 Orchestration

The pipeline runs as a single **Databricks Workflow**, driven by a control table.

**Control table** `control.batch_control`:

| Column | Purpose |
|---|---|
| `batch_id` | Batch identifier |
| `source_feed` | Which feed this batch belongs to |
| `status` | `pending`, `in-progress`, `completed`, `failed` |
| `row_count` | Records processed |
| `created_timestamp` | First seen |
| `updated_timestamp` | Last state change |
| `error_message` | Failure detail |

**Job structure**

```
get_next_batch          identify the earliest unprocessed batch
       ↓
check_has_batch         condition task
       ↓ true                    ↓ false
mark_in_progress                end, success
       ↓
landing_to_bronze
       ↓
bronze_to_silver
       ↓
silver_to_gold
       ↓
refresh_analytics
       ↓
mark_completed
```

A `mark_failed` task runs on the failure path of any processing task, setting the batch to `failed` so it becomes eligible again on the next run rather than being stuck.

**Task values** pass `p_batch_id` between tasks, so every notebook in the run operates on the same batch without hardcoding.

**Concurrency** is capped at one run, preventing two runs from claiming the same batch.

The job is scheduled. It checks for new batches, processes what it finds, and exits cleanly when there is nothing to do. No manual intervention, which is the point.

---

## 5. What this delivers

Against the problems in section 2:

| Problem | Resolution |
|---|---|
| No single source of truth | One governed model in Unity Catalog, all sources conformed |
| Manual, unscalable reporting | Automated pipeline, no spreadsheets |
| ERP migration broke continuity | Three schemas unioned and normalised into one history |
| Late and out-of-order data | Merge logic and lookback handle arrivals whenever they land |
| No history, retrospective change | Type 2 dimensions preserve point-in-time truth |
| No audit trail | Row-level provenance, audit log, Delta time travel |
| Cannot prove where a number came from | Every figure traces to a batch, a file and a load run |

