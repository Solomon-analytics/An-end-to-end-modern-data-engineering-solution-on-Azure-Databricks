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

### The data quality and standardisation issues:


**Different formats.** Transactional data arrives as Parquet with typed schemas. Reference data arrives as CSV, produced by whoever owns the spreadsheet. Two file formats, two levels of reliability, one pipeline.

**Different naming conventions.** Legacy tables use uppercase with abbreviations: `ORDER_NO`, `ORD_DT`, `CUST_ID`, `PRIORITY_CD`. Current tables use lowercase with full words: `order_id`, `order_date`, `customer_id`, `priority`. `CUST_MASTER` and `CAMPAIGN_LOG` still carry legacy naming despite being current files.

**Different vocabularies for the same thing.** The legacy order status is `DELIVERED`; the current one is `Delivered`. Legacy priority is `STD` and `EXP`; current is `Standard` and `Express`. Union them without normalising and every priority-level report silently splits into six categories instead of three.

**Schema drift between extracts.** The 2026 order file carries an `order_source` column that the 2025 and legacy files do not. Any pipeline assuming a fixed schema breaks at the January 2026 boundary.

**Different grains in the same chain.** Orders are one row per order. Line items are one row per line. Shipments and payments both fan out, so a single order can have several of each. Joining the chain naively inflates row counts by roughly a quarter.

**Different event clocks.** Each feed partitions by its own event date, not by order date. A January order can be invoiced in February and paid in May, so its three records sit in three batches spread across five months.

**Genuine defects.** Duplicate customer IDs in the master file. Null SKUs on order lines. Order headers with no lines attached. Missing invoice totals. Phone numbers exported from Excel with a leading apostrophe. A missing month in the targets file.



---

## 4. The solution

## 1. Requirements

### 1.1 Data ingestion

| # | Requirement |
|---|---|
| ING-01 | Ingest all source datasets into the lakehouse without transformation |
| ING-02 | Apply an explicit schema to every source, so a source change fails loudly rather than silently |
| ING-03 | Add audit columns to every row: ingestion timestamp, source file, batch identifier |
| ING-04 | Store all tables in Delta format |
| ING-05 | Static reference data is fully reloaded on each run |
| ING-06 | Batched transactional data is loaded incrementally, one batch per run |
| ING-07 | Reprocessing a batch must replace it, never duplicate it |

### 1.2 Data transformation

| # | Requirement |
|---|---|
| TRF-01 | Standardise column names to snake_case and rename to business-meaningful terms |
| TRF-02 | Remove columns not required for analytics |
| TRF-03 | Trim and normalise whitespace in string columns |
| TRF-04 | Remove rows where a business key is null |
| TRF-05 | Deduplicate on the declared business key, deterministically |
| TRF-06 | Flag data quality issues rather than silently dropping rows |
| TRF-07 | Preserve source business keys across all layers for traceability |
| TRF-08 | Log row counts at each stage for audit |
| TRF-09 | Apply business transformation rules |
| TRF-10 | Produce a dataset ready for dimensional modelling in gold |

### 1.3 Reporting and analytics

| # | Requirement |
|---|---|
| RPT-01 | Model optimised for analytical query patterns rather than transactional access |
| RPT-02 | Support both current-period and historical analysis |
| RPT-03 | Historical figures must remain stable when reference data changes |
| RPT-04 | Every reported figure traceable to a source file and batch |
| RPT-05 | Expose data quality status to consumers rather than concealing it |

### 1.4 Non-functional

| # | Requirement |
|---|---|
| NFR-01 | Pipeline runs on a schedule without manual intervention |
| NFR-02 | A failed run must be recoverable without manual data cleanup |
| NFR-03 | Batch processing state visible and queryable at all times |
| NFR-04 | Delta time travel and rollback available on every table |
| NFR-05 | Row counts, rejections and failures logged for every run |

---

## 2. Environment setup

### 2.1 Storage and governance

| Step | Detail |
|---|---|
| Platform | Databricks Free Edition, serverless compute |
| Governance | Unity Catalog, one metastore assigned to the workspace |
| Storage | Databricks-managed. No external cloud storage account is used |
| Catalog | `kestrel_data_eng_prj`, created without an explicit managed location so it inherits the metastore default |
| Landing volume | Managed volume `landing.files`, created without a location clause |
| File delivery | Batch folders uploaded to the volume through Catalog Explorer |

### 2.2 Catalog structure

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

### 2.3 Production equivalent

The landing zone here is a Unity Catalog **managed** volume, because Free Edition does not support custom storage locations.

In a production deployment the same layer would be an **external** volume over an ADLS Gen2 container, registered as a Unity Catalog external location and authenticated with a managed identity storage credential:

```sql
CREATE EXTERNAL LOCATION kestrel_landing
  URL 'abfss://landing@<storage-account>.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL kestrel_managed_identity);

CREATE EXTERNAL VOLUME kestrel_data_eng_prj.landing.files
  LOCATION 'abfss://landing@<storage-account>.dfs.core.windows.net/';
```

**The pipeline code is identical either way.** Both resolve to a governed `/Volumes/...` path, so every notebook, every path variable and every `dbutils.fs` call works unchanged. Only the object definition differs.

### 2.4 Notebook structure

```
/kestrel_data_eng_prj/
├── 00-common/
│   ├── 01.environment-configuration
│   ├── 02.bronze-helpers
│   └── 03.silver-helpers
├── 01-bronze/
├── 02-silver/
├── 03-gold/
└── 04-orchestration/
```

Folder names mirror the schemas, so the shape of the pipeline is visible from the workspace tree without reading any code.

---

## 3. Shared components

Rather than repeating configuration and write logic in every notebook, three shared notebooks sit in `00-common` and are pulled in with `%run` wherever they are needed. A change to the write pattern happens in one place and applies everywhere.

| Notebook | Holds | Used by |
|---|---|---|
| `01.environment-configuration` | Catalog and schema names, the landing volume path, a path variable per source dataset. Configuration only, no logic | Every notebook |
| `02.bronze-helpers` | `add_ingestion_metadata`, `write_to_bronze` | Bronze notebooks |
| `03.silver-helpers` | `trim_whitespaces`, `remove_nulls`, `remove_duplicates`, `write_to_silver` | Silver notebooks |

**What the bronze helpers do.** `add_ingestion_metadata` attaches an ingestion timestamp and the originating file path to every row, using Spark's built-in `_metadata` column so provenance does not have to be assembled by hand for each reader. `write_to_bronze` stamps the batch identifier onto the data and writes it as a Delta table partitioned by `batch_id`, using `replaceWhere` so that reprocessing a batch replaces it in place rather than appending a second copy.

**What the silver helpers do.** `trim_whitespaces` normalises leading, trailing and repeated internal whitespace across string columns only, leaving typed columns untouched. `remove_nulls` and `remove_duplicates` handle business key integrity. `write_to_silver` creates the Delta table on first run and merges on the business key thereafter, guarded so that an older batch cannot overwrite newer data, and preserving the original creation timestamp when a row is updated.

> Source: `00-common/01.environment-configuration`, `02.bronze-helpers`, `03.silver-helpers`

---

## 4. Landing to bronze

### 4.1 What this layer is for

Bronze holds the source data exactly as delivered, with provenance attached. No cleaning, no filtering, no business logic. Its purpose is to be the auditable record of what the source actually sent, so that any downstream figure can be traced back to a physical file.

### 4.2 What was set up

**A batch folder convention in the landing volume.** Files are uploaded into a folder per batch. Static reference files sit at the batch root; batched transactional datasets sit under a named subfolder, because they carry their own event-month partition.

```
/Volumes/kestrel_data_eng_prj/landing/files/
└── 2025-01/
    ├── Address.csv
    ├── sales_order/2025-01/part-000.parquet
    └── sales_order_lines/2025-01/part-000.parquet
```

**A `p_batch_id` parameter on every bronze notebook.** Declared as a widget and read into a variable at the top of each notebook. The same notebook therefore serves both the initial backfill and every incremental run, with nothing hardcoded and no separate code path to maintain.

**One notebook per source table.** Each builds its source path and target table name from the shared configuration, declares the schema, reads the file, attaches metadata and writes to bronze. The notebooks are short and near-identical by design, because all the reusable logic lives in `00-common`.

**Explicit schemas on CSV sources.** Every CSV is read against a declared `StructType` with `mode=FAILFAST`, so a source adding, removing or renaming a column fails the batch loudly instead of silently producing nulls. Parquet sources are read without a declared schema, since Parquet is self-describing and declaring one requires the physical types to match exactly, failing on differences as minor as `bigint` against `int`. Type casting for those sources happens in silver.

### 4.3 What each run does

1. Reads the batch identifier from the notebook parameter
2. Loads shared configuration and helper functions
3. Resolves the source file path for that batch and the target bronze table
4. Reads the file, with a declared schema where the format requires one
5. Attaches the ingestion timestamp and source file path to every row
6. Stamps the batch identifier on and writes to Delta, partitioned by batch

### 4.4 Decisions worth recording

| Decision | Reasoning |
|---|---|
| Explicit schema with `FAILFAST` on CSV | A source schema change should fail the batch, not produce silent nulls |
| Partition by `batch_id` | Makes `replaceWhere` cheap and lets a single batch be reprocessed in isolation |
| `overwrite` with `replaceWhere` rather than `append` | Reprocessing replaces the batch instead of duplicating it, which is what makes the load idempotent |
| Parameterised rather than dated notebooks | One code path for backfill and incremental load |
| Metadata attached before write | Provenance recorded at the point of ingest, not reconstructed afterwards |
| No transformation in bronze | Preserves the auditable record of what the source sent |

> Source: `01-bronze/` — one notebook per table. `01.address` is the reference implementation for a CSV source; `05.sales-order-lines` for a Parquet source.

---

## 5. Bronze to silver

### 5.1 What this layer is for

Silver is where the data becomes usable. Columns are renamed to business terms, values are cleaned and conformed, quality issues are assessed and flagged, and rows are merged on the business key so that corrections and lifecycle updates from later batches land in place rather than accumulating as duplicates.

### 5.2 What was set up

**A shared silver helper notebook**, holding the cleaning functions and the merge. As with bronze, the per-table notebooks stay short.

**A rename map per table**, applied on read, converting source column names to business-meaningful snake_case. `line_id` becomes `order_line_id`, `sku` becomes `product_sku`, `quantity` becomes `line_quantity`, and so on. Source technical columns not required downstream are dropped at the same point.

**Row count logging at each stage.** The incoming count, the count after removing null business keys, and the count after deduplication are printed on every run. Any unexplained movement in row count is therefore visible immediately rather than discovered weeks later in a report.

**Data quality flags rather than filters.** Where a row is suspect but usable, it is tagged and passed through. Only rows whose business key is null are removed, because such a row cannot be merged, joined or traced and has no downstream use.

**A merge on the declared business key**, with a guard preventing an older batch from overwriting newer data. This matters because the source feeds arrive on different event clocks: an order raised in January can be invoiced in February and paid in May, so batches do not arrive in a single ordered sequence.

### 5.3 What each run does

1. Reads the batch identifier from the notebook parameter
2. Loads shared configuration and helper functions
3. Reads the bronze table, filtered to the current batch
4. Drops columns not required downstream and renames the rest to business terms
5. Records the incoming row count
6. Trims and normalises whitespace across string columns
7. Applies the data quality flags defined for that table
8. Removes rows with a null business key, logging the count
9. Deduplicates on the business key, logging the count
10. Applies business transformation rules
11. Merges into the silver table on the business key

### 5.4 Decisions worth recording

| Decision | Reasoning |
|---|---|
| Filter bronze to the current batch | Silver processes one batch per run, matching the orchestration |
| Merge on the business key rather than append | Handles corrections and lifecycle updates arriving in later batches |
| Batch guard on the merge condition | Prevents an out-of-order batch overwriting newer data |
| Flag quality issues, do not drop | Consumers can see what is wrong rather than having rows silently removed |
| Drop only on a null business key | Such a row cannot be merged, joined or traced |
| Preserve `created_timestamp` on update | Retains when a record first entered the warehouse; only `updated_timestamp` moves |
| Row counts logged at every stage | Unexplained movement becomes visible on the run, not in a report |

> Source: `02-silver/` — one notebook per table. `05.sales-order-lines` is the reference implementation.

### 5.5 Business key and merge condition per table

| Silver table | Merged on |
|---|---|
| `sales_order` | `order_id` |
| `sales_order_lines` | `order_line_id` |
| `invoice` | `invoice_no` |
| `invoice_lines` | `invoice_no` + `line_no` |
| `shipments` | `shipment_id` |
| `payment` | `payment_id` |
| `address` | `customer_id` + `address_type` |

Where a natural composite key exists and is stable, it is preferred. Where the only stable identifier is the one supplied by the source, that is used and the dependency is recorded as an assumption. Candidate composites containing a mutable column, such as an amount or a date, were rejected: a correction changing that value would prevent the merge from matching the original row, inserting a duplicate rather than updating in place.

---

## 6. Data quality findings

Two issues were investigated in `sales_order_lines`. Both are flagged rather than resolved, because neither can be resolved from the data available.

**Identical lines under different keys.** A small number of order lines share the same order, product, quantity, unit price and discount, differing only in `order_line_id`. The table carries no line number and no requested delivery date, so a deliberately split line cannot be distinguished from an accidental re-key. Tagged as `sales_identical_flag` for the business to review.

The same investigation confirmed that `order_id + product_sku` is **not** the grain of this table. Lines legitimately repeat the same product where different portions attract different discount tiers, so a portion at the volume rate and a portion at full price appear as two lines. Deduplicating on that combination would have destroyed valid revenue lines. `order_line_id` is the only column expressing the grain, and is therefore the merge key.

**Missing product key.** A proportion of lines arrive with no `product_sku`. These are not dropped: they carry real revenue, and removing them would understate group totals with no error to indicate it had happened. Tagged as `product_is_null_flag` and routed to an explicit unknown product member when the fact is built in gold.

Analysis showed that `line_unit_price` identifies the product for the large majority of these rows, which indicates the SKU exists in the source system and is being lost during extraction rather than being genuinely unknown. This is published as a data quality finding rather than applied as a fix. Deriving a business key from a measure would couple the fact's key to a dimension attribute and break whenever a price changed, and it would mask a source defect that ought to be corrected upstream.

> Source: `02-silver/05.sales-order-lines` contains the investigation queries, retained as commented cells.

---

## 7. Audit and traceability

Every silver row carries the full chain back to source:

| Column | Set by | Answers |
|---|---|---|
| `batch_id` | Job parameter | Which batch delivered this row |
| `source_file` | Spark `_metadata` at ingest | Which physical file it came from |
| `ingestion_timestamp` | Bronze write | When it landed |
| `created_timestamp` | Silver first write | When the row first appeared in silver |
| `updated_timestamp` | Silver merge | When it was last changed |

Combined with Delta time travel on every table, any figure in a report can be traced to the file that produced it and the run that loaded it, and the state of any table at any past point can be reconstructed.

---

## 8. Notebook index

| Path | Purpose |
|---|---|
| `00-common/01.environment-configuration` | Catalog, schema and path variables |
| `00-common/02.bronze-helpers` | Ingestion metadata and bronze write |
| `00-common/03.silver-helpers` | Cleaning functions and silver merge |
| `01-bronze/` | One notebook per source table, landing to bronze |
| `02-silver/` | One notebook per table, bronze to silver |
| `03-gold/` | Dimensional model |
| `04-orchestration/` | Control table and job task notebooks |



