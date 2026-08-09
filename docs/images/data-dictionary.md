# Data Dictionary

`docs/data-dictionary.md`

Reference for the gold layer, which is what reporting queries. Silver and bronze are listed at the end.

Every gold table carries `created_timestamp` (when the row first entered gold) and `updated_timestamp` (when it was last rebuilt). These are omitted from the tables below to save repetition.

---

## Dimensions

### dim_customer

One row per customer. Ship-to address attached.

| Column | Type | Description |
|---|---|---|
| `customer_sk` | bigint | Surrogate key. Hash of `customer_id` |
| `customer_id` | string | Business key from the source customer master |
| `customer_name` | string | Trading name |
| `customer_segment` | string | SMB, Mid-Market or Enterprise |
| `customer_active_flag` | string | Y or N |
| `customer_street` | string | Ship-to street |
| `customer_postal_code` | string | Ship-to postcode |
| `customer_address_type` | string | Address role, always SHIP_TO in this dimension |
| `customer_city_id` | string | Foreign key to the city reference |
| `customer_city_name` | string | City |
| `customer_region_id` | string | Foreign key to the region reference |
| `customer_region_name` | string | Region: EU, US, APAC, LATAM or ME |

### dim_customer_account

One row per customer account. Commercial terms and bill-to address. Deduplicated to the latest created date.

| Column | Type | Description |
|---|---|---|
| `bill_to_account_sk` | bigint | Surrogate key. Hash of `account_id` |
| `account_id` | string | Business key, same value as `customer_id` |
| `account_segment` | string | SMB, Mid-Market or Enterprise |
| `account_credit_limit` | decimal | Approved credit limit |
| `account_payment_terms` | string | Prepaid, Net 15, Net 30 or Net 60 |
| `account_manager` | string | Owning account manager |
| `account_active_flag` | string | Y or N |
| `customer_account_created_date` | date | When the account was opened |
| `customer_account_created_date_id` | int | Date key, `yyyyMMdd` |
| `account_bill_to_postal_code` | string | Bill-to postcode |
| `account_bill_to_address_type` | string | Address role, always BILL_TO |
| `account_city_id` | string | Foreign key to the city reference |
| `account_city_name` | string | City |
| `account_region_id` | string | Foreign key to the region reference |
| `account_region_name` | string | Region |

### dim_product

One row per SKU, with its category hierarchy.

| Column | Type | Description |
|---|---|---|
| `product_sk` | bigint | Surrogate key. Hash of `product_id` |
| `product_id` | string | SKU, the business key |
| `product_name` | string | Product name |
| `product_brand` | string | Brand |
| `product_price` | decimal | List price |
| `product_cost` | decimal | Standard cost, used for margin |
| `product_supplier` | string | Supplying vendor |
| `product_sub_category_name` | string | Subcategory, 18 values |
| `product_category_name` | string | Category: Apparel, Beauty, Electronics, Home, Sports or Industrial |

### dim_campaign

One row per marketing campaign. Attributes arrive repeated on the daily campaign log and are aggregated to campaign grain here.

| Column | Type | Description |
|---|---|---|
| `campaign_sk` | bigint | Surrogate key. Hash of `campaign_id` |
| `campaign_id` | string | Business key |
| `campaign_name` | string | Campaign name |
| `campaign_channel` | string | Display, Email, Paid Search or Social |
| `total_campaign_budget` | decimal | Approved budget for the campaign |
| `campaign_start_date` | date | First day of the campaign |
| `campaign_end_date` | date | Last day of the campaign |
| `campaign_start_date_id` | int | Date key, `yyyyMMdd` |
| `campaign_end_date_id` | int | Date key, `yyyyMMdd` |
| `campaign_duration_days` | int | Length in days, inclusive of both endpoints |

---

## Facts

### fact_sales_order

One row per order. Invoice and payment status carried down from the order's invoice.

| Column | Type | Description |
|---|---|---|
| `order_sk` | bigint | Surrogate key. Hash of `order_number` |
| `order_number` | string | Source order number, degenerate dimension |
| `customer_sk` | bigint | Foreign key to `dim_customer` |
| `customer_name` | string | Denormalised from the customer dimension |
| `bill_to_account_sk` | bigint | Foreign key to `dim_customer_account` |
| `account_payment_terms` | string | Denormalised from the account dimension |
| `order_status` | string | Order lifecycle status |
| `channel_name` | string | Sales channel |
| `payment_method` | string | Method on the most recent payment against the invoice |
| `order_date` | date | Date the order was raised |
| `invoice_date` | date | Date the order was invoiced. Null if not yet invoiced |
| `payment_date` | date | Date of first payment. Null if unpaid |
| `order_date_id` | int | Date key, `yyyyMMdd` |
| `invoice_date_id` | int | Date key, `yyyyMMdd` |
| `payment_date_id` | int | Date key, `yyyyMMdd` |
| `order_total_gbp` | decimal | Invoice total converted to sterling at the rate for its invoice month |

**Notes.** Around 12% of orders have no invoice, and roughly 22% of invoices are never paid, so null invoice and payment dates are a valid business state rather than missing data. Payments are aggregated to invoice grain before joining, so an invoice settled in instalments does not duplicate the order.

### fact_sales_order_lines

One row per order line. The transactional grain of the model.

| Column | Type | Description |
|---|---|---|
| `order_line_id` | string | Source line identifier, the business key |
| `order_sk` | bigint | Foreign key to `fact_sales_order` |
| `sales_order_number` | string | Source order number, degenerate dimension |
| `product_sk` | bigint | Foreign key to `dim_product`. Unknown member where the SKU is missing |
| `product_number` | string | Source SKU. Null on around 1.5% of lines |
| `customer_sk` | bigint | Inherited from the order |
| `bill_to_account_sk` | bigint | Inherited from the order |
| `line_quantity` | int | Units ordered |
| `line_unit_price` | decimal | Price per unit before discount |
| `line_discount_pct` | decimal | Discount rate: 0, 0.05, 0.10 or 0.15 |
| `line_total` | decimal | As delivered by the source. Does **not** apply the discount |
| `net_line_value` | decimal | Calculated: quantity × unit price × (1 − discount). The correct revenue figure |
| `order_line_status` | string | Order status, inherited from the order |
| `order_date` | date | Inherited from the order |
| `order_date_id` | int | Date key, `yyyyMMdd` |

**Notes.** `line_total` is retained as delivered so the two can be compared. The source never applies the discount, which overstates revenue by roughly 5%. Use `net_line_value` for reporting.

### fact_shipment

One row per shipment. Kept separate from the order because 15% of orders ship in two consignments, and most of those use two carriers.

| Column | Type | Description |
|---|---|---|
| `shipment_sk` | bigint | Surrogate key. Hash of `shipment_number` |
| `shipment_number` | string | Source shipment identifier |
| `order_sk` | bigint | Foreign key to `fact_sales_order` |
| `sales_order_number` | string | Source order number, degenerate dimension |
| `ship_to_customer_sk` | bigint | Foreign key to `dim_customer` |
| `shipping_carrier` | string | Maersk, DHL, UPS, DSV or Kuehne+Nagel |
| `order_date` | date | Inherited from the order |
| `ship_date` | date | Date the consignment left the warehouse |
| `delivery_date` | date | Date delivered. Null while in transit, around 20% of rows |
| `order_date_id` | int | Date key, `yyyyMMdd` |
| `ship_date_id` | int | Date key, `yyyyMMdd` |
| `delivery_date_id` | int | Date key, `yyyyMMdd` |
| `order_to_ship_days` | int | Days from order to despatch |
| `transit_days` | int | Days from despatch to delivery. Null while in transit |
| `order_to_delivery_days` | int | Total cycle time. Null while in transit |
| `is_delivered` | boolean | True once a delivery date exists |
| `delivery_status` | string | Delivered or In transit |
| `shipment_count_on_order` | int | Total consignments on the parent order |
| `shipment_sequence` | int | Position of this consignment within the order |
| `is_split_shipment` | boolean | True where the order shipped in more than one consignment |
| `batch_id` | string | Batch that delivered the row |

**Notes.** Null transit days are correct and should not be coalesced to zero, or average transit time drops without explanation.

### fact_campaign

One row per campaign per day.

| Column | Type | Description |
|---|---|---|
| `campaign_sk` | bigint | Foreign key to `dim_campaign` |
| `campaign_log_date` | date | The day being reported |
| `campaign_log_date_id` | int | Date key, `yyyyMMdd` |
| `campaign_impressions` | bigint | Impressions served |
| `campaign_clicks` | bigint | Clicks recorded |
| `campaign_spend` | decimal | Spend for the day |

**Notes.** Ratios such as click-through rate and cost per click are not stored, because they do not aggregate. Derive them in the reporting layer from the components.

---

## Bridge

### bridge_campaign_product

Resolves the many-to-many relationship between campaigns and products. A campaign covers several SKUs and a SKU appears in several campaigns.

| Column | Type | Description |
|---|---|---|
| `campaign_sk` | bigint | Foreign key to `dim_campaign` |
| `product_sk` | bigint | Foreign key to `dim_product` |

**Notes.** Joining a measure through this bridge fans out. A SKU in three campaigns produces three rows, so campaign-level revenue would treble. Apportion the measure or restrict the query to one campaign.

---

## Silver layer

Cleaned and conformed, one table per source. Every table carries `batch_id`, `source_file`, `ingestion_timestamp`, `created_timestamp` and `updated_timestamp`.

| Table | Grain | Merged on |
|---|---|---|
| `sales_order` | Order | `order_number` |
| `sales_order_lines` | Order line | `order_line_id` |
| `invoice` | Invoice | `invoice_number` |
| `invoice_lines` | Invoice line | `invoice_number` + `line_number` |
| `payment` | Payment | `payment_id` |
| `shipment` | Shipment | `shipment_id` |
| `cust_master` | Customer | `customer_id` |
| `address` | Customer and address type | `customer_id` + `address_type` |
| `customer_contacts` | Customer | `customer_id` |
| `products` | SKU | `product_sku` |
| `subcategories` | Subcategory | `sub_category_id` |
| `cities` | City | `city_id` |
| `regions` | Region | `region_id` |
| `channels` | Channel | `channel_code` |
| `campaign_log` | Campaign and day | `campaign_id` + `log_date` |
| `campaign_sku` | Campaign and SKU | `campaign_id` + `sku` |
| `exchange_rates` | Currency and month | `currency` + `rate_month` |
| `sales_targets` | Region and month | `region_id` + `target_month` |
| `user_details` | User | `user_id` |

## Bronze layer

Source data as delivered, with provenance attached. Column names and types match the source. Every table adds:

| Column | Type | Description |
|---|---|---|
| `batch_id` | string | Batch folder the row came from |
| `source_file` | string | Full path of the physical file |
| `ingestion_timestamp` | timestamp | When the row was loaded |

Tables are partitioned by `batch_id` and written with `replaceWhere`, so reprocessing a batch replaces it rather than appending.
