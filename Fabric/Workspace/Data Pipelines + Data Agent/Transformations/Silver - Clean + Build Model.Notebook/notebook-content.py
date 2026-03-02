# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4729f13e-b56f-447a-a933-80a01e64e6cd",
# META       "default_lakehouse_name": "Silver_Model",
# META       "default_lakehouse_workspace_id": "0b45d13d-cfda-4a6b-a6b7-a712c6d98204",
# META       "known_lakehouses": [
# META         {
# META           "id": "4729f13e-b56f-447a-a933-80a01e64e6cd"
# META         },
# META         {
# META           "id": "5b73579d-beed-4fdd-b3b2-8b2d8c1bc565"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Build Conformed Model + Clean

# CELL ********************

# ===========================
# SILVER NOTEBOOK (Silver_Model)
# Idempotent build: safe to re-run and get the same results.
# Reads from Bronze, builds dims/facts for reporting
# Writes to: `<CATALOG>`.`Silver_Model`.dbo.<table>
# Expects variables `bronze` and `silver` with fully-qualified namespaces
#   defined in another cell, e.g.:
#   bronze = "`Fabric - Data & AI in Action`.`Bronze_Ingest`.dbo"
#   silver = "`Fabric - Data & AI in Action`.`Silver_Model`.dbo"
# ===========================

from pyspark.sql import functions as F, Window

# -----------------------------------
# CONFIG (fallbacks only if not defined elsewhere)
# -----------------------------------
try:
    bronze
except NameError:
    CATALOG   = "Fabric - Data & AI in Action"
    BRONZE_DB = "Bronze_Ingest"
    SCHEMA    = "dbo"
    bronze    = f"`{CATALOG}`.`{BRONZE_DB}`.{SCHEMA}"

try:
    silver
except NameError:
    CATALOG   = "Fabric - Data & AI in Action"
    SILVER_DB = "Silver_Model"
    SCHEMA    = "dbo"
    silver    = f"`{CATALOG}`.`{SILVER_DB}`.{SCHEMA}"

# -----------------------------------
# CLEAN REBUILD: drop Silver tables to ensure identical results on re-run
# -----------------------------------
for t in [
    "dim_product", "dim_store", "dim_date",
    "fact_sales_item", "fact_basket_product", "fact_basket_summary",
    "fact_product_counts", "kx_total_baskets"
]:
    spark.sql(f"DROP TABLE IF EXISTS {silver}.`{t}`")

# -----------------------------------
# LOAD BRONZE TABLES
# -----------------------------------
orders_bronze       = spark.table(f"{bronze}.orders")
order_items_bronze  = spark.table(f"{bronze}.order_items")
products_bronze     = spark.table(f"{bronze}.products")
prod_cats_bronze    = spark.table(f"{bronze}.product_categories")

# -----------------------------------
# Keys & stable columns
# -----------------------------------
order_id_col   = "order_id"
oi_order_id    = "order_id"
product_id_col = "product_id"
qty_col        = "amount"
unit_price_col = "base_price"
store_col      = "location"

# -----------------------------------
# DEDUP & normalize (deterministic)
# -----------------------------------
orders   = orders_bronze.dropDuplicates([order_id_col])
items    = order_items_bronze.dropDuplicates([oi_order_id, product_id_col])
products = products_bronze.dropDuplicates([product_id_col])

items    = items.withColumn(qty_col, F.col(qty_col).cast("double"))
products = products.withColumn(unit_price_col, F.col(unit_price_col).cast("double"))

# -----------------------------------
# DIM: PRODUCT (with category + brand)
#   NOTE: avoid volatile timestamps to ensure re-runs are identical.
# -----------------------------------
dim_product = (
    products.alias("p")
    .join(prod_cats_bronze.alias("c"), on="product_id", how="left")
    .select(
        F.col("p.product_id"),
        F.col("p.name").alias("product_name"),
        F.col("c.category").alias("category"),
        F.col("p.brand").alias("brand"),
        F.lit(True).alias("is_active"),
        F.lit(None).cast("timestamp").alias("effective_ts")  # stable (non-volatile)
    )
)
# Use overwrite to keep idempotent, allow schema overwrite to be safe across evolutions
dim_product.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{silver}.dim_product")

# -----------------------------------
# DIM: STORE (location -> region + channel)
# -----------------------------------
region_map = F.create_map(
    F.lit("Cologne"),    F.lit("NRW"),
    F.lit("Berlin"),     F.lit("Berlin"),
    F.lit("Munich"),     F.lit("Bavaria"),
    F.lit("Hamburg"),    F.lit("Hamburg"),
    F.lit("Metzingen"),  F.lit("Baden-Württemberg"),
    F.lit("Online"),     F.lit("Online")
)

dim_store = (
    orders
    .select(F.col(store_col).alias("store_id"))
    .where(F.col("store_id").isNotNull())
    .dropDuplicates()
    .withColumn("region", region_map.getItem(F.col("store_id")))
    .withColumn("channel", F.when(F.col("store_id") == F.lit("Online"), F.lit("online")).otherwise(F.lit("offline")))
)
dim_store.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.dim_store")

# -----------------------------------
# DIM: DATE from order_id (OYYYYMMDD-xxxxx) — deterministic
# -----------------------------------
date_str = F.regexp_extract(F.col(order_id_col), r"^O(\d{8})-", 1)
dt_col   = F.to_date(date_str, "yyyyMMdd")

dim_date = (
    orders
    .select(dt_col.alias("date"))
    .where(F.col("date").isNotNull())
    .dropDuplicates()
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",    F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month",   F.month("date"))
    .withColumn("day",     F.dayofmonth("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
)
dim_date.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.dim_date")

# -----------------------------------
# FACT: SALES ITEM (line-level; category + brand)
# -----------------------------------
fact_sales_item = (
    items.alias("i")
    .join(products.select("product_id", unit_price_col, "brand").alias("p"), "product_id", "left")
    .join(prod_cats_bronze.select("product_id", "category").alias("c"), "product_id", "left")
    .select(
        F.col("i.order_id").alias("order_id"),
        F.col("i.product_id").alias("product_id"),
        F.col(qty_col).alias("qty"),
        F.col(unit_price_col).alias("unit_price"),
        (F.col(qty_col) * F.col(unit_price_col)).alias("line_value"),
        F.col("c.category").alias("category"),
        F.col("p.brand").alias("brand")
    )
)
fact_sales_item.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.fact_sales_item")

# -----------------------------------
# FACT: BASKET -> PRODUCT
# -----------------------------------
fact_basket_product = (
    items
    .select(F.col(oi_order_id).alias("order_id"), F.col(product_id_col).alias("product_id"))
    .dropDuplicates()
)
fact_basket_product.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.fact_basket_product")

# -----------------------------------
# FACT: BASKET SUMMARY (includes user_id, promo_flag, promo_code)
# -----------------------------------
basket_size_df = (
    fact_basket_product.groupBy("order_id").agg(F.countDistinct("product_id").alias("basket_size"))
)

qty_df = (
    fact_sales_item.groupBy("order_id").agg(F.sum("qty").alias("basket_qty"))
)
value_df = (
    fact_sales_item.groupBy("order_id").agg(F.sum("line_value").alias("basket_value"))
)

orders_sel = (
    orders
    .select(
        F.col(order_id_col).alias("order_id"),
        F.col("user_id"),
        dt_col.alias("order_date"),
        F.col(store_col).cast("string").alias("store_id"),
        F.col("promo_flag").cast("boolean").alias("promo_flag"),
        F.col("promo_code").cast("string").alias("promo_code")
    )
)

fact_basket_summary = (
    orders_sel
    .join(basket_size_df, "order_id", "left")
    .join(qty_df,         "order_id", "left")
    .join(value_df,       "order_id", "left")
)
fact_basket_summary.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.fact_basket_summary")

# -----------------------------------
# SUPPORT: product counts, total baskets
# -----------------------------------
fact_product_counts = (
    fact_basket_product.groupBy("product_id").agg(F.countDistinct("order_id").alias("basket_cnt"))
)
fact_product_counts.write.mode("overwrite").format("delta").saveAsTable(f"{silver}.fact_product_counts")

total_baskets = fact_basket_product.select(F.countDistinct("order_id").alias("n")).collect()[0]["n"]
spark.createDataFrame([(int(total_baskets),)], "n int") \
    .write.mode("overwrite").format("delta").saveAsTable(f"{silver}.kx_total_baskets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
