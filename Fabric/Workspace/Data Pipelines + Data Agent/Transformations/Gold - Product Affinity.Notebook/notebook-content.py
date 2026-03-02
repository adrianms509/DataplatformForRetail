# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f6bb600-4dbb-4333-94af-85a72d865a64",
# META       "default_lakehouse_name": "Gold_Consume",
# META       "default_lakehouse_workspace_id": "0b45d13d-cfda-4a6b-a6b7-a712c6d98204",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f6bb600-4dbb-4333-94af-85a72d865a64"
# META         },
# META         {
# META           "id": "4729f13e-b56f-447a-a933-80a01e64e6cd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ===========================
# GOLD NOTEBOOK (Gold_Consume)
# Idempotent build: safe to re-run and get the same results.
# Reads from Silver, computes:
#   - Product Affinity (uplift) tables
#   - AOV, Top products/category, Regional split, Promo shares
# Writes to: `<CATALOG>`.`Gold_Consume`.dbo.<table>
# Expects variables `silver` and `gold` with fully-qualified namespaces
#   defined in another cell, e.g.:
#   silver = "`Fabric - Data & AI in Action`.`Silver_Model`.dbo"
#   gold   = "`Fabric - Data & AI in Action`.`Gold_Consume`.dbo"
# ===========================

from pyspark.sql import functions as F, Window

# -----------------------------------
# CONFIG (fallbacks only if not defined elsewhere)
# -----------------------------------
try:
    silver
except NameError:
    CATALOG    = "Fabric - Data & AI in Action"
    SILVER_DB  = "Silver_Model"
    SCHEMA     = "dbo"
    silver     = f"`{CATALOG}`.`{SILVER_DB}`.{SCHEMA}"

try:
    gold
except NameError:
    CATALOG    = "Fabric - Data & AI in Action"
    GOLD_DB    = "Gold_Consume"
    SCHEMA     = "dbo"
    gold       = f"`{CATALOG}`.`{GOLD_DB}`.{SCHEMA}"

# -----------------------------------
# CLEAN REBUILD: drop Gold tables to ensure identical results on re-run
# -----------------------------------
for t in [
    # Affinity / Uplift
    "fact_product_affinity",
    "fact_product_affinity_topk",
    "product_affinity_topk_named",
    # KPI & summary tables
    "sales_aov_overall",
    "sales_aov_by_region_channel",
    "sales_aov_by_brand",
    "sales_top_products_by_category",
    "sales_regional_split",
    "sales_promo_share_orders",
    "sales_promo_share_users",
    "sales_promo_share_by_channel",
]:
    spark.sql(f"DROP TABLE IF EXISTS {gold}.`{t}`")

# -----------------------------------
# Parameters for affinity & filters
# -----------------------------------
min_pair_cnt     = 5           # filter rare co-occurrences
top_k            = 10          # Top-K per LHS by lift (then confidence, then pair_cnt)

# Optional window filters (set to None/[] to disable)
filter_start_date = None       # e.g., "2025-01-01"
filter_end_date   = None       # e.g., "2025-12-31"
store_ids_filter  = []         # e.g., ["Cologne","Online"]
promo_context     = None       # None | "promo_only" | "non_promo_only"

# -----------------------------------
# Load Silver
# -----------------------------------
dim_product         = spark.table(f"{silver}.dim_product")
dim_store           = spark.table(f"{silver}.dim_store")
fact_sales_item     = spark.table(f"{silver}.fact_sales_item")
fact_basket_product = spark.table(f"{silver}.fact_basket_product")
fact_basket_summary = spark.table(f"{silver}.fact_basket_summary")

# ===========================================================
# PART A — PRODUCT AFFINITY (UPLIFT)  <-- restored
# ===========================================================

# Start from distinct items per order
basket_items = fact_basket_product.select("order_id", "product_id").dropDuplicates()

# Apply optional filters by joining order context
need_join = any([filter_start_date, filter_end_date, store_ids_filter, promo_context])
if need_join:
    joined = basket_items.join(
        fact_basket_summary.select("order_id", "order_date", "store_id", "promo_flag"),
        on="order_id",
        how="inner"
    )
    if filter_start_date:
        joined = joined.where(F.col("order_date") >= F.to_date(F.lit(filter_start_date)))
    if filter_end_date:
        joined = joined.where(F.col("order_date") <= F.to_date(F.lit(filter_end_date)))
    if store_ids_filter:
        joined = joined.where(F.col("store_id").isin(store_ids_filter))
    if promo_context == "promo_only":
        joined = joined.where(F.col("promo_flag") == True)
    elif promo_context == "non_promo_only":
        joined = joined.where((F.col("promo_flag") == False) | F.col("promo_flag").isNull())

    basket_items = joined.select("order_id", "product_id").dropDuplicates()

# Totals & per-item counts in the filtered window
total_baskets = basket_items.select(F.countDistinct("order_id").alias("n")).collect()[0]["n"]

if total_baskets == 0:
    # Write empty tables to keep downstream visuals happy
    empty_aff_schema = (
        "lhs_item_id string, rhs_item_id string, pair_cnt long, "
        "lhs_cnt long, rhs_cnt long, "
        "support_lhs double, support_rhs double, support_pair double, "
        "confidence_lhs_to_rhs double, lift double, refreshed_at timestamp"
    )
    empty_df = spark.createDataFrame([], empty_aff_schema)
    empty_df.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.fact_product_affinity")
    empty_df.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.fact_product_affinity_topk")

    empty_named_schema = (
        "lhs_item_id string, lhs_name string, rhs_item_id string, rhs_name string, "
        "pair_cnt long, support_pair double, confidence_lhs_to_rhs double, lift double, refreshed_at timestamp"
    )
    spark.createDataFrame([], empty_named_schema).write.mode("overwrite").format("delta").saveAsTable(f"{gold}.product_affinity_topk_named")
else:
    # Counts per product
    item_counts = (
        basket_items
        .groupBy("product_id")
        .agg(F.countDistinct("order_id").alias("cnt"))
        .cache()
    )

    # Pairwise co-occurrence counts (unordered)
    pairs_unordered = (
        basket_items.alias("a")
        .join(basket_items.alias("b"), on="order_id")
        .where(F.col("a.product_id") < F.col("b.product_id"))  # unordered, no self-pair, no duplicates
        .groupBy(
            F.col("a.product_id").alias("prod1"),
            F.col("b.product_id").alias("prod2")
        )
        .agg(F.countDistinct("a.order_id").alias("pair_cnt"))
        .cache()
    )

    # Expand to directional pairs
    pairs_directed = (
        pairs_unordered
        .select(
            F.col("prod1").alias("lhs_item_id"),
            F.col("prod2").alias("rhs_item_id"),
            F.col("pair_cnt")
        )
        .unionByName(
            pairs_unordered.select(
                F.col("prod2").alias("lhs_item_id"),
                F.col("prod1").alias("rhs_item_id"),
                F.col("pair_cnt")
            )
        )
    )

    # Join counts and compute metrics
    affinity = (
        pairs_directed
        .join(item_counts.withColumnRenamed("product_id", "lhs_item_id"), on="lhs_item_id", how="left")
        .withColumnRenamed("cnt", "lhs_cnt")
        .join(item_counts.withColumnRenamed("product_id", "rhs_item_id"), on="rhs_item_id", how="left")
        .withColumnRenamed("cnt", "rhs_cnt")
        .withColumn("support_lhs",  F.col("lhs_cnt").cast("double")  / F.lit(total_baskets))
        .withColumn("support_rhs",  F.col("rhs_cnt").cast("double")  / F.lit(total_baskets))
        .withColumn("support_pair", F.col("pair_cnt").cast("double") / F.lit(total_baskets))
        .withColumn(
            "confidence_lhs_to_rhs",
            F.when(F.col("lhs_cnt") > 0, F.col("pair_cnt").cast("double") / F.col("lhs_cnt")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "lift",
            F.when(
                (F.col("lhs_cnt") > 0) & (F.col("rhs_cnt") > 0),
                (F.col("pair_cnt").cast("double") / F.col("lhs_cnt")) / (F.col("rhs_cnt").cast("double") / F.lit(total_baskets))
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn("refreshed_at", F.current_timestamp())
    )

    # Filter rare pairs
    affinity_filtered = affinity.where(F.col("pair_cnt") >= F.lit(min_pair_cnt))

    # Top-K per LHS
    w = Window.partitionBy("lhs_item_id").orderBy(F.desc("lift"), F.desc("confidence_lhs_to_rhs"), F.desc("pair_cnt"))
    affinity_topk = (
        affinity_filtered
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") <= F.lit(top_k))
        .drop("rn")
    )

    # Persist to Gold
    affinity_filtered.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.fact_product_affinity")
    affinity_topk.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.fact_product_affinity_topk")

    # Convenience table with names for demos
    products_names = dim_product.select(
        F.col("product_id"),
        F.col("product_name")
    )

    topk_named = (
        affinity_topk
        .join(
            products_names.withColumnRenamed("product_id", "lhs_item_id")
                          .withColumnRenamed("product_name", "lhs_name"),
            on="lhs_item_id", how="left"
        )
        .join(
            products_names.withColumnRenamed("product_id", "rhs_item_id")
                          .withColumnRenamed("product_name", "rhs_name"),
            on="rhs_item_id", how="left"
        )
        .select(
            "lhs_item_id", "lhs_name", "rhs_item_id", "rhs_name",
            "pair_cnt", "support_pair", "confidence_lhs_to_rhs", "lift", "refreshed_at"
        )
    )
    topk_named.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.product_affinity_topk_named")

    # Cleanup caches
    item_counts.unpersist()
    pairs_unordered.unpersist()

# ===========================================================
# PART B — KPI & SUMMARY TABLES (AOV, Top products, Regional, Promo)
# ===========================================================

# 1) Average Order Value (AOV)
aov_overall = fact_basket_summary.select(F.avg("basket_value").alias("aov"))
aov_overall.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_aov_overall")

orders_enriched = (
    fact_basket_summary.alias("o")
    .join(dim_store.alias("s"), F.col("o.store_id") == F.col("s.store_id"), "left")
)

aov_by_seg = (
    orders_enriched
    .groupBy("region", "channel")
    .agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("basket_value").alias("revenue"),
        F.avg("basket_value").alias("aov")
    )
)
aov_by_seg.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_aov_by_region_channel")

# Also by brand (compute per-order brand value, then avg)
order_brand_rev = (
    fact_sales_item.groupBy("order_id", "brand")
    .agg(F.sum("line_value").alias("brand_order_value"))
)
aov_by_brand = (
    order_brand_rev.groupBy("brand")
    .agg(F.avg("brand_order_value").alias("aov_brand"))
)
aov_by_brand.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_aov_by_brand")

# 2) Top-Selling Products / Category
prod_sales = (
    fact_sales_item.select("product_id", "qty", "line_value").alias("f")
    .join(
        dim_product.select(
            F.col("product_id").alias("dp_product_id"),
            "product_name",
            F.col("category").alias("dp_category"),
            F.col("brand").alias("dp_brand")
        ).alias("p"),
        on=F.col("f.product_id") == F.col("p.dp_product_id"),
        how="left"
    )
    .groupBy(
        F.col("f.product_id").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.dp_category").alias("category"),
        F.col("p.dp_brand").alias("brand")
    )
    .agg(
        F.sum(F.col("f.qty")).alias("qty_sold"),
        F.sum(F.col("f.line_value")).alias("revenue")
    )
)

w_cat = Window.partitionBy("category").orderBy(F.desc("revenue"), F.desc("qty_sold"), F.asc("product_name"))
topN = 10
top_products_by_category = (
    prod_sales
    .withColumn("rank_in_category", F.row_number().over(w_cat))
    .where(F.col("rank_in_category") <= F.lit(topN))
)
top_products_by_category.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_top_products_by_category")

# 3) Regional split (Regions, online/offline, brands)
regional_split = (
    fact_sales_item.alias("f")
    .join(fact_basket_summary.select("order_id", "store_id").alias("o"), "order_id", "left")
    .join(dim_store.alias("s"), "store_id", "left")
    .groupBy("s.region", "s.channel", "f.brand")
    .agg(
        F.countDistinct("o.order_id").alias("orders"),
        F.sum("f.qty").alias("qty"),
        F.sum("f.line_value").alias("revenue")
    )
)
regional_split.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_regional_split")

# 4) Promo share (orders + unique users; also by channel)
promo_orders = orders_enriched.agg(
    F.sum(F.when(F.col("promo_flag") == True, 1).otherwise(0)).alias("orders_with_promo"),
    F.countDistinct("order_id").alias("orders_total")
).select(
    (F.col("orders_with_promo") / F.col("orders_total")).alias("promo_share_orders")
)
promo_orders.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_promo_share_orders")

promo_users = (
    orders_enriched
    .groupBy("user_id")
    .agg(F.max(F.when(F.col("promo_flag") == True, 1).otherwise(0)).alias("ever_promo"))
    .agg(
        F.sum("ever_promo").alias("users_with_promo"),
        F.count("*").alias("users_total")
    )
    .select((F.col("users_with_promo") / F.col("users_total")).alias("promo_share_users"))
)
promo_users.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_promo_share_users")

promo_by_channel = (
    orders_enriched
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("promo_flag") == True, 1).otherwise(0)).alias("orders_with_promo"),
        F.countDistinct("order_id").alias("orders_total"),
        (F.sum(F.when(F.col("promo_flag") == True, 1).otherwise(0)) / F.countDistinct("order_id")).alias("promo_share_orders")
    )
)
promo_by_channel.write.mode("overwrite").format("delta").saveAsTable(f"{gold}.sales_promo_share_by_channel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
