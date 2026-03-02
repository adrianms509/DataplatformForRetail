# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b73579d-beed-4fdd-b3b2-8b2d8c1bc565",
# META       "default_lakehouse_name": "Bronze_Ingest",
# META       "default_lakehouse_workspace_id": "0b45d13d-cfda-4a6b-a6b7-a712c6d98204",
# META       "known_lakehouses": [
# META         {
# META           "id": "5b73579d-beed-4fdd-b3b2-8b2d8c1bc565"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Generate Data

# MARKDOWN ********************

# ### Imports

# MARKDOWN ********************

# ### Demodata

# CELL ********************

# ----------------------------
# Categories (non-jackets) with German materials
# ----------------------------

# Minimal categories: ONLY size maps for inventory by category
categories = {
    "shoes":   {"sizes": [str(s) for s in range(39, 47)]},           # EU 39–46
    "trousers":{"sizes": ["29/30","30/32","31/32","31/34","32/30","32/32","32/34",
                          "33/32","33/34","34/32","34/34","36/32","38/32","40/34","42/34"]},
    "tshirts": {"sizes": ["XS", "S", "M", "L", "XL", "XXL", "XXXL"]},
    "jackets": {"sizes": ["S", "M", "L", "XL", "XXL"]},
}

# ----------------------------
# Curated 20 Jacket Titles: German materials, prices, colors
# ----------------------------
jacket_catalog = [


    {
        "title": "Jacke mit kontrastierendem Einsatz",
        "material": "Polyester",
        "price": 199.00,
        "colors": ["Grau", "Schwarz", "Dunkelblau"]
    },
    {
        "title": "Steppmantel mit Kapuze",
        "material": "Polyester",
        "price": 249.00,
        "colors": ["Dunkelblau", "Schwarz", "Olive"]
    },
    {
        "title": "Jacke aus gestepptem Satin",
        "material": "Polyester-Satin",
        "price": 169.00,
        "colors": ["Dunkelblau", "Schwarz"]
    },
    {
        "title": "Relaxed-Fit Kapuzenjacke aus  Material-Mix",
        "material": "Material-Mix (Polyester/Baumwolle)",
        "price": 124.00,
        "colors": ["Schwarz", "Olive"]
    },
    {
        "title": "Steppjacke mit Logo-Patch",
        "material": "Polyester",
        "price": 139.00,
        "colors": ["Dunkelrot", "Schwarz", "Hellbraun"]
    },
    {
        "title": "Jacke mit Double-B-Monogramm",
        "material": "Polyester-Jacquard",
        "price": 179.00,
        "colors": ["Beige", "Dunkelblau", "Schwarz"]
    },
    {
        "title": "Jacke mit Kapuzenschirm",
        "material": "Polyester",
        "price": 169.00,
        "colors": ["Dunkelblau", "Hellgrün"]
    },
    {
        "title": "Daunenjacke mit Brusttaschen",
        "material": "Daunen",
        "price": 279.00,
        "colors": ["Dunkelblau", "Schwarz"]
    },
    {
        "title": "Daunenjacke mit abnehmbarer Kapuze",
        "material": "Daunen",
        "price": 279.00,
        "colors": ["Hellgrün", "Schwarz"]
    },
    {
        "title": "Jacke aus angerauter Mikrofaser",
        "material": "Mikrofaser (Synthetik)",
        "price": 359.00,
        "colors": ["Dunkelbraun"]
    },
    {
        "title": "Parka mit Daunenfüllung",
        "material": "Daunen",
        "price": 419.00,
        "colors": ["Schwarz", "Olive", "Dunkelblau"]
    },
    {
        "title": "Wasserdichter GORE-TEX® Windbreaker mit innenliegender Reißverschlusstasche",
        "material": "GORE-TEX",
        "price": 379.00,
        "colors": ["Schwarz"]
    },
    {
        "title": "Kapuzenjacke mit reflektierenden Details",
        "material": "Polyester",
        "price": 139.00,
        "colors": ["Schwarz", "Grün"]
    },
    {
        "title": "Steppjacke mit Logo-Print",
        "material": "Polyester",
        "price": 139.00,
        "colors": ["Schwarz"]
    },
    {
        "title": "Jacke mit Jacquard-Monogrammen",
        "material": "Polyester-Jacquard",
        "price": 319.00,
        "colors": ["Hellbraun", "Dunkelblau", "Schwarz"]
    },
    {
        "title": "Jacke aus  Gewebe mit Kapuze",
        "material": "Polyester",
        "price": 239.00,
        "colors": ["Dunkelblau"]
    },
    {
        "title": "Active Kapuzenparka mit Allover-Print",
        "material": "Polyester",
        "price": 179.00,
        "colors": ["Dunkelblau"]
    },
    {
        "title": "Verstaubare Jacke aus leichtem Ripstop-Gewebe mit  Finish",
        "material": "Nylon-Ripstop",
        "price": 119.00,
        "colors": ["Grau"]
    },
    {
        "title": "Bomberjacke mit Monogramm-Jacquard",
        "material": "Polyester-Jacquard",
        "price": 189.00,
        "colors": ["Natur"]
    },
    {
        "title": "Steppjacke aus schimmerndem Gewebe",
        "material": "Polyester",
        "price": 169.00,
        "colors": ["Rot", "Schwarz"]
    },
]

shoe_catalog = [

    {"title": "Derbys aus Leder mit Nahtdetails", "material": "Leder", "price": 119.00, "colors": ["Schwarz", "Dunkelbraun", "Braun"]},
    {"title": "Sneakers aus Veloursleder mit Double-B-Monogramm", "material": "Veloursleder", "price": 119.00, "colors": ["Hellbeige", "Dunkelgrau"]},
    {"title": "Gary Sneakers aus genarbtem Leder mit charakteristischem Besatz", "material": "Genarbtes Leder", "price": 299.00, "colors": ["Weiß", "Dunkelblau", "Khaki"]},
    {"title": "TTNM EVO Sneakers mit gestricktem Obermaterial", "material": "Strick", "price": 159.00, "colors": ["Dunkelblau", "Dunkelrot", "Dunkelbraun"]},
    {"title": "Sneakers aus verschiedenen Materialien mit Kontrast-Ferse", "material": "Materialmix", "price": 79.00, "colors": ["Schwarz", "Dunkelblau"]},
    {"title": "Sneakers aus Leder mit Gummisohlen", "material": "Leder", "price": 159.00, "colors": ["Weiß", "Schwarz"]},
    {"title": "Sneakers aus Kunstleder mit unifarbener und genarbter Struktur", "material": "Kunstleder", "price": 114.00, "colors": ["Schwarz", "Weiß"]},
    {"title": "Schnür-Sneakers aus Kunstleder mit Logo-Details", "material": "Kunstleder", "price": 79.00, "colors": ["Schwarz"]},
    {"title": "Loafer aus Leder mit Pennyloafer-Besatz", "material": "Leder", "price": 210.00, "colors": ["Schwarz", "Dunkelrot"]},
    {"title": "Boots aus Leder mit Logo-Detail", "material": "Leder", "price": 230.00, "colors": ["Schwarz"]},
    {"title": "Sneakers aus Veloursleder mit Perforationen", "material": "Veloursleder", "price": 230.00, "colors": ["Dunkelbraun", "Beige"]},
    {"title": "Sneakers mit Schnürung und Kunstleder-Besatz", "material": "Materialmix (Kunstleder/Textil)", "price": 199.00, "colors": ["Schwarz", "Dunkelblau"]},
    {"title": "Derbys aus Leder mit geprägtem Logo", "material": "Leder", "price": 180.00, "colors": ["Schwarz"]},
    {"title": "Loafer aus Leder mit Pennyloafer-Bündchen", "material": "Leder", "price": 210.00, "colors": ["Dunkelbraun", "Schwarz"]},
    {"title": "Lowtop Sneakers aus Leder mit Logo-Prägung", "material": "Leder", "price": 199.00, "colors": ["Weiß", "Dunkelbraun", "Dunkelblau"]},
    {"title": "Sneakers aus Nappaleder mit goldfarbenem Branding", "material": "Nappaleder", "price": 199.00, "colors": ["Schwarz", "Weiß", "Dunkelblau"]},
    {"title": "Sneakers aus Nappaleder mit goldfarbenem Logo", "material": "Nappaleder", "price": 180.00, "colors": ["Schwarz", "Weiß"]},
    {"title": "Lowtop Sneakers mit strukturierten Details", "material": "Synthetik/Textil", "price": 160.00, "colors": ["Weiß", "Dunkelblau"]},
    {"title": "Hybrid-Boots aus Veloursleder und Canvas", "material": "Veloursleder/Canvas", "price": 210.00, "colors": ["Schwarz", "Khaki"]},
    {"title": "In Italien gefertigte Slides mit großem Logo-Detail", "material": "Synthetik", "price": 50.00, "colors": ["Schwarz"]},
]

tshirt_catalog = [
    {
        "title": "Zweierpack T-Shirts aus Stretch-Baumwolle mit V-Ausschnitt",
        "material": "Stretch-Baumwolle",
        "price": 42.00,
        "colors": ["Weiß", "Weiß/Schwarz", "Hellgrau"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Jersey mit Logo-Aufnäher",
        "material": "Baumwoll-Jersey",
        "price": 42.00,
        "colors": ["Schwarz", "Grau", "Weiß", "Dunkelblau", "Hellgrau"]
    },
    {
        "title": "Zweierpack T-Shirts aus Stretch-Baumwolle mit V-Ausschnitt",
        "material": "Stretch-Baumwolle",
        "price": 42.00,
        "colors": ["Schwarz", "Weiß", "Weiß/Schwarz", "Hellgrau"]
    },
    {
        "title": "T-Shirt aus Stretch-Baumwolle mit Streifen und Logo",
        "material": "Stretch-Baumwolle",
        "price": 35.00,
        "colors": ["Schwarz", "Grau", "Weiß"]
    },
    {
        "title": "Rollkragentop aus Baumwolle mit Logo",
        "material": "Baumwolle",
        "price": 48.00,
        "colors": ["Schwarz", "Weiß"]
    },
    {
        "title": "T-Shirt aus Stretch-Baumwolle mit Kontrast-Logo",
        "material": "Stretch-Baumwolle",
        "price": 28.00,
        "colors": ["Hellgrün", "Hellblau", "Gelb"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Mix mit kreisförmiger Jacquard-Struktur",
        "material": "Baumwoll-Mix",
        "price": 54.00,
        "colors": ["Natur", "Beige", "Hellbeige"]
    },
    {
        "title": "Sportives, feuchtigkeitsableitendes T-Shirt mit Piqué-Struktur",
        "material": "Polyester/Piqué",
        "price": 40.00,
        "colors": ["Weiß", "Dunkelblau", "Schwarz"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Jersey mit reflektierendem Logo",
        "material": "Baumwoll-Jersey",
        "price": 35.00,
        "colors": ["Schwarz", "Grün", "Dunkelblau"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Jersey mit Stack-Logo",
        "material": "Baumwoll-Jersey",
        "price": 30.00,
        "colors": ["Hellgrau", "Weiß", "Schwarz"]
    },
    {
        "title": "Zweierpack Slim-Fit T-Shirts aus Stretch-Baumwolle",
        "material": "Stretch-Baumwolle",
        "price": 69.95,  # Multipack (Preis ohne Rabatt angegeben in Liste)
        "colors": ["Weiß", "Weiß/Schwarz", "Hellgrau"]
    },
    {
        "title": "Relaxed-Fit T-Shirt aus Baumwolle mit Logo-Print",
        "material": "Baumwolle",
        "price": 30.00,
        "colors": ["Braun", "Rot", "Hellgrün", "Grün", "Olive"]
    },
    {
        "title": "BECKHAM x BOSS T-Shirt aus merzerisierter Baumwolle",
        "material": "Merzerisierte Baumwolle",
        "price": 94.00,
        "colors": ["Weiß", "Dunkelblau"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Jersey mit Dégradé-Effekt",
        "material": "Baumwoll-Jersey",
        "price": 40.00,
        "colors": ["Blau"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Jersey mit Dobermann-Print",
        "material": "Baumwoll-Jersey",
        "price": 36.00,
        "colors": ["Schwarz", "Weiß"]
    },
    {
        "title": "T-Shirt aus Stretch-Baumwolle mit Logo-Kragen",
        "material": "Stretch-Baumwolle",
        "price": 42.00,
        "colors": ["Weiß", "Schwarz"]
    },
    {
        "title": "T-Shirt aus Baumwoll-Mix mit Waffelstruktur",
        "material": "Baumwoll-Mix",
        "price": 54.00,
        "colors": ["Hellbeige", "Braun", "Hellgrün"]
    },
    {
        "title": "Regular-Fit T-Shirt aus Baumwoll-Jersey",
        "material": "Baumwoll-Jersey",
        "price": 59.95,  # im Feed ohne Rabatt ausgewiesen
        "colors": ["Schwarz", "Weiß", "Beige"]
    },
    {
        "title": "Schnell trocknendes Slim-Fit T-Shirt mit Vier-Wege-Stretch",
        "material": "Recycling-Polyester (NovaPoly)",
        "price": 99.95,
        "colors": ["Hellgrün", "Weiß", "Schwarz"]
    },
    {
        "title": "T-Shirt aus merzerisierter Baumwolle mit Hahnentritt-Jacquard",
        "material": "Merzerisierte Baumwolle",
        "price": 79.00,
        "colors": ["Blau"]
    },
]

trousers_catalog = [
    {"title": "Tapered-Fit Hose aus elastischem Baumwoll-Twill", "material": "Baumwoll-Twill", "price": 169.95, "colors": ["Beige"]},
    {"title": "Oversized-Fit Hose aus drapiertem Twill", "material": "Twill (drapiert)", "price": 229.00, "colors": ["Hellbeige", "Dunkelbraun"]},
    {"title": "Tapered-Fit Hose aus  Stretch-Gewebe", "material": "s Stretch-Gewebe", "price": 79.00, "colors": ["Dunkelblau", "Schwarz", "Grau"]},
    {"title": "Modern-Fit Hose aus Stretch-Baumwolle mit Seersucker-Struktur", "material": "Stretch-Baumwolle (Seersucker)", "price": 149.95, "colors": ["Schwarz"]},
    {"title": "Slim-Fit Chino aus elastischem Baumwoll-Satin", "material": "Baumwoll-Satin (elastisch)", "price": 69.00, "colors": ["Hellbeige", "Beige", "Hellgrün"]},
    {"title": "Denim-Chino aus weichem Baumwoll-Mix", "material": "Baumwoll-Mix (Denim)", "price": 59.00, "colors": ["Schwarz", "Dunkelblau"]},
    {"title": "Hose aus leichtem, bügelleichtem Gewebe", "material": "Leichtes, bügelleichtes Gewebe", "price": 69.00, "colors": ["Hellgrün", "Schwarz"]},
    {"title": "Slim-Fit Hose aus  Stretch-Gewebe (Dunkelgrau/Weiß/Natur)", "material": "s Stretch-Gewebe", "price": 84.00, "colors": ["Dunkelgrau", "Weiß", "Natur"]},
    {"title": "Modern-Fit Hose aus Stretch-Twill mit Bügelfalten", "material": "Stretch-Twill", "price": 119.95, "colors": ["Hellbraun", "Dunkelblau", "Dunkelrot"]},
    {"title": "Slim-Fit Hose aus  Stretch-Gewebe (Grün-Serie)", "material": "s Stretch-Gewebe", "price": 89.00, "colors": ["Grün", "Dunkelblau", "Hellblau"]},
    {"title": "Extra Slim-Fit Twill-Hose aus Woll-Mix", "material": "Woll-Mix (Twill)", "price": 149.95, "colors": ["Schwarz"]},
    {"title": "Oversized Hose mit filigranem Muster und Leinen", "material": "Leinen-Mix", "price": 299.00, "colors": ["Hellbeige"]},
    {"title": "Modern-Fit Hose aus Baumwoll-Mix mit Twill-Struktur", "material": "Baumwoll-Mix (Twill-Struktur)", "price": 169.95, "colors": ["Schwarz", "Dunkelblau"]},
    {"title": "Slim-Fit Hose aus Twill mit Tunnelzugbund", "material": "Twill (elastisch)", "price": 139.95, "colors": ["Schwarz"]},
    {"title": "Relaxed-Fit Cargo-Hose aus Baumwoll-Ripstop", "material": "Ripstop (Baumwolle)", "price": 129.95, "colors": ["Beige"]},
    {"title": "Modern-Fit Hose aus Stretch-Gewebe mit unterbrochenem Karo-Muster", "material": "Stretch-Gewebe (Karo)", "price": 179.95, "colors": ["Grau gemustert"]},
    {"title": "Hose aus Leinen-Mix mit Logo-Detail", "material": "Leinen-Mix", "price": 149.95, "colors": ["Dunkelblau", "Hellgrau"]},
    {"title": "Slim-Fit Hose aus strukturierter Stretch-Baumwolle", "material": "Stretch-Baumwolle (strukturiert)", "price": 159.95, "colors": ["Dunkelgrau", "Beige", "Dunkelblau"]},
    {"title": "Modern-Fit Hose aus elastischer Baumwoll-Gabardine", "material": "Gabardine (elastisch)", "price": 139.95, "colors": ["Blau", "Schwarz", "Hellgrau"]},
    {"title": "Relaxed-Fit Hose aus Ripstop-Gewebe", "material": "Ripstop", "price": 129.95, "colors": ["Schwarz", "Olive", "Dunkelblau"]},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters & Simulation Loop

# CELL ********************

# ===========================
# BRONZE NOTEBOOK (Bronze_Ingest)
# Uses pre-defined product catalogs & categories (in another cell)
# Drops existing Bronze tables, then recreates:
#   - products (with brand), product_categories, product_inventory
#   - orders (with 5% promo), order_items
# Writes to: `<CATALOG>`.`Bronze_Ingest`.dbo.<table>
# Expects a variable `bronze` with the fully-qualified namespace defined in another cell.
# ===========================

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import date, timedelta
import random

# -----------------------------------
# CONFIG (expects `bronze` already defined elsewhere)
# Fallback only if not defined (you can remove this try/except if you always set `bronze` yourself)
# -----------------------------------
try:
    bronze  # do not redefine if it already exists
except NameError:
    CATALOG   = "Fabric - Data & AI in Action"
    BRONZE_DB = "Bronze_Ingest"
    SCHEMA    = "dbo"
    bronze    = f"`{CATALOG}`.`{BRONZE_DB}`.{SCHEMA}"

# -----------------------------------
# Enable Delta schema evolution (safe default)
# -----------------------------------
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -----------------------------------
# HARD RESET: Drop existing Bronze tables (idempotent)
# -----------------------------------
for t in ["product_inventory", "product_categories", "products", "orders", "order_items"]:
    spark.sql(f"DROP TABLE IF EXISTS {bronze}.`{t}`")

# -----------------------------------
# SCHEMAS
# -----------------------------------
products_schema = T.StructType([
    T.StructField("product_id",  T.StringType(), False),
    T.StructField("name",        T.StringType(), False),
    T.StructField("material",    T.StringType(), True),
    T.StructField("base_price",  T.DoubleType(), True),
    T.StructField("color",       T.StringType(), True),
    T.StructField("brand",       T.StringType(), True),  # NEW
])

product_categories_schema = T.StructType([
    T.StructField("product_id", T.StringType(), False),
    T.StructField("category",   T.StringType(), False),
])

inventory_schema = T.StructType([
    T.StructField("product_id", T.StringType(), False),
    T.StructField("location",   T.StringType(), False),
    T.StructField("size",       T.StringType(), False),
    T.StructField("amount",     T.IntegerType(), False),
])

orders_schema = T.StructType([
    T.StructField("order_id",   T.StringType(), False),
    T.StructField("user_id",    T.StringType(), True),
    T.StructField("location",   T.StringType(), True),
    T.StructField("promo_flag", T.BooleanType(), True),   # NEW
    T.StructField("promo_code", T.StringType(), True),    # NEW
])

order_items_schema = T.StructType([
    T.StructField("order_id",   T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("amount",     T.IntegerType(), False),
    T.StructField("color",      T.StringType(), True),
])

# -----------------------------------
# HELPERS
# (Assumes the following are defined in another cell:
#   categories, jacket_catalog, shoe_catalog, tshirt_catalog, trousers_catalog)
# -----------------------------------
def gen_product_id(idx:int) -> str:
    return f"P{idx:04d}"

def infer_brand_from_title(title: str) -> str:
    """Detect brand in title; otherwise fallback 60/40 BOSS/HUGO."""
    t = (title or "").upper()
    if "BOSS" in t:
        return "BOSS"
    if "HUGO" in t:
        return "HUGO"
    return "BOSS" if random.random() < 0.60 else "HUGO"

def sample_stock():
    """Heavily skewed toward low stock, but still in [0..20]."""
    bucket = random.random()
    if bucket < 0.70:
        return random.randint(0, 7)     # mostly near 0–7
    elif bucket < 0.95:
        return random.randint(8, 14)    # sometimes mid
    else:
        return random.randint(15, 20)   # rarely high

# -----------------------------------
# PARAMETERS
# -----------------------------------
random_seed    = 42
days_back      = 90
orders_per_day = 1000
random.seed(random_seed)

# -----------------------------------
# Phase A — Build products from catalogs + inventory
# -----------------------------------
def generate_products_and_inventory():
    idx = 1
    products_py = []
    product_cats_py = []

    def add_from_catalog(catalog, category):
        nonlocal idx, products_py, product_cats_py
        for item in catalog:
            for clr in item["colors"]:
                pid = gen_product_id(idx)
                brand = infer_brand_from_title(item["title"])
                products_py.append({
                    "product_id": pid,
                    "name": item["title"],
                    "material": item["material"],
                    "base_price": float(item["price"]),
                    "color": clr,
                    "brand": brand,
                })
                product_cats_py.append({"product_id": pid, "category": category})
                idx += 1

    # Build from pre-defined catalogs (must be present in the session)
    add_from_catalog(jacket_catalog,   "jackets")
    add_from_catalog(shoe_catalog,     "shoes")
    add_from_catalog(tshirt_catalog,   "tshirts")
    add_from_catalog(trousers_catalog, "trousers")

    products_df = spark.createDataFrame(products_py, schema=products_schema)
    product_categories_df = spark.createDataFrame(product_cats_py, schema=product_categories_schema)

    # Persist to Bronze
    # Allow schema overwrite so 'brand' exists even on fresh/changed runs
    products_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{bronze}.products")
    product_categories_df.write.mode("overwrite").format("delta").saveAsTable(f"{bronze}.product_categories")

    # Inventory build
    locations = ["Cologne", "Metzingen", "Munich", "Hamburg", "Online"]
    inv_rows = []
    prod_with_cat = (
        products_df.join(product_categories_df, on="product_id", how="left")
                   .select("product_id", "category")
    )
    for r in prod_with_cat.collect():
        pid = r["product_id"]; cat = r["category"]
        for loc in locations:
            for sz in categories[cat]["sizes"]:
                amt = sample_stock()
                inv_rows.append({"product_id": pid, "location": loc, "size": sz, "amount": amt})

    product_inventory_df = spark.createDataFrame(inv_rows, schema=inventory_schema)
    product_inventory_df.write.mode("overwrite").format("delta").saveAsTable(f"{bronze}.product_inventory")

# -----------------------------------
# Phase B — Order simulation (category-aware; 5% promo)
# -----------------------------------
def simulate_orders():
    products  = spark.table(f"{bronze}.products")
    prod_cats = spark.table(f"{bronze}.product_categories")

    prod = (products.join(prod_cats, on="product_id", how="left")
                   .select("product_id", "name", "color", "category"))

    # Build category pick lists
    prod_groups = {}
    for row in prod.collect():
        prod_groups.setdefault(row["category"], []).append({
            "product_id": row["product_id"],
            "name": row["name"],
            "color": row["color"],
        })

    def pick_product_by_category(cat_key):
        return random.choice(prod_groups[cat_key])

    pair_biases = [
        ("trousers", "tshirts", 0.35),
        ("jackets",  "tshirts", 0.20),
        ("shoes",    "trousers", 0.15),
        ("shoes",    "jackets", 0.10),
    ]

    def ensure_distinct(items):
        seen = set(); out = []
        for it in items:
            if it["product_id"] not in seen:
                out.append(it); seen.add(it["product_id"])
        return out

    start_date = date.today() - timedelta(days=days_back)
    date_list = [start_date + timedelta(days=i) for i in range(days_back)]

    orders_rows, order_items_rows = [], []
    user_pool = [f"U{u:05d}" for u in range(1, 10001)]
    locations = ["Cologne", "Berlin", "Munich", "Hamburg", "Online"]

    def make_order_id(dt, n): return f"O{dt.strftime('%Y%m%d')}-{n:05d}"
    def rand_qty():
        x = random.random()
        if x < 0.85: return 1
        elif x < 0.97: return 2
        else: return 3

    cats = ["shoes", "trousers", "tshirts", "jackets"]
    promo_codes = ["WELCOME5", "INFLU5", "VIP5", "SOCIAL5"]

    for d in date_list:
        for n in range(orders_per_day):
            order_id = make_order_id(d, n+1)
            user_id = random.choice(user_pool)
            location = random.choice(locations)

            # 5% promo assignment
            is_promo = (random.random() < 0.05)
            promo_code = random.choice(promo_codes) if is_promo else None

            r = random.random()
            basket = []

            # Try biased combos first
            cum = 0.0; chosen = None
            for a_cat, b_cat, p in pair_biases:
                cum += p
                if r < cum:
                    chosen = (a_cat, b_cat); break

            if chosen:
                a_cat, b_cat = chosen
                basket.extend([pick_product_by_category(a_cat),
                               pick_product_by_category(b_cat)])
                if random.random() < 0.30:
                    basket.append(pick_product_by_category(random.choice(cats)))
            else:
                # Random basket 1–4 items
                k = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05], k=1)[0]
                for _ in range(k):
                    basket.append(pick_product_by_category(random.choice(cats)))

            basket = ensure_distinct(basket)
            if not basket:
                basket = [pick_product_by_category(random.choice(cats))]

            # Orders row (includes promo fields)
            orders_rows.append({
                "order_id": order_id,
                "user_id": user_id,
                "location": location,
                "promo_flag": is_promo,
                "promo_code": promo_code
            })

            # Items rows
            for item in basket:
                order_items_rows.append({
                    "order_id": order_id,
                    "product_id": item["product_id"],
                    "amount": rand_qty(),
                    "color": item["color"]
                })

    # Persist to Bronze
    orders_df = spark.createDataFrame(orders_rows, schema=orders_schema)
    order_items_df = spark.createDataFrame(order_items_rows, schema=order_items_schema)
    orders_df.write.mode("overwrite").format("delta").saveAsTable(f"{bronze}.orders")
    order_items_df.write.mode("overwrite").format("delta").saveAsTable(f"{bronze}.order_items")

# -----------------------------------
# Run phases (clean rebuild)
# -----------------------------------
generate_products_and_inventory()
simulate_orders()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM products.product_inventory LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
