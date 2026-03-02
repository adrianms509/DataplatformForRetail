# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "466c4a92-9730-9477-458a-9ba9d69f520b",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Demo Ingest combination finder

# MARKDOWN ********************

# ## Aquire credentials

# CELL ********************

from notebookutils import mssparkutils
from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData
import json, random, time, datetime as dt

# 1) Read secrets from Key Vault via the Fabric connection
KV_CONN = "YOUR Keyvault URI here"
TENANT_ID    = mssparkutils.credentials.getSecret(KV_CONN, "aad-tenant-id")
CLIENT_ID    = mssparkutils.credentials.getSecret(KV_CONN, "aad-client-id")
CLIENT_SECRET = mssparkutils.credentials.getSecret(KV_CONN, "aad-client-secret")


ES_FQDN_SALES = mssparkutils.credentials.getSecret(KV_CONN, "es-fqdn-sales")
ES_FQDN_SALES   = "esehamr9dzvlkih3dhx60r.servicebus.windows.net"

ES_NAME_SALES = mssparkutils.credentials.getSecret(KV_CONN, "es-name-sales")
ES_NAME_SALES   = "es_45b12718-d13f-49ec-96b6-362462708467"       

ES_FQDN_COMBINATIONS = mssparkutils.credentials.getSecret(KV_CONN, "es-fqdn-combinations")
ES_FQDN_COMBINATIONS   = "eseham25vehxkq6o8mvgv4.servicebus.windows.net"  

ES_NAME_COMBINATIONS = mssparkutils.credentials.getSecret(KV_CONN, "es-name-combinations")
ES_NAME_COMBINATIONS   = "es_7cf522f0-22a1-46c9-8c76-c6526cc7fb96"    

# 2) Create an AAD credential for the SP
cred = ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

# 3) Producer using AAD to the Event Hubs-compatible endpoint
sales_producer = EventHubProducerClient(
    fully_qualified_namespace=ES_FQDN_SALES,
    eventhub_name=ES_NAME_SALES,
    credential=cred
)

combinations_producer = EventHubProducerClient(
    fully_qualified_namespace=ES_FQDN_COMBINATIONS,
    eventhub_name=ES_NAME_COMBINATIONS,
    credential=cred
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Simulation Loop

# MARKDOWN ********************

# ### Config + Catalog

# CELL ********************

import copy

SOURCE_CATALOG = {
  "hosen": [
    {
      "id": "hosen-1",
      "name": "Jeans aus cremefarbenem Denim mit weitem Beinverlauf und Bundfalten auf der Vorderseite",
      "brand": "Boss",
      "price": 129.00,
      "originalPrice": 249.00,
      "color": "Creme",
      "image": "Jeans-aus-cremefarbenem-Denim-mit-weitem-Beinverlauf-und Bundfalten-auf-der-Vorderseite.jpg",
      "sizes": ["26/32", "27/32", "28/32", "29/32"]
    },
    {
      "id": "hosen-2",
      "name": "Blaue Slim-Fit Jeans aus bequemem Stretch-Denim",
      "brand": "Boss",
      "price": 199.95,
      "originalPrice": None,
      "color": "Blau",
      "image": "Blaue-Slim-Fit-Jeans-aus-bequemem-Stretch-Denim.jpg",
      "sizes": ["25/30", "25/32", "26/28", "26/30", "26/32", "27/28", "27/30", "27/32", "28/28", "28/30", "28/32", "29/30", "29/32", "30/30", "30/32"]
    },
    {
      "id": "hosen-3",
      "name": "Oversized Slim-Fit Hose aus strukturiertem Stretch-Gewebe",
      "brand": "Boss",
      "price": 149.00,
      "originalPrice": 249.00,
      "color": "Grau",
      "image": "Oversized-Slim-Fit-Hose-aus-strukturiertem-Stretch-Gewebe.jpg",
      "sizes": ["34", "36", "46"]
    },
    {
      "id": "hosen-4",
      "name": "Slim-Fit Hose aus elastischem Punto-Milano-Gewebe",
      "brand": "Boss",
      "price": 124.00,
      "originalPrice": 179.95,
      "color": "Beige",
      "image": "Slim-Fit-Hose-aus-elastischem-Punto-Milano-Gewebe.jpg",
      "sizes": ["34", "36", "38", "40", "42", "44", "46"]
    }
  ],
  "jacken": [
    {
      "id": "jacken-1",
      "name": "Wasserabweisende Jacke mit geraffter Taille",
      "brand": "Boss",
      "price": 319.00,
      "originalPrice": 449.00,
      "color": "Beige",
      "image": "Wasserabweisende-Jacke-mit-geraffter-Taille.jpg",
      "sizes": ["34", "36", "38", "40", "42" , "44", "48"]
    },
    {
      "id": "jacken-2",
      "name": "Bikerjacke aus genarbtem Leder mit Reißverschlüssen und Nieten",
      "brand": "Boss",
      "price": 319.00,
      "originalPrice": 449.00,
      "color": "Schwarz",
      "image": "Bikerjacke-aus-genarbtem-Leder-mit-Reißverschlüssen-und-Nieten.jpg",
      "sizes": ["32", "34", "36", "40" , "44"]
    },
    {
      "id": "jacken-3",
      "name": "Wasserabweisende, wattierte Reitsport-Bomberjacke",
      "brand": "Boss",
      "price": 229.00,
      "originalPrice": 289.00,
      "color": "Dunkelblau",
      "image": "Wasserabweisende-wattierte-Reitsport-Bomberjacke.jpg",
      "sizes": ["M", "L", "XL"]
    },
    {
      "id": "jacken-4",
      "name": "Ärmellose Jacke im Boxy-Fit aus indigoblauem Denim",
      "brand": "Boss",
      "price": 89.00,
      "originalPrice": 149.95,
      "color": "Indigoblau",
      "image": "Ärmellose-Jacke-im-Boxy-Fit-aus-indigoblauem-Denim.jpg",
      "sizes": ["M", "L", "XL"]
    }
  ],
  "pullover": [
    {
      "id": "pullover-1",
      "name": "T-Shirt aus Baumwoll-Jersey mit Logo-Detail",
      "brand": "BOSS",
      "price": 35.00,
      "originalPrice": 49.95,
      "color": "Koralle",
      "image": "t-shirt-baumwoll-jersey-mit-logo-detail.jpg",
      "sizes": ["XS", "S", "M", "L", "XL"]
    },
    {
      "id": "pullover-2",
      "name": "Ärmelloses Top aus Merinowolle mit Stehkragen",
      "brand": "BOSS",
      "price": 89.00,
      "originalPrice": 139.95,
      "color": "Rosa",
      "image": "Ärmelloses-Top-aus-Merinowolle-mit-Stehkragen.jpg",
      "sizes": ["S", "M", "L", "XXL"]
    },
    {
      "id": "pullover-3",
      "name": "T-Shirt aus merzerisierter Baumwolle mit Double-B-Monogramm",
      "brand": "BOSS",
      "price": 49.00,
      "originalPrice": 79.95,
      "color": "Rosa",
      "image": "T-Shirt-aus-merzerisierter-Baumwolle-mit-Double-B-Monogramm.jpg",
      "sizes": ["XS", "M"]
    },
    {
      "id": "pullover-4",
      "name": "Rollkragen-Longsleeve aus Stretch-Jersey",
      "brand": "BOSS",
      "price": 40.00,
      "originalPrice": 79.95,
      "color": "Schwarz",
      "image": "Rollkragen-Longsleeve-aus-Stretch-Jersey.jpg",
      "sizes": ["M"]
    }
  ],
  "roecke": [
    {
      "id": "roecke-1",
      "name": "A-Linien-Wickelrock aus Stretch-Baumwolle",
      "brand": "Boss",
      "price": 249.00,
      "originalPrice": None,
      "color": "Schwarz",
      "image": "A-Linien-Wickelrock-aus-Stretch-Baumwolle.jpg",
      "sizes": ["34", "36", "38", "40", "42", "44", "46"]
    },
    {
      "id": "roecke-2",
      "name": "Bleistiftrock aus Leder mit Bahnen",
      "brand": "Boss",
      "price": 319.00,
      "originalPrice": 349.00,
      "color": "Aubergine",
      "image": "Bleistiftrock-aus-Leder-mit-Bahnen.jpg",
      "sizes": ["36", "38", "44"]
    },
    {
      "id": "roecke-3",
      "name": "Slim-Fit Bleistiftrock aus in Italien gefertigter Schurwolle",
      "brand": "Boss",
      "price": 179.95,
      "originalPrice": None,
      "color": "Schwarz",
      "image": "Slim-Fit-Bleistiftrock-aus-in-Italien-gefertigter-Schurwolle.jpg",
      "sizes": ["32", "34", "36", "38", "40", "42", "44", "46"]
    },
    {
      "id": "roecke-4",
      "name": "Minirock aus Tweed mit Feder-Muster",
      "brand": "Boss",
      "price": 134.00,
      "originalPrice": 199.95,
      "color": "Bunt",
      "image": "Minirock-aus-Tweed-mit-Feder-Muster.jpg",
      "sizes": ["36", "38", "40", "42", "44", "46"]
    }
  ],
  "kleider": [
    {
      "id": "kleider-1",
      "name": "Wickelkleid aus Krepp-Georgette mit Schmetterlingsprint",
      "brand": "Boss",
      "price": 139.00,
      "originalPrice": 229.00,
      "color": "Bunt",
      "image": "Wickelkleid-aus-Krepp-Georgette-mit-Schmetterlingsprint.jpg",
      "sizes": ["32", "34", "36", "38", "40", "42", "44"]
    },
    {
      "id": "kleider-2",
      "name": "Kleid aus Schurwolle mit Paspeln am Bund",
      "brand": "Boss",
      "price": 349.00,
      "originalPrice": None,
      "color": "Burgundy",
      "image": "Kleid-aus-Schurwolle-mit-Paspeln-am-Bund.jpg",
      "sizes": ["32", "34", "36", "38", "40", "42", "44", "46"]
    },
    {
      "id": "kleider-3",
      "name": "Kleid aus Stretch-Jersey mit Schlüsselloch-Ausschnitt",
      "brand": "Boss",
      "price": 329.00,
      "originalPrice": None,
      "color": "Schwarz",
      "image": "Kleid-aus-Stretch-Jersey-mit-Schluesselloch-Ausschnitt.jpg",
      "sizes": ["32", "34", "36", "38", "40", "42", "44", "46"]
    },
    {
      "id": "kleider-4",
      "name": "Relaxed-Fit Hemdblusenkleid aus Stretch-Baumwolle",
      "brand": "Boss",
      "price": 179.00,
      "originalPrice": 229.00,
      "color": "Beige",
      "image": "Relaxed-Fit-Hemdblusenkleid-aus-Stretch-Baumwolle.jpg",
      "sizes": ["32", "34", "36", "38", "40", "42", "44", "46"]
    }
  ],
  "schuhe": [
    {
      "id": "schuhe-1",
      "name": "Sneakers aus Veloursleder mit Gummisohlen",
      "brand": "Boss",
      "price": 199.00,
      "originalPrice": None,
      "color": "Creme",
      "image": "Sneakers-aus-Veloursleder-mit-Gummisohlen.jpg",
      "sizes": ["35", "36", "37", "38", "39", "40", "41", "42"]
    },
    {
      "id": "schuhe-2",
      "name": "Mules mit offener Ferse aus Brush-Off-Leder",
      "brand": "Hugo",
      "price": 199.00,
      "originalPrice": None,
      "color": "Schwarz",
      "image": "Mules-mit-offener-Ferse-aus-Brush-Off-Leder.jpg",
      "sizes": ["36", "37", "38","39","40", "41"]
    },
    {
      "id": "schuhe-3",
      "name": "Boots aus Kunstleder mit Schließe am Riemen",
      "brand": "Hugo",
      "price": 159.00,
      "originalPrice": 230.00,
      "color": "Schwarz",
      "image": "Boots-aus-Kunstleder-mit-Schließe-am-Riemen.jpg",
      "sizes": ["35","36", "37", "38", "40", "42"]
    },
    {
      "id": "schuhe-4",
      "name": "Pumps aus Veloursleder mit komfortabler Innensohle",
      "brand": "Boss",
      "price": 399.00,
      "originalPrice": None,
      "color": "Creme",
      "image": "Pumps-aus-Veloursleder-mit-komfortabler-Innensohle.jpg",
      "sizes": ["36", "37", "37.5", "38", "39", "40", "41", "42", "43", "44", "45", "46"]
    },
    {
      "id": "schuhe-5",
      "name": "Sneakers aus Veloursleder mit Gummisohlen",
      "brand": "Boss",
      "price": 139.00,
      "originalPrice": 199.00,
      "color": "Orange",
      "image": "Sneakers-aus-Veloursleder-mit-Gummisohlen-Orange.jpg",
      "sizes": ["35", "36", "37", "38", "39", "40", "42"]
    },
    {
      "id": "schuhe-6",
      "name": "Kniehohe Boots aus Veloursleder und Leder",
      "brand": "Boss",
      "price": 550.00,
      "originalPrice": None,
      "color": "Creme",
      "image": "Kniehohe-Boots-aus-Veloursleder-und-Leder.jpg",
      "sizes": ["38", "38.5"]
    },
    {
      "id": "schuhe-7",
      "name": "Boots aus Veloursleder mit kubanischem Absatz",
      "brand": "Boss",
      "price": 299.00,
      "originalPrice": None,
      "color": "Schwarz",
      "image": "Boots-aus-Veloursleder-mit-kubanischem-Absatz.jpg",
      "sizes": ["36", "37", "38", "39", "40", "41", "42"]
    }
  ]
}

def _map_item(it, category):
    return {
        "product_id": it["id"],
        "name": it["name"],
        "brand": it.get("brand"),
        "price": float(it["price"]),
        "originalPrice": it.get("originalPrice"),
        "color": it.get("color"),
        "image": it.get("image"),
        "sizes": list(it.get("sizes", [])),
        "category": category
    }

# Build the simulation catalog structure used by the generator
CATALOG = {
    # Simulation "tops" include jackets + pullovers
    "tops":       [_map_item(x, "jacke")    for x in SOURCE_CATALOG["jacken"]] +
                  [_map_item(x, "pullover") for x in SOURCE_CATALOG["pullover"]],
    # "trousers" include pants + skirts
    "trousers":   [_map_item(x, "hose")     for x in SOURCE_CATALOG["hosen"]] +
                  [_map_item(x, "rock")     for x in SOURCE_CATALOG["roecke"]],
    # shoes map 1:1
    "shoes":      [_map_item(x, "schuhe")   for x in SOURCE_CATALOG["schuhe"]],
    # one-piece dresses
    "one_pieces": [_map_item(x, "kleid")    for x in SOURCE_CATALOG["kleider"]],
}

# (Optional) A deep copy to avoid accidental mutation by other cells
CATALOG = copy.deepcopy(CATALOG)

# Quick sanity check (can be commented out)
sum_items = sum(len(v) for v in CATALOG.values())
print(f"Catalog loaded. Items total: {sum_items} | tops={len(CATALOG['tops'])}, trousers={len(CATALOG['trousers'])}, shoes={len(CATALOG['shoes'])}, one_pieces={len(CATALOG['one_pieces'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Enhanced Simulation Loop (fixed refs + ratio, randomness, promo usage, value trends)

# This cell:
# - Keeps promo usage to ~8% (within your 5–10% target)
# - Uses Poisson arrivals to average ~1.5 orders/sec (randomized timing)
# - Picks combinations/sec so orders/combinations ≈ 50–60%
# - Adds a smooth 30-min sinusoidal trend to item prices for visible order value waves
# - Is defensive: if Cell 2 helpers are missing, it defines minimal fallbacks so it runs

import math
import time
import random
import datetime as dt
from collections import deque

# -----------------------
# Defensive fallbacks (only used if Cell 2 wasn't executed)
# -----------------------
if "utc_now_iso" not in globals():
    def utc_now_iso():
        return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

if "new_id" not in globals():
    import uuid
    def new_id(prefix):
        return f"{prefix}{uuid.uuid4().hex[:12]}"

if "random_user" not in globals():
    def random_user():
        return f"user_{random.randint(1000, 9999)}"

if "pick_outfit_items" not in globals():
    # Minimal picker if Cell 2 hasn't run; tries to use CATALOG structure
    def pick_outfit_items():
        items = []
        if "CATALOG" in globals() and isinstance(CATALOG, dict):
            use_one_piece = (random.random() < 0.25) and (len(CATALOG.get("one_pieces", [])) > 0)
            if use_one_piece:
                items = [random.choice(CATALOG["one_pieces"])]
            else:
                top = random.choice(CATALOG.get("tops", [])) if CATALOG.get("tops") else None
                bottom = random.choice(CATALOG.get("trousers", [])) if CATALOG.get("trousers") else None
                items = [x for x in (top, bottom) if x]
            if (random.random() < 0.65) and CATALOG.get("shoes"):
                items.append(random.choice(CATALOG["shoes"]))
        return items

if "to_order_item" not in globals():
    def to_order_item(combo_item):
        return {
            "product_id": combo_item.get("product_id"),
            "name": combo_item.get("name"),
            "color": combo_item.get("color"),
            "size": combo_item.get("size"),
            "quantity": 1,
            "price": combo_item.get("price"),
        }

if "send_json_event" not in globals():
    import json
    try:
        from azure.eventhub import EventData
    except Exception:
        EventData = None
    def send_json_event(producer, payload):
        data = json.dumps(payload, ensure_ascii=False)
        if hasattr(producer, "create_batch") and EventData is not None:
            batch = producer.create_batch()
            batch.add(EventData(data))
            producer.send_batch(batch)
        elif hasattr(producer, "send"):
            producer.send(data)  # very basic fallback
        else:
            raise RuntimeError("Producer object does not support sending events.")

if "build_combination" not in globals():
    def build_combination(combination_user, community_created):
        combination_id = new_id("comb_")
        raw_items = pick_outfit_items()
        combo_items = [to_combination_item(p) for p in raw_items]
        return {
            "combination_id": combination_id,
            "user": combination_user,
            "is_community_created": bool(community_created),
            "items": combo_items,
        }

if "build_combination_created_event" not in globals():
    def build_combination_created_event(combination):
        return {
            "event_type": "combination_created",
            "event_time": utc_now_iso(),
            "combination_id": combination["combination_id"],
            "user": combination["user"],
            "is_community_created": combination["is_community_created"],
            "items": combination["items"],
        }

if "build_order" not in globals():
    def build_order(combination, order_user):
        order_id = new_id("ord_")
        order_items = [to_order_item(ci) for ci in combination["items"]]
        subtotal = round(sum(oi["price"] * oi["quantity"] for oi in order_items), 2)
        promo_code, discount, total = apply_promotion(subtotal)
        return {
            "order_id": order_id,
            "combination_id": combination["combination_id"],
            "user": order_user,
            "items": order_items,
            "promo_code": promo_code,
            "discount": discount,
            "total_price": total,
        }

if "build_sale_event" not in globals():
    def build_sale_event(order, combination):
        return {
            "event_type": "sale",
            "event_time": utc_now_iso(),
            "order_id": order["order_id"],
            "combination_id": order["combination_id"],
            "user": order["user"],
            "is_community_created": combination["is_community_created"],
            "promo_code": order.get("promo_code"),
            "discount": order.get("discount"),
            "total_price": order.get("total_price"),
            "items": order.get("items", []),
        }

# State containers if missing
if "pending_generated_for_purchase" not in globals():
    pending_generated_for_purchase = deque()
if "community_combos" not in globals():
    community_combos = {}
if "PCT_PURCHASES_FROM_NEW_GENERATIONS" not in globals():
    PCT_PURCHASES_FROM_NEW_GENERATIONS = 0.75  # default from Cell 2

# -----------------------
# Knobs (per your requirements)
# -----------------------
DESIRED_ORDERS_PER_SEC = 1.5                                    # ~1.5 orders/sec on average
TARGET_ORDER_TO_COMBO_RATIO = random.uniform(0.50, 0.60)        # target ~50–60% of combos become orders
COMBINATIONS_PER_SEC = max(0.5, DESIRED_ORDERS_PER_SEC / TARGET_ORDER_TO_COMBO_RATIO)

PROMO_USAGE_PROB = 0.08                                         # ~8% of orders use promo
TREND_PERIOD_MIN = 30                                           # order value trend period
TREND_AMPLITUDE = 0.12                                          # ±12% trend swing

# -----------------------
# Trend helper
# -----------------------
def _price_trend_multiplier(t_epoch: float | None = None) -> float:
    """Smooth sinusoidal multiplier for prices over time; period = TREND_PERIOD_MIN."""
    if t_epoch is None:
        t_epoch = time.time()
    period_s = TREND_PERIOD_MIN * 60.0
    angle = 2.0 * math.pi * ((t_epoch % period_s) / period_s)
    return 1.0 + TREND_AMPLITUDE * math.sin(angle)

# -----------------------
# Override promo application to enforce low promo usage
# -----------------------
def apply_promotion(subtotal: float):
    """
    Applies a promotion with ~8% probability. When applied, select existing codes with weights.
    """
    if random.random() >= PROMO_USAGE_PROB:
        return None, 0.0, round(subtotal, 2)

    promo = random.choices(
        population=["WELCOME10", "BUNDLE5", "FREESHIP"],
        weights=[0.5, 0.3, 0.2],
        k=1
    )[0]

    discount = 0.0
    if promo == "WELCOME10":
        discount = 0.10 * subtotal
    elif promo == "BUNDLE5":
        discount = 5.00 if subtotal >= 50 else 0.0
    elif promo == "FREESHIP":
        discount = 0.0

    total = round(max(subtotal - discount, 0.0), 2)
    return promo, round(discount, 2), total

# -----------------------
# Override combination item pricing to inject trend
# -----------------------
def to_combination_item(product):
    # Start with small randomization around base price
    base = product["price"] * random.uniform(0.95, 1.10)
    # Apply slow trend for visible waves in total order values
    price = round(base * _price_trend_multiplier(), 2)

    sizes = product.get("sizes") or [None]
    size = random.choice(sizes)

    return {
        "product_id": product.get("product_id"),
        "name": product.get("name"),
        "brand": product.get("brand"),
        "category": product.get("category"),
        "price": price,
        "original_price": product.get("price"),
        "color": product.get("color"),
        "size": size,
        "image_url": product.get("image"),
    }

# -----------------------
# Poisson-style arrival processes (random inter-arrivals)
# -----------------------
def _next_order_due(now_mono: float) -> float:
    # Exponential inter-arrival with mean 1/DESIRED_ORDERS_PER_SEC
    return now_mono + random.expovariate(DESIRED_ORDERS_PER_SEC)

def _next_combo_due(now_mono: float) -> float:
    # Exponential inter-arrival for combinations to reach target ratio
    return now_mono + random.expovariate(COMBINATIONS_PER_SEC)

# -----------------------
# Start enhanced loop
# -----------------------
print(
    f"Enhanced loop starting.\n"
    f"Target orders/sec ≈ {DESIRED_ORDERS_PER_SEC:.2f} (randomized)\n"
    f"Target order/combination ratio ≈ {TARGET_ORDER_TO_COMBO_RATIO:.2f}\n"
    f"Implied combinations/sec ≈ {COMBINATIONS_PER_SEC:.2f}\n"
    f"Promo usage ≈ {PROMO_USAGE_PROB*100:.1f}%\n"
    f"Price trend: ±{int(TREND_AMPLITUDE*100)}% over {TREND_PERIOD_MIN} minutes\n"
)

now_mono = time.monotonic()
next_order_t = _next_order_due(now_mono)
next_combo_t = _next_combo_due(now_mono)

orders_sent_v2 = 0
combos_sent_v2 = 0
last_log_t = time.monotonic()
start_wall = time.time()

try:
    while True:
        now_mono = time.monotonic()

        # 1) Generate combinations → combinations stream
        if now_mono >= next_combo_t:
            user = random_user()
            c = build_combination(combination_user=user, community_created=False)
            send_json_event(combinations_producer, build_combination_created_event(c))
            combos_sent_v2 += 1

            pending_generated_for_purchase.append(c)
            next_combo_t = _next_combo_due(now_mono)

        # 2) Create orders → sales stream
        if now_mono >= next_order_t:
            buyer = random_user()

            use_new_generation = (
                (random.random() < PCT_PURCHASES_FROM_NEW_GENERATIONS)
                and (len(pending_generated_for_purchase) > 0)
            )

            if use_new_generation and len(pending_generated_for_purchase) > 0:
                combo = pending_generated_for_purchase.popleft()
            else:
                if community_combos:
                    combo = random.choice(list(community_combos.values()))
                else:
                    # In case there are no community combos, synthesize one (do not emit it)
                    combo = build_combination(combination_user=random_user(), community_created=False)

            order = build_order(combo, order_user=buyer)
            send_json_event(sales_producer, build_sale_event(order, combo))
            orders_sent_v2 += 1

            next_order_t = _next_order_due(now_mono)

        # 3) Periodic logging
        if (now_mono - last_log_t) >= 5.0:
            elapsed = max(1e-9, time.time() - start_wall)
            orders_per_sec_obs = orders_sent_v2 / elapsed
            combos_per_sec_obs = combos_sent_v2 / elapsed
            ratio_obs = (orders_sent_v2 / max(1, combos_sent_v2))
            trend_now = _price_trend_multiplier()

            print(
                f"{dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()} "
                f"totals → combos: {combos_sent_v2}, orders: {orders_sent_v2}, "
                f"obs_rates: orders/s={orders_per_sec_obs:.2f}, combos/s={combos_per_sec_obs:.2f}, "
                f"orders/combos={ratio_obs:.2f}, price_trend×={trend_now:.3f}, "
                f"pending_new_for_purchase={len(pending_generated_for_purchase)}"
            )
            last_log_t = now_mono

        time.sleep(0.01)

except KeyboardInterrupt:
    print("Enhanced simulation stopped by user.")
finally:
    # Close producers gracefully if they exist
    try:
        sales_producer.close()
    except Exception as e:
        print(f"Warning: failed to close sales producer: {e}")

    try:
        combinations_producer.close()
    except Exception as e:
        print(f"Warning: failed to close combinations producer: {e}")

    print("Producers closed (enhanced loop).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
