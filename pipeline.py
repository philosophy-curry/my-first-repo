"""
Curry Data Pipeline: Lake -> Warehouse -> Mart
インド料理カレー データパイプライン（最小構成・ループなし）
"""

import json
import pathlib

ROOT = pathlib.Path(__file__).parent


def load(path: str) -> dict:
    return json.loads((ROOT / path).read_text())


def save(path: str, data: dict) -> None:
    (ROOT / path).write_text(json.dumps(data, ensure_ascii=False, indent=2))


# ── Layer 1: Data Lake ────────────────────────────────────────────────────────

def ingest_lake() -> dict:
    """生データをレイクから読み込む"""
    raw = load("data_lake/raw_curries.json")
    print(f"[Lake] {len(raw['records'])} records loaded")
    return raw


# ── Layer 2: Data Warehouse ───────────────────────────────────────────────────

def build_warehouse(raw: dict) -> dict:
    """レイクデータを正規化してウェアハウスへ"""
    fact  = load("data_warehouse/fact_curry.json")
    dim_r = load("data_warehouse/dim_region.json")
    dim_s = load("data_warehouse/dim_spice.json")

    region_index = {r["region_id"]: r for r in dim_r["records"]}
    spice_index  = {s["spice_id"]:  s for s in dim_s["records"]}

    enriched = []
    for record in fact["records"]:
        region = region_index.get(record["region_id"], {})
        spices = [spice_index[sid]["name"] for sid in record["spice_ids"] if sid in spice_index]
        enriched.append({**record, "region_name": region.get("region"), "resolved_spices": spices})

    print(f"[Warehouse] {len(enriched)} records enriched")
    return {"enriched_curries": enriched}


# ── Layer 3: Data Mart ────────────────────────────────────────────────────────

def build_mart(warehouse: dict) -> None:
    """ウェアハウスから分析用マートを生成・更新"""
    curries = warehouse["enriched_curries"]

    # Vegetarian mart
    veg = [c for c in curries if c["is_vegetarian"]]
    save("data_mart/mart_vegetarian.json", {
        "mart":        "vegetarian_curries",
        "description": "ベジタリアン向けカレー一覧（分析用）",
        "records":     [{"name": c["name"], "region": c["region_name"],
                         "spice_level": c["spice_level"], "base": c["base"],
                         "key_spices": c["resolved_spices"]} for c in veg]
    })
    print(f"[Mart] vegetarian: {len(veg)} records")

    # Spice ranking mart
    ranked = sorted(curries, key=lambda c: c["spice_level"], reverse=True)
    save("data_mart/mart_spice_ranking.json", {
        "mart":        "spice_level_ranking",
        "description": "辛さランキング（分析用）",
        "records":     [{"rank": i + 1, "name": c["name"],
                         "spice_level": c["spice_level"],
                         "region": c["region_name"]} for i, c in enumerate(ranked)]
    })
    print(f"[Mart] spice_ranking: {len(ranked)} records")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    raw       = ingest_lake()
    warehouse = build_warehouse(raw)
    build_mart(warehouse)
    print("Pipeline complete.")
