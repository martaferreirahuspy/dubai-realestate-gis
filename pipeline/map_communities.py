#!/usr/bin/env python3
"""
map_communities.py
------------------
Maps DLD AREA_EN values to Dubai Municipality COMMUNITY names
(the shapeName in the GeoJSON) so rows can be visualised on the map.

Strategy (in order of reliability):
  1. Exact match       – normalised uppercase comparison
  2. Geocode + PIP     – Google Maps lat/lng → point-in-polygon against GeoJSON
                         Results cached in geocode_cache.json (no repeat API calls)
  3. Fuzzy match       – last resort for spelling variants that fail geocoding

New column added to every row:
  COMMUNITY   – the shapeName (title-case DM community name) that joins to the GeoJSON

Usage:
    python3 map_communities.py \
        --input  transactions_raw.csv \
        --output transactions_mapped.csv \
        --geojson ../dubai_communities.geojson \
        --api-key YOUR_GOOGLE_MAPS_KEY  # optional, skip to use cache/fuzzy only

Environment variable alternative for API key:
    GOOGLE_MAPS_API_KEY
"""

import argparse
import csv
import json
import os
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path

import requests
from shapely.geometry import shape, Point


GEOCODE_URL   = "https://maps.googleapis.com/maps/api/geocode/json"
CACHE_FILE    = Path(__file__).parent / "geocode_cache.json"
FUZZY_THRESH  = 0.72


# ── Load GeoJSON ───────────────────────────────────────────────────────────────

def load_geojson(path: str) -> tuple[dict, list]:
    """Returns (lookup_by_cname_e_upper, features_list)."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    features = data["features"]
    lookup = {}
    for feat in features:
        p = feat["properties"]
        key = (p.get("CNAME_E") or "").upper().strip()
        if key:
            lookup[key] = p.get("shapeName", "")
    return lookup, features


# ── Matching helpers ───────────────────────────────────────────────────────────

def fuzzy_best(query: str, candidates: list[str], threshold: float) -> tuple[str, float]:
    best_key, best_score = "", 0.0
    for cand in candidates:
        score = SequenceMatcher(None, query, cand).ratio()
        if score > best_score:
            best_score, best_key = score, cand
    return (best_key, best_score) if best_score >= threshold else ("", 0.0)


def geocode(area: str, api_key: str, session: requests.Session) -> tuple[float | None, float | None]:
    for query in [f"{area}, Dubai, UAE", f"{area}, UAE"]:
        try:
            r = session.get(
                GEOCODE_URL,
                params={"address": query, "components": "country:AE", "key": api_key, "language": "en"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "OK" and data.get("results"):
                loc = data["results"][0]["geometry"]["location"]
                return loc["lat"], loc["lng"]
        except Exception as e:
            print(f"  [geocode error] {area}: {e}", file=sys.stderr)
    return None, None


def point_in_polygon(lon: float, lat: float, features: list) -> str | None:
    pt = Point(lon, lat)
    for feat in features:
        try:
            if shape(feat["geometry"]).contains(pt):
                return feat["properties"].get("shapeName", "")
        except Exception:
            continue
    return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Map DLD AREA_EN → DM COMMUNITY shapeName")
    parser.add_argument("--input",   required=True)
    parser.add_argument("--output",  required=True)
    parser.add_argument("--geojson", required=True)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("GOOGLE_MAPS_API_KEY")

    print("Loading GeoJSON...")
    lookup, features = load_geojson(args.geojson)
    cname_keys = list(lookup.keys())
    shape_names = list(lookup.values())

    print(f"  {len(features)} communities loaded.")

    # Load geocode cache
    cache: dict = {}
    if CACHE_FILE.exists():
        cache = json.loads(CACHE_FILE.read_text())
        print(f"  Geocode cache: {len(cache)} entries.")

    # Read transactions
    with open(args.input, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    unique_areas = sorted({r.get("AREA_EN", "").strip() for r in rows if r.get("AREA_EN", "").strip()})
    print(f"\nMapping {len(unique_areas)} unique areas  [exact → geocode (cached) → fuzzy]")

    mapping: dict[str, str] = {}   # AREA_EN → shapeName
    stats = {"exact": 0, "geocode": 0, "fuzzy": 0, "none": 0}
    fuzzy_queue = []

    session = requests.Session() if api_key else None
    if session:
        session.headers["User-Agent"] = "dubai-realestate-pipeline/1.0"

    for area in unique_areas:
        norm = area.upper().strip()

        # 1. Exact
        if norm in lookup:
            mapping[area] = lookup[norm]
            stats["exact"] += 1
            continue

        # 2. Geocode + PIP (cache first)
        if area in cache:
            cached = cache[area]
            if cached:
                mapping[area] = cached
                stats["geocode"] += 1
                continue
            # cache says no PIP hit → fall to fuzzy
        elif api_key:
            lat, lon = geocode(area, api_key, session)
            time.sleep(0.05)
            if lat is not None:
                shape_name = point_in_polygon(lon, lat, features)
                if shape_name:
                    cache[area] = shape_name
                    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
                    mapping[area] = shape_name
                    stats["geocode"] += 1
                    print(f"  geocode  {area!r:<45} → {shape_name!r}")
                    continue
                else:
                    cache[area] = None
                    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))
                    print(f"  geocode  {area!r:<45} → outside polygons, trying fuzzy")
            else:
                cache[area] = None
                CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2))

        fuzzy_queue.append(area)

    # 3. Fuzzy (last resort)
    for area in fuzzy_queue:
        norm = area.upper().strip()
        best_key, score = fuzzy_best(norm, cname_keys, FUZZY_THRESH)
        if best_key:
            shape_name = lookup[best_key]
            mapping[area] = shape_name
            stats["fuzzy"] += 1
            print(f"  fuzzy    {area!r:<45} → {shape_name!r} ({score:.2f})")
        else:
            mapping[area] = ""
            stats["none"] += 1
            print(f"  UNMATCHED {area!r}", file=sys.stderr)

    # Write output
    out_fields = fieldnames + (["COMMUNITY"] if "COMMUNITY" not in fieldnames else [])
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            area = row.get("AREA_EN", "").strip()
            out_row = dict(row)
            out_row["COMMUNITY"] = mapping.get(area, "")
            writer.writerow(out_row)

    total = len(unique_areas)
    print(f"\n── RESULTS ────────────────────────────────────────────────────")
    print(f"  Exact     : {stats['exact']:>4}  ({stats['exact']/total*100:.1f}%)")
    print(f"  Geocode   : {stats['geocode']:>4}  ({stats['geocode']/total*100:.1f}%)")
    print(f"  Fuzzy     : {stats['fuzzy']:>4}  ({stats['fuzzy']/total*100:.1f}%)")
    print(f"  Unmatched : {stats['none']:>4}  ({stats['none']/total*100:.1f}%)")
    print(f"  Rows written: {len(rows):,} → {args.output}")


if __name__ == "__main__":
    main()
