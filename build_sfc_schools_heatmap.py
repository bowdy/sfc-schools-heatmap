#!/usr/bin/env python3
"""
Build an interactive UK heatmap of SFC Parent Pact sign-ups at school level.

Downloads school-level pact data from the SFC website's JS bundle,
geocodes postcodes, calculates engagement percentages, and generates
a self-contained HTML file with a Leaflet.js heatmap + county choropleth.

Usage: python3 build_sfc_schools_heatmap.py
Output: index.html (open in browser)
"""

import urllib.request
import re
import json
import os
import csv
import time
import shutil
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ─── County mapping (from sfc-heatmap/build_heatmap.py) ──────────────────────

# Multiple SFC regions that merge into one GeoJSON county
MERGE_MAP = {
    "Greater London": [
        "London, Bromley", "London, Central", "London, East",
        "London, North West", "London, North", "London, South East",
        "London, South West", "London, West"
    ],
    "Greater Manchester": ["Greater Manchester", "Manchester"],
    "Somerset": ["Somerset", "Somerset, Bath"],
    "Durham": ["County Durham", "Tees Valley"],
}

# SFC regions with a single national total mapped to multiple GeoJSON sub-features
NATIONAL_REGIONS = {
    "Scotland": [
        "Central", "Dumfries and Galloway", "Eilean Siar", "Fife",
        "Grampian", "Highland", "Lothian", "Orkney Islands",
        "Scottish Borders", "Shetland Islands", "Strathclyde", "Tayside"
    ],
    "Wales": [
        "Clwyd", "Dyfed", "Gwent", "Gwynedd", "Mid Glamorgan",
        "Powys", "South Glamorgan", "West Glamorgan"
    ],
    "Northern Ireland": [
        "Antrim", "Armagh", "Down", "Fermanagh", "Londonderry", "Tyrone"
    ],
}

# Direct rename: SFC name -> GeoJSON name
RENAME_MAP = {
    "County Durham": "Durham",
}

# Regions with no GeoJSON boundary (omitted from map)
OMITTED = {"Channel Islands", "Isle of Man", "National (UK)"}


# ─── Step 1: Download and extract school-level pact data ─────────────────────

def extract_school_data():
    """Download the SFC JS bundle and extract school-level pact counts with region."""
    print("[1/5] Extracting school-level pact data from app_embed.js...")

    cache_path = os.path.join(DATA_DIR, "app_embed.js")
    max_age = 24 * 3600  # 24 hours

    if os.path.exists(cache_path) and (time.time() - os.path.getmtime(cache_path)) < max_age:
        print(f"  Using cached bundle ({os.path.getsize(cache_path):,} bytes)")
        with open(cache_path, "r", errors="replace") as f:
            js = f.read()
    else:
        url = "https://static.smartphonefreechildhood.org/app_embed.js"
        print(f"  Downloading {url}...")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = urllib.request.urlopen(req, timeout=120).read()
        js = data.decode("utf-8", errors="replace")
        with open(cache_path, "wb") as f:
            f.write(data)
        print(f"  Downloaded {len(data):,} bytes")

    # Two-pass extraction: first find region blocks, then schools within each
    # Pattern: region:"RegionName",...,schools:[{...},{...},...]}
    # We find each region header and its position, then extract schools between
    region_header_pattern = r'region:"([^"]*)",legacy_region_short:"([^"]*)",legacy_region_simple:"([^"]*)",pacts:(\d+)'
    region_matches = list(re.finditer(region_header_pattern, js))

    if not region_matches:
        raise RuntimeError("Could not find region headers in app_embed.js.")

    # Build region boundaries: each region spans from its match to the next region
    region_spans = []
    for i, m in enumerate(region_matches):
        start = m.start()
        end = region_matches[i + 1].start() if i + 1 < len(region_matches) else len(js)
        region_spans.append((m.group(1), int(m.group(4)), start, end))

    # Extract schools within each region span
    school_pattern = (
        r'address:"([^"]*)",'
        r'high_age:(\d+),'
        r'legacy_school_name:(?:null|"[^"]*"),'
        r'low_age:(\d+),'
        r'pacts:(\d+),'
        r'postcode:"([^"]*)",'
        r'pupils:"(\d*)",'
        r'school_id:"([^"]*)",'
        r'school_name:"([^"]*)"'
    )

    schools = []
    region_data = {}  # region_name -> {pacts, pupils, school_count}

    for region_name, region_pacts, start, end in region_spans:
        chunk = js[start:end]
        matches = re.findall(school_pattern, chunk)

        region_school_count = 0
        region_school_pacts = 0
        region_school_pupils = 0

        for m in matches:
            addr, high_age, low_age, pacts, postcode, pupils, school_id, school_name = m
            school_name = school_name.replace("\\u0027", "'").replace("\\'", "'")
            addr = addr.replace("\\u0027", "'").replace("\\'", "'")

            p = int(pupils) if pupils else 0
            pc = int(pacts)

            schools.append({
                "id": school_id,
                "name": school_name,
                "pacts": pc,
                "pupils": p,
                "postcode": postcode,
                "address": addr,
                "high_age": int(high_age),
                "low_age": int(low_age),
                "lat": 0.0,
                "lng": 0.0,
                "region": region_name,
            })

            region_school_count += 1
            region_school_pacts += pc
            region_school_pupils += p

        region_data[region_name] = {
            "pacts": region_pacts,
            "school_pacts": region_school_pacts,
            "pupils": region_school_pupils,
            "school_count": region_school_count,
        }

    total_school_pacts = sum(s["pacts"] for s in schools)
    total_region_pacts = sum(rd["pacts"] for rd in region_data.values())
    print(f"  Extracted {len(schools):,} schools with {total_school_pacts:,} total pacts")
    print(f"  ({len(region_data)} regions with {total_region_pacts:,} total pacts)")

    # Nation breakdown
    from collections import Counter
    nations = Counter()
    for s in schools:
        prefix = s["id"].split("/")[0] if "/" in s["id"] else "unknown"
        nations[prefix] += 1
    print(f"  By nation: {dict(nations)}")

    return schools, region_data


# ─── Step 2: Geocode postcodes ───────────────────────────────────────────────

def geocode_postcodes(schools):
    """Bulk geocode UK postcodes using postcodes.io (free, no key needed)."""
    print("\n[2/5] Geocoding postcodes...")

    cache_path = os.path.join(DATA_DIR, "postcode_geocodes.json")
    cached = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            cached = json.load(f)
        print(f"  Loaded {len(cached):,} cached geocodes")

    unique_postcodes = set()
    for s in schools:
        pc = s["postcode"].upper().strip().replace(" ", "")
        if pc and pc not in cached:
            unique_postcodes.add(pc)

    print(f"  {len(unique_postcodes):,} new postcodes to geocode")

    if unique_postcodes:
        postcodes_list = sorted(unique_postcodes)
        batch_size = 100
        total_batches = (len(postcodes_list) + batch_size - 1) // batch_size

        for i in range(0, len(postcodes_list), batch_size):
            batch = postcodes_list[i:i + batch_size]
            batch_num = i // batch_size + 1

            if batch_num % 20 == 1 or batch_num == total_batches:
                print(f"  Batch {batch_num}/{total_batches}...")

            payload = json.dumps({"postcodes": batch}).encode("utf-8")
            req = urllib.request.Request(
                "https://api.postcodes.io/postcodes",
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                method="POST",
            )

            try:
                resp = urllib.request.urlopen(req, timeout=30)
                data = json.loads(resp.read().decode("utf-8"))

                for item in data.get("result", []):
                    query = item.get("query", "").upper().replace(" ", "")
                    result = item.get("result")
                    if result and result.get("latitude") and result.get("longitude"):
                        cached[query] = [
                            round(result["latitude"], 5),
                            round(result["longitude"], 5),
                        ]
            except Exception as e:
                print(f"  WARNING: Batch {batch_num} failed: {e}")

            if batch_num < total_batches:
                time.sleep(0.15)

        with open(cache_path, "w") as f:
            json.dump(cached, f, separators=(",", ":"))
        print(f"  Saved {len(cached):,} geocodes to cache")

    geocoded = 0
    failed = 0
    for s in schools:
        pc = s["postcode"].upper().strip().replace(" ", "")
        if pc in cached:
            s["lat"], s["lng"] = cached[pc]
            geocoded += 1
        else:
            failed += 1

    print(f"  Geocoded: {geocoded:,}, Failed: {failed:,}")

    before = len(schools)
    schools = [s for s in schools if s["lat"] != 0.0]
    if before != len(schools):
        print(f"  Removed {before - len(schools)} schools with no coordinates")

    return schools


# ─── Step 3: Calculate engagement metrics ────────────────────────────────────

def calculate_metrics(schools):
    """Calculate pact engagement percentage for each school."""
    print("\n[3/5] Calculating engagement metrics...")

    for s in schools:
        if s["pupils"] > 0:
            s["pact_pct"] = round(min(s["pacts"] / s["pupils"] * 100, 100), 1)
        else:
            s["pact_pct"] = 0

    with_pct = [s for s in schools if s["pact_pct"] > 0]
    avg_pct = sum(s["pact_pct"] for s in with_pct) / len(with_pct) if with_pct else 0
    print(f"  Schools with calculable %: {len(with_pct):,}")
    print(f"  Average pact %: {avg_pct:.1f}%")

    bands = {"0%": 0, "1-10%": 0, "11-25%": 0, "26-50%": 0, "51-75%": 0, "76-100%": 0}
    for s in schools:
        p = s["pact_pct"]
        if p == 0:
            bands["0%"] += 1
        elif p <= 10:
            bands["1-10%"] += 1
        elif p <= 25:
            bands["11-25%"] += 1
        elif p <= 50:
            bands["26-50%"] += 1
        elif p <= 75:
            bands["51-75%"] += 1
        else:
            bands["76-100%"] += 1

    print(f"  Distribution: {bands}")
    return schools


# ─── Step 3b: Export CSV ─────────────────────────────────────────────────────

def export_csv(schools):
    """Export school data to CSV for sharing."""
    csv_path = os.path.join(DATA_DIR, "sfc_school_pacts.csv")

    nation_map = {
        "england": "England",
        "scotland": "Scotland",
        "wales": "Wales",
        "ni": "Northern Ireland",
    }

    fieldnames = [
        "school_id", "name", "region", "nation", "pacts", "pupils",
        "pact_pct", "postcode", "address", "lat", "lng", "low_age", "high_age",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in schools:
            prefix = s["id"].split("/")[0] if "/" in s["id"] else "unknown"
            writer.writerow({
                "school_id": s["id"],
                "name": s["name"],
                "region": s["region"],
                "nation": nation_map.get(prefix, prefix),
                "pacts": s["pacts"],
                "pupils": s["pupils"],
                "pact_pct": s["pact_pct"],
                "postcode": s["postcode"],
                "address": s["address"],
                "lat": s["lat"],
                "lng": s["lng"],
                "low_age": s["low_age"],
                "high_age": s["high_age"],
            })

    print(f"  Exported {len(schools):,} schools to {csv_path}")


# ─── Step 4: Aggregate county data + load GeoJSON ───────────────────────────

def build_county_data(schools, region_data):
    """Aggregate school data by SFC region and map to GeoJSON counties."""
    print("\n[4/5] Building county-level aggregation...")

    # Aggregate schools by region
    from collections import defaultdict
    region_agg = defaultdict(lambda: {"pacts": 0, "pupils": 0, "school_count": 0})
    for s in schools:
        r = region_agg[s["region"]]
        r["pacts"] += s["pacts"]
        r["pupils"] += s["pupils"]
        r["school_count"] += 1

    # Calculate pact_pct per region
    for name, r in region_agg.items():
        if r["pupils"] > 0:
            r["pact_pct"] = round(min(r["pacts"] / r["pupils"] * 100, 100), 1)
        else:
            r["pact_pct"] = 0

    print(f"  Aggregated {len(region_agg)} regions from school data")

    # Load GeoJSON — copy from sfc-heatmap if not present
    geo_path = os.path.join(DATA_DIR, "uk-counties-simplified.geojson")
    if not os.path.exists(geo_path):
        src = os.path.join(SCRIPT_DIR, "..", "sfc-heatmap", "uk-counties-simplified.geojson")
        if os.path.exists(src):
            shutil.copy2(src, geo_path)
            print(f"  Copied GeoJSON from sfc-heatmap ({os.path.getsize(geo_path):,} bytes)")
        else:
            # Download from GitHub
            url = "https://raw.githubusercontent.com/evansd/uk-ceremonial-counties/master/uk-ceremonial-counties.geojson"
            print(f"  Downloading county boundaries...")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            data = urllib.request.urlopen(req, timeout=120).read()
            with open(geo_path, "wb") as f:
                f.write(data)
            print(f"  Downloaded {len(data):,} bytes (raw — not simplified)")

    with open(geo_path, "r") as f:
        geojson = json.load(f)

    # Map SFC regions to GeoJSON counties
    county_pacts = {}
    county_pupils = {}
    county_schools = {}
    county_labels = {}

    # Get all GeoJSON county names
    geo_names = {f["properties"].get("county", f["properties"].get("name", ""))
                 for f in geojson["features"]}

    # 1. Handle merged regions
    already_mapped = set()
    for geo_name, sfc_names in MERGE_MAP.items():
        total_pacts = sum(region_agg.get(n, {}).get("pacts", 0) for n in sfc_names)
        total_pupils = sum(region_agg.get(n, {}).get("pupils", 0) for n in sfc_names)
        total_schools = sum(region_agg.get(n, {}).get("school_count", 0) for n in sfc_names)
        if total_pacts > 0 or total_schools > 0:
            county_pacts[geo_name] = total_pacts
            county_pupils[geo_name] = total_pupils
            county_schools[geo_name] = total_schools
            parts = [f"{n}: {region_agg.get(n, {}).get('pacts', 0):,}" for n in sfc_names if region_agg.get(n, {}).get("pacts", 0) > 0]
            if len(parts) > 1:
                county_labels[geo_name] = " + ".join(parts)
        already_mapped.update(sfc_names)

    # 2. Handle national regions (Scotland, Wales, NI)
    for sfc_name, geo_counties in NATIONAL_REGIONS.items():
        agg = region_agg.get(sfc_name, {})
        pacts = agg.get("pacts", 0)
        pupils = agg.get("pupils", 0)
        sc = agg.get("school_count", 0)
        if pacts > 0 or sc > 0:
            # Distribute evenly across sub-regions (or broadcast total)
            n_counties = len(geo_counties)
            for county in geo_counties:
                county_pacts[county] = pacts
                county_pupils[county] = pupils
                county_schools[county] = sc
                county_labels[county] = f"{sfc_name} total (not broken down by sub-region)"
        already_mapped.add(sfc_name)

    already_mapped.update(OMITTED)

    # 3. Handle direct matches
    for sfc_name, agg in region_agg.items():
        if sfc_name in already_mapped:
            continue

        target = sfc_name
        if sfc_name in RENAME_MAP:
            target = RENAME_MAP[sfc_name]

        if target in geo_names:
            county_pacts[target] = agg["pacts"]
            county_pupils[target] = agg["pupils"]
            county_schools[target] = agg["school_count"]
        elif sfc_name not in OMITTED:
            print(f"  WARNING: SFC region '{sfc_name}' not matched to GeoJSON county")

    # Annotate GeoJSON features
    mapped = 0
    for feature in geojson["features"]:
        county = feature["properties"].get("county", feature["properties"].get("name", ""))
        if not county:
            continue
        pacts = county_pacts.get(county, 0)
        pupils = county_pupils.get(county, 0)
        sc = county_schools.get(county, 0)
        pct = round(min(pacts / pupils * 100, 100), 1) if pupils > 0 else 0

        feature["properties"]["pacts"] = pacts
        feature["properties"]["pupils"] = pupils
        feature["properties"]["pact_pct"] = pct
        feature["properties"]["school_count"] = sc
        feature["properties"]["label"] = county_labels.get(county, "")
        if pacts > 0:
            mapped += 1

    print(f"  Mapped {mapped} of {len(geojson['features'])} counties with pact data")
    return geojson


# ─── Step 5: Generate interactive HTML ───────────────────────────────────────

def generate_html(schools, geojson, output_path):
    """Generate a self-contained HTML file with heatmap, markers, and county choropleth."""
    print("\n[5/5] Generating interactive heatmap...")

    extraction_date = datetime.now().strftime("%d %B %Y")
    total_pacts = sum(s["pacts"] for s in schools)
    total_schools = len(schools)
    total_pupils = sum(s["pupils"] for s in schools)
    avg_pact_pct = sum(s["pact_pct"] for s in schools) / len(schools) if schools else 0

    # Top schools by pact count
    top_by_pacts = sorted(schools, key=lambda s: -s["pacts"])[:20]
    top_by_pacts_json = json.dumps([
        {"n": s["name"], "p": s["pacts"], "pct": s["pact_pct"]}
        for s in top_by_pacts
    ])

    # Top schools by pact %
    top_by_pct = sorted([s for s in schools if s["pupils"] >= 10], key=lambda s: -s["pact_pct"])[:20]
    top_by_pct_json = json.dumps([
        {"n": s["name"], "p": s["pacts"], "pct": s["pact_pct"]}
        for s in top_by_pct
    ])

    # Prepare school data for embedding (minimal keys for size)
    school_data = []
    for s in schools:
        school_data.append([
            round(s["lat"], 5),   # 0: lat
            round(s["lng"], 5),   # 1: lng
            s["pacts"],           # 2: pact count
            s["pact_pct"],        # 3: pact %
            s["name"],            # 4: name
            s["pupils"],          # 5: pupils
            s["address"],         # 6: address
            s["id"],              # 7: school_id
            s["postcode"],        # 8: postcode
            s["low_age"],         # 9: low_age
            s["high_age"],        # 10: high_age
        ])

    schools_json = json.dumps(school_data, separators=(",", ":"))
    geojson_json = json.dumps(geojson, separators=(",", ":"))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SFC Parent Pact — School-Level Heatmap</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
    <script src="https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
    <style>
        * {{{{ margin: 0; padding: 0; box-sizing: border-box; }}}}
        body {{{{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }}}}
        #map {{{{ width: 100vw; height: 100vh; }}}}

        .panel {{{{
            padding: 14px 18px;
            background: rgba(255,255,255,0.95);
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
            border-radius: 10px;
            max-width: 340px;
            max-height: 90vh;
            overflow-y: auto;
        }}}}
        .panel h2 {{{{
            font-size: 17px;
            color: #1a1a2e;
            margin-bottom: 2px;
        }}}}
        .panel .total {{{{
            font-size: 24px;
            font-weight: 800;
            color: #e63946;
        }}}}
        .panel .subtitle {{{{
            font-size: 12px;
            color: #666;
            margin-top: 2px;
        }}}}
        .panel .stats {{{{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 6px;
            margin: 10px 0;
            font-size: 12px;
        }}}}
        .panel .stat-box {{{{
            background: #f8f8f8;
            padding: 6px 8px;
            border-radius: 6px;
        }}}}
        .panel .stat-box .val {{{{
            font-size: 16px;
            font-weight: 700;
            color: #1a1a2e;
        }}}}
        .panel .stat-box .label {{{{
            color: #888;
            font-size: 11px;
        }}}}

        .controls {{{{
            margin-top: 10px;
            border-top: 1px solid #eee;
            padding-top: 10px;
        }}}}
        .controls h4 {{{{
            font-size: 13px;
            color: #333;
            margin-bottom: 6px;
        }}}}
        .controls label {{{{
            display: block;
            font-size: 12px;
            padding: 2px 0;
            cursor: pointer;
        }}}}
        .controls label input {{{{
            margin-right: 4px;
        }}}}

        .top-list {{{{
            margin-top: 10px;
            border-top: 1px solid #eee;
            padding-top: 8px;
        }}}}
        .top-list h4 {{{{
            font-size: 13px;
            color: #333;
            margin-bottom: 4px;
        }}}}
        .top-item {{{{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 3px 0;
            border-bottom: 1px solid #f5f5f5;
            font-size: 11px;
        }}}}
        .top-item .rank {{{{ color: #999; width: 18px; }}}}
        .top-item .name {{{{ flex: 1; color: #333; margin: 0 6px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}}}
        .top-item .count {{{{ font-weight: 600; color: #e63946; white-space: nowrap; }}}}

        .legend-panel {{{{
            padding: 10px 14px;
            background: rgba(255,255,255,0.92);
            box-shadow: 0 1px 5px rgba(0,0,0,0.3);
            border-radius: 8px;
            font-size: 12px;
        }}}}
        .legend-panel h4 {{{{
            font-size: 13px;
            color: #333;
            margin-bottom: 6px;
        }}}}
        .legend-row {{{{
            display: flex;
            align-items: center;
            padding: 2px 0;
        }}}}
        .legend-dot {{{{
            width: 14px;
            height: 14px;
            border-radius: 50%;
            margin-right: 8px;
            flex-shrink: 0;
        }}}}
        .legend-swatch {{{{
            width: 14px;
            height: 14px;
            margin-right: 8px;
            flex-shrink: 0;
            border: 1px solid #ccc;
        }}}}

        .search-box {{{{
            padding: 8px 12px;
            background: rgba(255,255,255,0.95);
            box-shadow: 0 2px 6px rgba(0,0,0,0.2);
            border-radius: 8px;
            min-width: 280px;
        }}}}
        .search-box input {{{{
            width: 100%;
            padding: 6px 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 13px;
            outline: none;
        }}}}
        .search-box input:focus {{{{ border-color: #e63946; }}}}
        .search-results {{{{
            max-height: 200px;
            overflow-y: auto;
            margin-top: 4px;
        }}}}
        .search-result {{{{
            padding: 5px 8px;
            cursor: pointer;
            font-size: 12px;
            border-bottom: 1px solid #f0f0f0;
        }}}}
        .search-result:hover {{{{ background: #f5f5f5; }}}}
        .search-result .sr-name {{{{ font-weight: 600; color: #333; }}}}
        .search-result .sr-meta {{{{ color: #888; font-size: 11px; }}}}

        .counter {{{{
            font-size: 11px;
            color: #666;
            margin-top: 6px;
        }}}}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        // ── School data: [lat, lng, pacts, pact_pct, name, pupils, address, school_id, postcode, low_age, high_age] ──
        const S = {schools_json};
        const TOTAL_PACTS = {total_pacts};
        const TOTAL_SCHOOLS = {total_schools};
        const countiesGeo = {geojson_json};

        // ── Top schools lists ──
        const topByPacts = {top_by_pacts_json};
        const topByPct = {top_by_pct_json};

        // ── Color functions ──
        // For % metric (school markers + county choropleth)
        function pctColor(pct) {{{{
            if (pct >= 50) return '#67000d';
            if (pct >= 35) return '#a50f15';
            if (pct >= 25) return '#cb181d';
            if (pct >= 15) return '#ef3b2c';
            if (pct >= 10) return '#fb6a4a';
            if (pct >= 5)  return '#fc9272';
            if (pct > 0)   return '#fcbba1';
            return '#d9d9d9';
        }}}}

        // For total pacts metric (county choropleth)
        function totalColor(pacts) {{{{
            if (pacts > 10000) return '#67000d';
            if (pacts > 8000)  return '#a50f15';
            if (pacts > 5000)  return '#cb181d';
            if (pacts > 3000)  return '#ef3b2c';
            if (pacts > 2000)  return '#fb6a4a';
            if (pacts > 1000)  return '#fc9272';
            if (pacts > 500)   return '#fcbba1';
            if (pacts > 100)   return '#fee0d2';
            if (pacts > 0)     return '#fff5f0';
            return '#f7f7f7';
        }}}}

        // For total pacts metric (school markers)
        function totalMarkerColor(pacts) {{{{
            if (pacts >= 200) return '#67000d';
            if (pacts >= 100) return '#a50f15';
            if (pacts >= 50)  return '#cb181d';
            if (pacts >= 30)  return '#ef3b2c';
            if (pacts >= 20)  return '#fb6a4a';
            if (pacts >= 10)  return '#fc9272';
            if (pacts > 0)    return '#fcbba1';
            return '#d9d9d9';
        }}}}

        function pctRadius(pacts) {{{{
            if (pacts >= 100) return 10;
            if (pacts >= 50) return 8;
            if (pacts >= 20) return 6;
            if (pacts >= 5) return 5;
            return 4;
        }}}}

        // ── Map setup ──
        const map = L.map('map', {{{{
            center: [54.5, -3.5],
            zoom: 6,
            zoomControl: true,
        }}}});

        L.tileLayer('https://{{{{s}}}}.basemaps.cartocdn.com/light_all/{{{{z}}}}/{{{{x}}}}/{{{{y}}}}{{{{r}}}}.png', {{{{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/">CARTO</a> | Data: <a href="https://smartphonefreechildhood.org">SFC</a>',
            subdomains: 'abcd',
            maxZoom: 18,
        }}}}).addTo(map);

        // ── State ──
        let activeView = 'heat'; // 'heat', 'markers', 'both', 'county'
        let activeMetric = 'pct'; // 'pct' or 'total'
        let showSecondaryOnly = false;
        let visibleSchools = S.length;

        // ── Layers ──
        // Heat layer
        function getHeatData() {{{{
            const filtered = showSecondaryOnly
                ? S.filter(s => s[10] >= 16 && s[9] <= 14)
                : S;
            if (activeMetric === 'pct') {{{{
                return filtered.map(s => [s[0], s[1], s[3] * 10]); // weight by pact_pct
            }}}} else {{{{
                return filtered.map(s => [s[0], s[1], s[2]]); // weight by pact count
            }}}}
        }}}}

        const heatLayer = L.heatLayer(getHeatData(), {{{{
            radius: 20,
            blur: 25,
            maxZoom: 12,
            max: activeMetric === 'pct' ? 500 : 200,
            gradient: {{{{
                0.0: '#fff5f0',
                0.2: '#fee0d2',
                0.4: '#fcbba1',
                0.5: '#fc9272',
                0.6: '#fb6a4a',
                0.7: '#ef3b2c',
                0.8: '#cb181d',
                0.9: '#a50f15',
                1.0: '#67000d'
            }}}}
        }}}});

        // Marker cluster layer
        const markers = L.markerClusterGroup({{{{
            chunkedLoading: true,
            maxClusterRadius: 50,
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: false,
            iconCreateFunction: function(cluster) {{{{
                const children = cluster.getAllChildMarkers();
                let totalPacts = 0;
                children.forEach(m => {{{{ totalPacts += m.options.pacts || 0; }}}});
                let dim = 30;
                if (totalPacts > 500) dim = 50;
                else if (totalPacts > 100) dim = 40;
                return L.divIcon({{{{
                    html: '<div style="background:rgba(230,57,70,0.8);color:white;border-radius:50%;width:'+dim+'px;height:'+dim+'px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;border:2px solid rgba(255,255,255,0.8);">' +
                        (totalPacts >= 1000 ? (totalPacts/1000).toFixed(1)+'k' : totalPacts) + '</div>',
                    className: '',
                    iconSize: [dim, dim]
                }}}});
            }}}}
        }}}});

        // County choropleth layer
        let countyLayer = null;

        function countyStyle(feature) {{{{
            const p = feature.properties;
            const val = activeMetric === 'pct' ? p.pact_pct : p.pacts;
            const color = activeMetric === 'pct' ? pctColor(val) : totalColor(val);
            return {{{{
                fillColor: color,
                weight: 1.5,
                opacity: 1,
                color: '#666',
                fillOpacity: 0.7,
            }}}};
        }}}}

        function onEachCounty(feature, layer) {{{{
            layer.on({{{{
                mouseover: function(e) {{{{
                    const l = e.target;
                    l.setStyle({{{{ weight: 3, color: '#333', fillOpacity: 0.85 }}}});
                    l.bringToFront();
                }}}},
                mouseout: function(e) {{{{
                    countyLayer.resetStyle(e.target);
                }}}},
                click: function(e) {{{{
                    const p = feature.properties;
                    const pctStr = p.pupils > 0 ? p.pact_pct + '%' : 'N/A';
                    let content = '<div style="min-width:200px">' +
                        '<div style="font-weight:700;font-size:14px;color:#1a1a2e">' + (p.county || p.name || 'Unknown') + '</div>';
                    if (p.label) {{{{
                        content += '<div style="font-size:11px;color:#888;margin:2px 0">' + p.label + '</div>';
                    }}}}
                    content += '<div style="margin:8px 0;display:grid;grid-template-columns:1fr 1fr;gap:6px">' +
                        '<div style="background:#fff5f0;padding:6px;border-radius:4px;text-align:center">' +
                            '<div style="font-size:20px;font-weight:800;color:#e63946">' + (p.pacts || 0).toLocaleString() + '</div>' +
                            '<div style="font-size:10px;color:#888">pacts signed</div>' +
                        '</div>' +
                        '<div style="background:#f0f7ff;padding:6px;border-radius:4px;text-align:center">' +
                            '<div style="font-size:20px;font-weight:800;color:#1a6dd4">' + pctStr + '</div>' +
                            '<div style="font-size:10px;color:#888">of ' + (p.pupils || 0).toLocaleString() + ' pupils</div>' +
                        '</div>' +
                    '</div>' +
                    '<div style="font-size:11px;color:#666">' + (p.school_count || 0) + ' schools</div>' +
                    '</div>';
                    L.popup().setLatLng(e.latlng).setContent(content).openOn(map);
                }}}}
            }}}});
        }}}}

        function buildCountyLayer() {{{{
            if (countyLayer) map.removeLayer(countyLayer);
            countyLayer = L.geoJson(countiesGeo, {{{{
                style: countyStyle,
                onEachFeature: onEachCounty,
                filter: function(feature) {{{{
                    return feature.properties.county != null;
                }}}}
            }}}});
            return countyLayer;
        }}}}

        function buildSfcLink(schoolId) {{{{
            return 'https://smartphonefreechildhood.org/parent-pact-results?school=' + schoolId;
        }}}}

        function addMarkers() {{{{
            markers.clearLayers();
            let count = 0;
            S.forEach(function(s) {{{{
                if (showSecondaryOnly && !(s[10] >= 16 && s[9] <= 14)) return;
                count++;
                const fillColor = activeMetric === 'pct' ? pctColor(s[3]) : totalMarkerColor(s[2]);
                const marker = L.circleMarker([s[0], s[1]], {{{{
                    radius: pctRadius(s[2]),
                    fillColor: fillColor,
                    color: '#fff',
                    weight: 1,
                    opacity: 0.9,
                    fillOpacity: 0.85,
                    pacts: s[2],
                }}}});

                const isSecondary = s[10] >= 16 && s[9] <= 14;
                const level = isSecondary ? 'Secondary' : (s[10] <= 11 ? 'Primary' : 'Other');
                const pctStr = s[5] > 0 ? s[3] + '%' : 'N/A';

                marker.bindPopup(
                    '<div style="min-width:200px">' +
                    '<div style="font-weight:700;font-size:14px;color:#1a1a2e">' + s[4] + '</div>' +
                    '<div style="font-size:11px;color:#888;margin:2px 0">' + s[6] + ', ' + s[8] + '</div>' +
                    '<div style="margin:8px 0;display:grid;grid-template-columns:1fr 1fr;gap:6px">' +
                        '<div style="background:#fff5f0;padding:6px;border-radius:4px;text-align:center">' +
                            '<div style="font-size:20px;font-weight:800;color:#e63946">' + s[2] + '</div>' +
                            '<div style="font-size:10px;color:#888">pacts signed</div>' +
                        '</div>' +
                        '<div style="background:#f0f7ff;padding:6px;border-radius:4px;text-align:center">' +
                            '<div style="font-size:20px;font-weight:800;color:#1a6dd4">' + pctStr + '</div>' +
                            '<div style="font-size:10px;color:#888">of ' + (s[5] || '?') + ' pupils</div>' +
                        '</div>' +
                    '</div>' +
                    '<div style="font-size:11px;color:#666">' + level + ' (ages ' + s[9] + '-' + s[10] + ')</div>' +
                    '<a href="' + buildSfcLink(s[7]) + '" target="_blank" style="font-size:11px;color:#e63946">View on SFC &rarr;</a>' +
                    '</div>',
                    {{{{ maxWidth: 280 }}}}
                );
                markers.addLayer(marker);
            }}}});
            visibleSchools = count;
            updateCounter();
        }}}}

        function updateHeatData() {{{{
            heatLayer.setLatLngs(getHeatData());
            heatLayer.setOptions({{{{ max: activeMetric === 'pct' ? 500 : 200 }}}});
            const filtered = showSecondaryOnly
                ? S.filter(s => s[10] >= 16 && s[9] <= 14)
                : S;
            visibleSchools = filtered.length;
            updateCounter();
        }}}}

        function setView(view) {{{{
            activeView = view;
            map.removeLayer(heatLayer);
            map.removeLayer(markers);
            if (countyLayer) map.removeLayer(countyLayer);

            if (view === 'heat' || view === 'both') {{{{
                map.addLayer(heatLayer);
                updateHeatData();
            }}}}
            if (view === 'markers' || view === 'both') {{{{
                addMarkers();
                map.addLayer(markers);
            }}}}
            if (view === 'county') {{{{
                buildCountyLayer();
                map.addLayer(countyLayer);
            }}}}

            // Update radio buttons
            document.querySelectorAll('input[name="view"]').forEach(r => {{{{
                r.checked = r.value === view;
            }}}});

            // Show/hide secondary filter (not relevant for county view)
            const secFilter = document.getElementById('secFilterWrap');
            if (secFilter) secFilter.style.display = view === 'county' ? 'none' : '';

            updateLegend();
            updateLeaderboard();
        }}}}

        function setMetric(metric) {{{{
            activeMetric = metric;
            document.querySelectorAll('input[name="metric"]').forEach(r => {{{{
                r.checked = r.value === metric;
            }}}});

            // Re-render active view
            if (activeView === 'heat' || activeView === 'both') updateHeatData();
            if (activeView === 'markers' || activeView === 'both') addMarkers();
            if (activeView === 'county' && countyLayer) {{{{
                buildCountyLayer();
                map.addLayer(countyLayer);
            }}}}

            updateLegend();
            updateLeaderboard();
        }}}}

        // Start with heat view
        map.addLayer(heatLayer);

        function updateCounter() {{{{
            const el = document.getElementById('counter');
            if (el) {{{{
                if (activeView === 'county') {{{{
                    el.textContent = 'County view';
                }}}} else {{{{
                    el.textContent = visibleSchools.toLocaleString() + ' of ' + TOTAL_SCHOOLS.toLocaleString() + ' schools shown';
                }}}}
            }}}}
        }}}}

        function updateLegend() {{{{
            const el = document.getElementById('legendContent');
            if (!el) return;

            if (activeView === 'county' && activeMetric === 'total') {{{{
                el.innerHTML = '<h4>Total pacts</h4>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#67000d"></div>10,000+</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#a50f15"></div>8,000-10,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#cb181d"></div>5,000-8,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#ef3b2c"></div>3,000-5,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fb6a4a"></div>2,000-3,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fc9272"></div>1,000-2,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fcbba1"></div>500-1,000</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fee0d2"></div>100-500</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fff5f0"></div>1-100</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#f7f7f7"></div>No data</div>';
            }}}} else if (activeMetric === 'total') {{{{
                el.innerHTML = '<h4>Total pacts</h4>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#67000d"></div>200+</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#a50f15"></div>100-200</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#cb181d"></div>50-100</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#ef3b2c"></div>30-50</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fb6a4a"></div>20-30</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fc9272"></div>10-20</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fcbba1"></div>1-10</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#d9d9d9"></div>No data</div>' +
                    '<div style="margin-top:6px;font-size:10px;color:#888">Marker size = pact count</div>';
            }}}} else if (activeView === 'county') {{{{
                el.innerHTML = '<h4>Pact engagement %</h4>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#67000d"></div>50%+</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#a50f15"></div>35-50%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#cb181d"></div>25-35%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#ef3b2c"></div>15-25%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fb6a4a"></div>10-15%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fc9272"></div>5-10%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#fcbba1"></div>1-5%</div>' +
                    '<div class="legend-row"><div class="legend-swatch" style="background:#d9d9d9"></div>No data</div>';
            }}}} else {{{{
                el.innerHTML = '<h4>Pact engagement %</h4>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#67000d"></div>50%+</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#a50f15"></div>35-50%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#cb181d"></div>25-35%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#ef3b2c"></div>15-25%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fb6a4a"></div>10-15%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fc9272"></div>5-10%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#fcbba1"></div>1-5%</div>' +
                    '<div class="legend-row"><div class="legend-dot" style="background:#d9d9d9"></div>No data</div>' +
                    '<div style="margin-top:6px;font-size:10px;color:#888">Marker size = pact count<br>Marker colour = % of pupils</div>';
            }}}}
        }}}}

        function updateLeaderboard() {{{{
            const el = document.getElementById('leaderboard');
            if (!el) return;

            const items = activeMetric === 'total' ? topByPacts : topByPct;
            const title = activeMetric === 'total' ? 'Top 15 schools by pacts' : 'Top 15 schools by %';
            let html = '<h4>' + title + '</h4>';
            items.slice(0, 15).forEach(function(s, i) {{{{
                const display = activeMetric === 'total'
                    ? s.p + ' (' + s.pct + '%)'
                    : s.pct + '% (' + s.p + ')';
                html += '<div class="top-item">' +
                    '<span class="rank">' + (i+1) + '</span>' +
                    '<span class="name" title="' + s.n + '">' + s.n + '</span>' +
                    '<span class="count">' + display + '</span>' +
                '</div>';
            }}}});
            el.innerHTML = html;
        }}}}

        // ── Header panel ──
        const header = L.control({{{{ position: 'topleft' }}}});
        header.onAdd = function() {{{{
            const div = L.DomUtil.create('div', 'panel');

            div.innerHTML =
                '<h2>SFC Parent Pact</h2>' +
                '<div style="font-size:13px;color:#555">School-Level Engagement</div>' +
                '<div class="total">{total_pacts:,} pacts signed</div>' +
                '<div class="subtitle">Across {total_schools:,} schools &middot; {extraction_date}</div>' +
                '<div class="stats">' +
                    '<div class="stat-box"><div class="val">{total_schools:,}</div><div class="label">Schools</div></div>' +
                    '<div class="stat-box"><div class="val">{avg_pact_pct:.1f}%</div><div class="label">Avg engagement</div></div>' +
                '</div>' +
                '<div class="controls">' +
                    '<h4>View mode</h4>' +
                    '<label><input type="radio" name="view" value="heat" checked onchange="setView(this.value)"> Heatmap</label>' +
                    '<label><input type="radio" name="view" value="markers" onchange="setView(this.value)"> Markers</label>' +
                    '<label><input type="radio" name="view" value="both" onchange="setView(this.value)"> Both</label>' +
                    '<label><input type="radio" name="view" value="county" onchange="setView(this.value)"> County</label>' +
                '</div>' +
                '<div class="controls">' +
                    '<h4>Metric</h4>' +
                    '<label><input type="radio" name="metric" value="pct" checked onchange="setMetric(this.value)"> % of pupils</label>' +
                    '<label><input type="radio" name="metric" value="total" onchange="setMetric(this.value)"> Total pacts</label>' +
                '</div>' +
                '<div id="secFilterWrap" style="margin-top:8px">' +
                    '<label><input type="checkbox" id="secOnly" onchange="toggleSecondary(this.checked)"> Secondary schools only</label>' +
                '</div>' +
                '<div class="top-list" id="leaderboard">' +
                    '<h4>Top 15 schools by pacts</h4>' +
                '</div>' +
                '<div class="counter" id="counter">{total_schools:,} of {total_schools:,} schools shown</div>';

            L.DomEvent.disableClickPropagation(div);
            L.DomEvent.disableScrollPropagation(div);
            return div;
        }}}};
        header.addTo(map);

        // Initialize leaderboard after panel is added
        updateLeaderboard();

        function toggleSecondary(checked) {{{{
            showSecondaryOnly = checked;
            if (activeView === 'heat' || activeView === 'both') updateHeatData();
            if (activeView === 'markers' || activeView === 'both') addMarkers();
        }}}}

        // ── Search ──
        const search = L.control({{{{ position: 'topright' }}}});
        search.onAdd = function() {{{{
            const div = L.DomUtil.create('div', 'search-box');
            div.innerHTML = '<input type="text" id="searchInput" placeholder="Search schools...">' +
                '<div class="search-results" id="searchResults"></div>';
            L.DomEvent.disableClickPropagation(div);
            L.DomEvent.disableScrollPropagation(div);
            return div;
        }}}};
        search.addTo(map);

        document.getElementById('searchInput').addEventListener('input', function(e) {{{{
            const q = e.target.value.toLowerCase().trim();
            const results = document.getElementById('searchResults');
            if (q.length < 2) {{{{ results.innerHTML = ''; return; }}}}

            const matches = S.filter(s => s[4].toLowerCase().includes(q)).slice(0, 15);
            results.innerHTML = matches.map(function(s) {{{{
                const pctStr = s[5] > 0 ? ' (' + s[3] + '%)' : '';
                return '<div class="search-result" onclick="zoomTo(' + s[0] + ',' + s[1] + ',\\''+s[4].replace(/'/g,"\\\\'")+'\\','+s[2]+')">' +
                    '<div class="sr-name">' + s[4] + '</div>' +
                    '<div class="sr-meta">' + s[2] + ' pacts' + pctStr + ' &middot; ' + s[8] + '</div>' +
                '</div>';
            }}}}).join('');
        }}}});

        function zoomTo(lat, lng, name, pacts) {{{{
            map.setView([lat, lng], 14);
            if (activeView === 'heat' || activeView === 'county') {{{{
                setView('both');
            }}}}
            L.popup()
                .setLatLng([lat, lng])
                .setContent('<b>' + name + '</b><br>' + pacts + ' pacts signed')
                .openOn(map);
            document.getElementById('searchResults').innerHTML = '';
            document.getElementById('searchInput').value = '';
        }}}}

        // ── Legend ──
        const legend = L.control({{{{ position: 'bottomright' }}}});
        legend.onAdd = function() {{{{
            const div = L.DomUtil.create('div', 'legend-panel');
            div.innerHTML = '<div id="legendContent"></div>';
            return div;
        }}}};
        legend.addTo(map);
        updateLegend();
    </script>
</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Written {output_path} ({size_mb:.1f} MB)")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("SFC Parent Pact — School-Level Heatmap Builder")
    print("=" * 60)

    # Step 1: Extract school data (with region)
    schools, region_data = extract_school_data()

    # Step 2: Geocode postcodes
    schools = geocode_postcodes(schools)

    # Step 3: Calculate metrics
    schools = calculate_metrics(schools)

    # Save intermediate data
    data_path = os.path.join(DATA_DIR, "sfc_school_pacts.json")
    with open(data_path, "w") as f:
        json.dump(schools, f, separators=(",", ":"))
    print(f"\n  Saved {len(schools):,} schools to {data_path}")

    # Step 3b: Export CSV
    export_csv(schools)

    # Step 4: Build county aggregation
    geojson = build_county_data(schools, region_data)

    # Step 5: Generate HTML
    output_path = os.path.join(SCRIPT_DIR, "index.html")
    generate_html(schools, geojson, output_path)

    print("\n" + "=" * 60)
    print("DONE! Open index.html in your browser.")
    print("=" * 60)


if __name__ == "__main__":
    main()
