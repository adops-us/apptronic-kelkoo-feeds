import os
import json
import datetime
import pathlib
import xml.etree.ElementTree as ET
from typing import Any, Dict, List
import requests

# ===== Env / constants =====
KELKOO_TOKEN_1 = os.getenv("KELKOO_TOKEN_1", "")
KELKOO_MERCHANT_FEED_URL = os.getenv("KELKOO_MERCHANT_FEED_URL", "")

# Fail fast with a clear message
REQUIRED_ENVS = ["KELKOO_TOKEN_1"]
missing = [k for k in REQUIRED_ENVS if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing required envs: {', '.join(missing)}")

# Adjust these to the EXACT param keys from your Merchant Feeds docs:
PARAM_SPOTLIGHT_KEY = "spotlight"         # e.g., "spotlight" or "onlySpotlight"
PARAM_MATCH_KEY = "merchantMatch"         # e.g., "merchantMatch" or "matchedOnly"
PARAM_COUNTRY_KEY = "country"             # if country scoping is needed

DEFAULT_COUNTRY = "us"                    # change if needed
ENABLE_SPOTLIGHT_ONLY = "no"             # set True to pull only Spotlight merchants
ENABLE_MATCH_ONLY = "yes"                  # set True to pull only “matched” merchants

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
DOCS.mkdir(exist_ok=True)
(LOGS := DOCS / "logs").mkdir(exist_ok=True)

def now_utc() -> datetime.datetime:
    try:
        return datetime.datetime.now(datetime.UTC)
    except Exception:
        return datetime.datetime.utcnow()

def ts() -> str:
    return now_utc().strftime("%Y%m%d-%H%M%S")

def fetch_from_kelkoo() -> List[Dict[str, Any]]:
    if not KELKOO_TOKEN_1:
        raise RuntimeError("KELKOO_TOKEN_1 is not set")
    if not KELKOO_MERCHANT_FEED_URL:
        raise RuntimeError("KELKOO_MERCHANT_FEED_URL is not set")

    headers = {"Authorization": f"Bearer {KELKOO_TOKEN_1}"}

    # Build params exactly as your Merchant Feed doc specifies.
    params = {}
    if PARAM_COUNTRY_KEY:
        params[PARAM_COUNTRY_KEY] = DEFAULT_COUNTRY
    if PARAM_SPOTLIGHT_KEY:
        params[PARAM_SPOTLIGHT_KEY] = str(ENABLE_SPOTLIGHT_ONLY).lower()
    if PARAM_MATCH_KEY:
        params[PARAM_MATCH_KEY] = str(ENABLE_MATCH_ONLY).lower()

    r = requests.get(KELKOO_MERCHANT_FEED_URL, headers=headers, params=params, timeout=60)
    r.raise_for_status()

    # Try JSON first; if it fails, parse XML
    try:
        data = r.json()
        if isinstance(data, dict):
            # Adjust to your response shape; common containers shown below:
            for key in ("merchants", "data", "items", "results"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # if nothing matched and dict itself is one merchant:
            return [data]
        elif isinstance(data, list):
            return data
    except ValueError:
        pass  # not JSON

    # XML fallback (adjust tag names to your doc)
    merchants = []
    root = ET.fromstring(r.content)
    for m in root.findall(".//Merchant"):
        merchants.append({
            "merchant_id": (m.findtext("MerchantID") or "").strip(),
            "merchant_name": (m.findtext("MerchantName") or "").strip(),
            "domain": (m.findtext("Domain") or "").strip(),
            "country": (m.findtext("Country") or "").strip(),
            "logo": (m.findtext("LogoUrl") or "").strip(),
            "homepage": (m.findtext("HomepageUrl") or "").strip(),
        })
    return merchants

def normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Map provider fields -> stable schema.
    After your first run, look at docs/merchants_*.json and tighten these mappings.
    """
    out = []
    for r in rows:
        merchant_id   = str(r.get("merchant_id") or r.get("merchantId") or r.get("id") or "")
        merchant_name = r.get("merchant_name") or r.get("merchantName") or r.get("name") or ""
        domain        = r.get("domain") or r.get("root_domain") or r.get("website") or ""
        country       = (r.get("country") or r.get("country_code") or DEFAULT_COUNTRY).upper()
        logo          = r.get("logo") or r.get("logo_url") or r.get("image") or ""
        homepage      = r.get("homepage") or r.get("merchant_url") or (f"https://{domain}" if domain else "")

        out.append({
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "domain": domain,
            "country": country,
            "logo": logo,
            "homepage": homepage,
            "updated_at": now_utc().isoformat(),
        })
    return out

def write_xml(rows: List[Dict[str, Any]], path: pathlib.Path):
    root = ET.Element("Merchants", updated=now_utc().isoformat())
    for r in rows:
        m = ET.SubElement(root, "Merchant")
        ET.SubElement(m, "MerchantID").text = r["merchant_id"]
        ET.SubElement(m, "MerchantName").text = r["merchant_name"]
        ET.SubElement(m, "Domain").text = r["domain"]
        ET.SubElement(m, "Country").text = r["country"]
        ET.SubElement(m, "LogoUrl").text = r["logo"]
        ET.SubElement(m, "HomepageUrl").text = r["homepage"]
        ET.SubElement(m, "UpdatedAt").text = r["updated_at"]
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)

def write_json(rows: List[Dict[str, Any]], path: pathlib.Path):
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    merchants = fetch_from_kelkoo()
    rows = normalize(merchants)

    stamp = ts()
    xml_ts = DOCS / f"merchants_{stamp}.xml"
    json_ts = DOCS / f"merchants_{stamp}.json"
    xml_latest = DOCS / "merchants_latest.xml"
    json_latest = DOCS / "merchants_latest.json"

    write_xml(rows, xml_ts)
    write_json(rows, json_ts)
    write_xml(rows, xml_latest)
    write_json(rows, json_latest)

    # (optional) very simple health file
    (DOCS / "health.json").write_text(
        json.dumps(
            {"last_count": len(rows), "last_updated": now_utc().isoformat(), "status": "ok", "note": ""},
            indent=2
        ),
        "utf-8"
    )

if __name__ == "__main__":
    main()
