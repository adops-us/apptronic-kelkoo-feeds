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

# ---- helpers for safe gets & XML building ----
def _sg(d: Dict[str, Any], key: str, default: Any = None):
    """Safe get with default."""
    val = d.get(key, default)
    return default if val is None else val

def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes"}
    return False

def _text(elem: ET.Element, name: str, value: Any):
    ET.SubElement(elem, name).text = "" if value is None else str(value)

def _list(parent: ET.Element, name: str, items: List[Any], item_tag: str, map_fn):
    container = ET.SubElement(parent, name)
    for it in (items or []):
        map_fn(ET.SubElement(container, item_tag), it)

def _dict_as_json(parent: ET.Element, name: str, obj: Dict[str, Any]):
    # For arbitrary objects from API, store as compact JSON in XML
    _text(parent, name, json.dumps(obj or {}, ensure_ascii=False, separators=(",", ":")))

# ---- normalization to a rich, stable schema mirroring Kelkoo fields ----
def normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Pass through Kelkoo fields (when present) exactly as requested, while also keeping
    your legacy keys for compatibility. Defaults used when fields are missing.
    """
    out = []
    for r in rows:
        # Legacy/compat mapping (kept)
        merchant_id   = str(r.get("merchant_id") or r.get("merchantId") or r.get("id") or "")
        merchant_name = r.get("merchant_name") or r.get("merchantName") or r.get("name") or ""
        domain        = r.get("domain") or r.get("root_domain") or r.get("website") or ""
        country       = (r.get("country") or r.get("country_code") or DEFAULT_COUNTRY).upper()
        logo          = r.get("logo") or r.get("logo_url") or r.get("image") or r.get("logoUrl") or ""
        homepage      = r.get("homepage") or r.get("merchant_url") or r.get("url") or (f"https://{domain}" if domain else "")

        # Exact Kelkoo fields per your example (mirror names)
        k_id = _sg(r, "id", merchant_id or 0)
        k_name = _sg(r, "name", merchant_name)
        k_url = _sg(r, "url", homepage)
        k_summary = _sg(r, "summary", "")
        k_logoUrl = _sg(r, "logoUrl", logo)
        k_websiteId = _sg(r, "websiteId", 0)
        k_deliveryCountries = _sg(r, "deliveryCountries", []) or []
        k_categories = _sg(r, "categories", []) or []
        k_directoryLinks = _sg(r, "directoryLinks", []) or []
        k_supportsLinks = _to_bool(_sg(r, "supportsLinks", False))
        k_supportsLinksOfferMatch = _to_bool(_sg(r, "supportsLinksOfferMatch", False))
        k_supportsLinksMerchantMatch = _to_bool(_sg(r, "supportsLinksMerchantMatch", False))
        k_forbiddenTrafficTypes = _sg(r, "forbiddenTrafficTypes", []) or []
        k_targetCos = _sg(r, "targetCos", {}) or {}
        k_currency = _sg(r, "currency", "")
        k_visibilityRecentlyChanged = _sg(r, "visibilityRecentlyChanged", {}) or {}
        k_visible = _to_bool(_sg(r, "visible", True))
        k_isNew = _to_bool(_sg(r, "isNew", False))
        k_merchantEstimatedCpc = _sg(r, "merchantEstimatedCpc", 0)
        k_merchantMobileEstimatedCpc = _sg(r, "merchantMobileEstimatedCpc", 0)
        k_merchantTier = _sg(r, "merchantTier", "")
        k_topNetworkCpc = _sg(r, "topNetworkCpc", 0)
        k_topNetworkCpcLinks = _sg(r, "topNetworkCpcLinks", 0)
        k_topNetworkVpl = _sg(r, "topNetworkVpl", 0)
        k_topNetworkVplLinks = _sg(r, "topNetworkVplLinks", 0)
        k_cpcsBySubId = _sg(r, "cpcsBySubId", []) or []
        k_cpcsBySubIdForLinks = _sg(r, "cpcsBySubIdForLinks", []) or []
        k_spotlight = _to_bool(_sg(r, "spotlight", False))
        k_prospectionValues = _sg(r, "prospectionValues", []) or []

        out.append({
            # Legacy/compat block
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "domain": domain,
            "country": country,
            "logo": logo,
            "homepage": homepage,
            "updated_at": now_utc().isoformat(),

            # Kelkoo block (exact field names)
            "id": k_id,
            "name": k_name,
            "url": k_url,
            "summary": k_summary,
            "logoUrl": k_logoUrl,
            "websiteId": k_websiteId,
            "deliveryCountries": k_deliveryCountries,
            "categories": [
                {
                    "id": _sg(c, "id", 0),
                    "name": _sg(c, "name", ""),
                    "numberOfOffers": _sg(c, "numberOfOffers", 0),
                    "estimatedCpc": _sg(c, "estimatedCpc", 0),
                    "mobileEstimatedCpc": _sg(c, "mobileEstimatedCpc", 0),
                } for c in k_categories
            ],
            "directoryLinks": [
                {
                    "urlTemplate": _sg(d, "urlTemplate", ""),
                    "estimatedCpc": _sg(d, "estimatedCpc", 0),
                    "mobileEstimatedCpc": _sg(d, "mobileEstimatedCpc", 0),
                    "eventType": _sg(d, "eventType", ""),
                    "eventStartDate": _sg(d, "eventStartDate", 0),
                    "eventEndDate": _sg(d, "eventEndDate", 0),
                } for d in k_directoryLinks
            ],
            "supportsLinks": k_supportsLinks,
            "supportsLinksOfferMatch": k_supportsLinksOfferMatch,
            "supportsLinksMerchantMatch": k_supportsLinksMerchantMatch,
            "forbiddenTrafficTypes": k_forbiddenTrafficTypes,
            "targetCos": k_targetCos,
            "currency": k_currency,
            "visibilityRecentlyChanged": k_visibilityRecentlyChanged,
            "visible": k_visible,
            "isNew": k_isNew,
            "merchantEstimatedCpc": k_merchantEstimatedCpc,
            "merchantMobileEstimatedCpc": k_merchantMobileEstimatedCpc,
            "merchantTier": k_merchantTier,
            "topNetworkCpc": k_topNetworkCpc,
            "topNetworkCpcLinks": k_topNetworkCpcLinks,
            "topNetworkVpl": k_topNetworkVpl,
            "topNetworkVplLinks": k_topNetworkVplLinks,
            "cpcsBySubId": [
                {
                    "id": _sg(s, "id", ""),
                    "estimatedCpc": _sg(s, "estimatedCpc", 0),
                    "mobileEstimatedCpc": _sg(s, "mobileEstimatedCpc", 0),
                } for s in k_cpcsBySubId
            ],
            "cpcsBySubIdForLinks": [
                {
                    "id": _sg(s, "id", ""),
                    "estimatedCpc": _sg(s, "estimatedCpc", 0),
                    "mobileEstimatedCpc": _sg(s, "mobileEstimatedCpc", 0),
                } for s in k_cpcsBySubIdForLinks
            ],
            "spotlight": k_spotlight,
            "prospectionValues": [
                {
                    "simulatedVpl": _sg(p, "simulatedVpl", ""),
                    "simulatedDesktopCpc": _sg(p, "simulatedDesktopCpc", 0),
                    "simulatedMobileCpc": _sg(p, "simulatedMobileCpc", 0),
                } for p in k_prospectionValues
            ],
        })
    return out

# ---- XML writer including all Kelkoo fields ----
def write_xml(rows: List[Dict[str, Any]], path: pathlib.Path):
    def map_category(elem: ET.Element, c: Dict[str, Any]):
        _text(elem, "Id", _sg(c, "id", 0))
        _text(elem, "Name", _sg(c, "name", ""))
        _text(elem, "NumberOfOffers", _sg(c, "numberOfOffers", 0))
        _text(elem, "EstimatedCpc", _sg(c, "estimatedCpc", 0))
        _text(elem, "MobileEstimatedCpc", _sg(c, "mobileEstimatedCpc", 0))

    def map_directory_link(elem: ET.Element, d: Dict[str, Any]):
        _text(elem, "UrlTemplate", _sg(d, "urlTemplate", ""))
        _text(elem, "EstimatedCpc", _sg(d, "estimatedCpc", 0))
        _text(elem, "MobileEstimatedCpc", _sg(d, "mobileEstimatedCpc", 0))
        _text(elem, "EventType", _sg(d, "eventType", ""))
        _text(elem, "EventStartDate", _sg(d, "eventStartDate", 0))
        _text(elem, "EventEndDate", _sg(d, "eventEndDate", 0))

    def map_cpc_by_subid(elem: ET.Element, s: Dict[str, Any]):
        _text(elem, "Id", _sg(s, "id", ""))
        _text(elem, "EstimatedCpc", _sg(s, "estimatedCpc", 0))
        _text(elem, "MobileEstimatedCpc", _sg(s, "mobileEstimatedCpc", 0))

    def map_prospection(elem: ET.Element, p: Dict[str, Any]):
        _text(elem, "SimulatedVpl", _sg(p, "simulatedVpl", ""))
        _text(elem, "SimulatedDesktopCpc", _sg(p, "simulatedDesktopCpc", 0))
        _text(elem, "SimulatedMobileCpc", _sg(p, "simulatedMobileCpc", 0))

    root = ET.Element("Merchants", updated=now_utc().isoformat())
    for r in rows:
        m = ET.SubElement(root, "Merchant")

        # Legacy/compat block
        _text(m, "MerchantID", _sg(r, "merchant_id", ""))
        _text(m, "MerchantName", _sg(r, "merchant_name", ""))
        _text(m, "Domain", _sg(r, "domain", ""))
        _text(m, "Country", _sg(r, "country", ""))
        _text(m, "LogoUrlCompat", _sg(r, "logo", ""))  # avoid clashing with Kelkoo 'logoUrl'
        _text(m, "HomepageUrlCompat", _sg(r, "homepage", ""))
        _text(m, "UpdatedAt", _sg(r, "updated_at", ""))

        # Kelkoo block (exact field names but in XML-friendly casing)
        _text(m, "Id", _sg(r, "id", 0))
        _text(m, "Name", _sg(r, "name", ""))
        _text(m, "Url", _sg(r, "url", ""))
        _text(m, "Summary", _sg(r, "summary", ""))
        _text(m, "LogoUrl", _sg(r, "logoUrl", ""))
        _text(m, "WebsiteId", _sg(r, "websiteId", 0))
        _list(m, "DeliveryCountries", _sg(r, "deliveryCountries", []), "Country", lambda e, x: _text(e, "Code", x))

        _list(m, "Categories", _sg(r, "categories", []), "Category", map_category)
        _list(m, "DirectoryLinks", _sg(r, "directoryLinks", []), "DirectoryLink", map_directory_link)

        _text(m, "SupportsLinks", str(_to_bool(_sg(r, "supportsLinks", False))).lower())
        _text(m, "SupportsLinksOfferMatch", str(_to_bool(_sg(r, "supportsLinksOfferMatch", False))).lower())
        _text(m, "SupportsLinksMerchantMatch", str(_to_bool(_sg(r, "supportsLinksMerchantMatch", False))).lower())

        _list(m, "ForbiddenTrafficTypes", _sg(r, "forbiddenTrafficTypes", []), "Type", lambda e, x: _text(e, "Name", x))

        _dict_as_json(m, "TargetCos", _sg(r, "targetCos", {}))
        _text(m, "Currency", _sg(r, "currency", ""))

        _dict_as_json(m, "VisibilityRecentlyChanged", _sg(r, "visibilityRecentlyChanged", {}))

        _text(m, "Visible", str(_to_bool(_sg(r, "visible", True))).lower())
        _text(m, "IsNew", str(_to_bool(_sg(r, "isNew", False))).lower())

        _text(m, "MerchantEstimatedCpc", _sg(r, "merchantEstimatedCpc", 0))
        _text(m, "MerchantMobileEstimatedCpc", _sg(r, "merchantMobileEstimatedCpc", 0))
        _text(m, "MerchantTier", _sg(r, "merchantTier", ""))

        _text(m, "TopNetworkCpc", _sg(r, "topNetworkCpc", 0))
        _text(m, "TopNetworkCpcLinks", _sg(r, "topNetworkCpcLinks", 0))
        _text(m, "TopNetworkVpl", _sg(r, "topNetworkVpl", 0))
        _text(m, "TopNetworkVplLinks", _sg(r, "topNetworkVplLinks", 0))

        _list(m, "CpcsBySubId", _sg(r, "cpcsBySubId", []), "SubId", map_cpc_by_subid)
        _list(m, "CpcsBySubIdForLinks", _sg(r, "cpcsBySubIdForLinks", []), "SubId", map_cpc_by_subid)

        _text(m, "Spotlight", str(_to_bool(_sg(r, "spotlight", False))).lower())
        _list(m, "ProspectionValues", _sg(r, "prospectionValues", []), "Prospection", map_prospection)

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
