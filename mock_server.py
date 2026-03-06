"""
Mock server for simulating external data sources.

Provides realistic law firm data with intentional imperfections:
- GET /firms: paginated, ~10% random 500 errors, 20 req/min rate limit enforced (429)
- GET /firms/{id}/firmographic: ~20% missing fields, occasional schema inconsistency
- GET /firms/{id}/contact: ~30% null email or LinkedIn URL
- POST /webhooks/crm: accepts lead payloads, ~5% failures
- POST /webhooks/email: accepts campaign payloads, ~5% failures

Dataset includes near-duplicate firms to test deduplication logic.
"""

import random
import time
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any

app = FastAPI(title="GTM Mock Server")

# ---------------------------------------------------------------------------
# Realistic law firm dataset (50 firms)
# ---------------------------------------------------------------------------

PRACTICE_AREAS = [
    "Corporate Law", "Litigation", "Real Estate", "Employment Law",
    "Intellectual Property", "Tax", "Banking & Finance", "Environmental Law",
    "Family Law", "Criminal Defense", "Immigration", "Healthcare",
    "Mergers & Acquisitions", "Bankruptcy", "Insurance Defense",
]

FIRMS = [
    {"id": "firm_001", "name": "Baker & Sterling LLP", "domain": "bakersterling.com", "region": "CA", "country": "US", "practice_areas": ["Corporate Law", "Mergers & Acquisitions"], "num_lawyers": 210},
    {"id": "firm_002", "name": "Thornton Hughes Partners", "domain": "thorntonhughes.com", "region": "NY", "country": "US", "practice_areas": ["Litigation", "Employment Law"], "num_lawyers": 145},
    {"id": "firm_003", "name": "Clarke Whitfield", "domain": "clarkewhitfield.com.au", "region": "NSW", "country": "Australia", "practice_areas": ["Real Estate", "Banking & Finance"], "num_lawyers": 88},
    {"id": "firm_004", "name": "Matsuda & Ono Legal", "domain": "matsudaono.jp", "region": "JP", "country": "Japan", "practice_areas": ["Corporate Law", "Intellectual Property"], "num_lawyers": 62},
    {"id": "firm_005", "name": "Prescott Law Group", "domain": "prescottlaw.com", "region": "FL", "country": "US", "practice_areas": ["Litigation", "Insurance Defense"], "num_lawyers": 175},
    {"id": "firm_006", "name": "Ashford & Crane", "domain": "ashfordcrane.com", "region": "MA", "country": "US", "practice_areas": ["Tax", "Corporate Law"], "num_lawyers": 320},
    {"id": "firm_007", "name": "Lim & Partners", "domain": "limpartners.sg", "region": "SG", "country": "Singapore", "practice_areas": ["Banking & Finance", "Corporate Law"], "num_lawyers": 54},
    {"id": "firm_008", "name": "Henderson Marks", "domain": "hendersonmarks.com.au", "region": "VIC", "country": "Australia", "practice_areas": ["Employment Law", "Litigation"], "num_lawyers": 102},
    {"id": "firm_009", "name": "Rivera & Goldstein", "domain": "riveragoldstein.com", "region": "CA", "country": "US", "practice_areas": ["Real Estate", "Environmental Law"], "num_lawyers": 93},
    {"id": "firm_010", "name": "Whitmore Chambers", "domain": "whitmorechambers.com", "region": "GA", "country": "US", "practice_areas": ["Criminal Defense", "Litigation"], "num_lawyers": 28},
    {"id": "firm_011", "name": "Chen & Associates", "domain": "chenassociates.hk", "region": "HK", "country": "Hong Kong", "practice_areas": ["Corporate Law", "Banking & Finance"], "num_lawyers": 76},
    {"id": "firm_012", "name": "Oakhurst Legal", "domain": "oakhurstlegal.com", "region": "WA", "country": "US", "practice_areas": ["Intellectual Property", "Corporate Law"], "num_lawyers": 112},
    {"id": "firm_013", "name": "Nguyen Park LLP", "domain": "nguyenpark.com", "region": "OR", "country": "US", "practice_areas": ["Immigration", "Employment Law"], "num_lawyers": 41},
    {"id": "firm_014", "name": "Barlow Dean & Associates", "domain": "barlowdean.com.au", "region": "QLD", "country": "Australia", "practice_areas": ["Litigation", "Real Estate"], "num_lawyers": 67},
    {"id": "firm_015", "name": "Fujimoto Legal Group", "domain": "fujimotolegal.jp", "region": "JP", "country": "Japan", "practice_areas": ["Mergers & Acquisitions", "Tax"], "num_lawyers": 130},
    {"id": "firm_016", "name": "Hargrove & Mitchell", "domain": "hargrovemitchell.com", "region": "NC", "country": "US", "practice_areas": ["Healthcare", "Employment Law"], "num_lawyers": 85},
    {"id": "firm_017", "name": "Sinclair Rowe Partners", "domain": "sinclairrowe.com", "region": "NY", "country": "US", "practice_areas": ["Bankruptcy", "Corporate Law"], "num_lawyers": 198},
    {"id": "firm_018", "name": "Tan & Loh Advocates", "domain": "tanloh.sg", "region": "SG", "country": "Singapore", "practice_areas": ["Litigation", "Corporate Law"], "num_lawyers": 47},
    {"id": "firm_019", "name": "Montague Blackwell", "domain": "montagueblackwell.com.au", "region": "NSW", "country": "Australia", "practice_areas": ["Tax", "Real Estate"], "num_lawyers": 156},
    {"id": "firm_020", "name": "Kim & Yoon Legal", "domain": "kimyoon.com", "region": "CA", "country": "US", "practice_areas": ["Intellectual Property", "Litigation"], "num_lawyers": 73},
    {"id": "firm_021", "name": "Calloway & Swift", "domain": "callowayswift.com", "region": "NV", "country": "US", "practice_areas": ["Real Estate", "Corporate Law"], "num_lawyers": 58},
    {"id": "firm_022", "name": "Drummond Falk LLP", "domain": "drummondfalk.com", "region": "MA", "country": "US", "practice_areas": ["Environmental Law", "Litigation"], "num_lawyers": 142},
    {"id": "firm_023", "name": "Watanabe & Ito", "domain": "watanabeito.jp", "region": "JP", "country": "Japan", "practice_areas": ["Corporate Law", "Banking & Finance"], "num_lawyers": 89},
    {"id": "firm_024", "name": "Pemberton Hayes", "domain": "pembertonhayes.com.au", "region": "VIC", "country": "Australia", "practice_areas": ["Employment Law", "Healthcare"], "num_lawyers": 71},
    {"id": "firm_025", "name": "Ortiz & Delgado", "domain": "ortizdelgado.com", "region": "AZ", "country": "US", "practice_areas": ["Immigration", "Family Law"], "num_lawyers": 34},
    {"id": "firm_026", "name": "Hartley Moore & Co", "domain": "hartleymoore.com", "region": "GA", "country": "US", "practice_areas": ["Insurance Defense", "Litigation"], "num_lawyers": 265},
    {"id": "firm_027", "name": "Leung & Chan", "domain": "leungchan.hk", "region": "HK", "country": "Hong Kong", "practice_areas": ["Real Estate", "Corporate Law"], "num_lawyers": 95},
    {"id": "firm_028", "name": "Stokes Whitaker LLP", "domain": "stokeswhitaker.com", "region": "FL", "country": "US", "practice_areas": ["Tax", "Mergers & Acquisitions"], "num_lawyers": 183},
    {"id": "firm_029", "name": "MacGregor & Finch", "domain": "macgregorfinch.com.au", "region": "WA", "country": "Australia", "practice_areas": ["Corporate Law", "Litigation"], "num_lawyers": 44},
    {"id": "firm_030", "name": "Yeung Associates", "domain": "yeungassociates.sg", "region": "SG", "country": "Singapore", "practice_areas": ["Banking & Finance", "Intellectual Property"], "num_lawyers": 38},
    {"id": "firm_031", "name": "Caldwell & Pratt", "domain": "caldwellpratt.com", "region": "NY", "country": "US", "practice_areas": ["Corporate Law", "Employment Law"], "num_lawyers": 410},
    {"id": "firm_032", "name": "Nakamura Legal", "domain": "nakamuralegal.jp", "region": "JP", "country": "Japan", "practice_areas": ["Intellectual Property", "Litigation"], "num_lawyers": 57},
    {"id": "firm_033", "name": "Bradshaw & Kemp", "domain": "bradshawkemp.com", "region": "NC", "country": "US", "practice_areas": ["Real Estate", "Environmental Law"], "num_lawyers": 81},
    {"id": "firm_034", "name": "Langford Pierce", "domain": "langfordpierce.com.au", "region": "NSW", "country": "Australia", "practice_areas": ["Litigation", "Bankruptcy"], "num_lawyers": 119},
    {"id": "firm_035", "name": "Park & Sun LLP", "domain": "parksun.com", "region": "WA", "country": "US", "practice_areas": ["Corporate Law", "Tax"], "num_lawyers": 66},
    {"id": "firm_036", "name": "Vasquez Law Partners", "domain": "vasquezlaw.com", "region": "CA", "country": "US", "practice_areas": ["Criminal Defense", "Immigration"], "num_lawyers": 22},
    {"id": "firm_037", "name": "Aldridge & Shaw", "domain": "aldridgeshaw.com", "region": "FL", "country": "US", "practice_areas": ["Healthcare", "Corporate Law"], "num_lawyers": 148},
    {"id": "firm_038", "name": "Koh & Teo Advocates", "domain": "kohteo.sg", "region": "SG", "country": "Singapore", "practice_areas": ["Mergers & Acquisitions", "Corporate Law"], "num_lawyers": 83},
    {"id": "firm_039", "name": "Holt Jennings Group", "domain": "holtjennings.com", "region": "MA", "country": "US", "practice_areas": ["Litigation", "Insurance Defense"], "num_lawyers": 237},
    {"id": "firm_040", "name": "Takahashi & Mori", "domain": "takahashimori.jp", "region": "JP", "country": "Japan", "practice_areas": ["Banking & Finance", "Corporate Law"], "num_lawyers": 104},
    {"id": "firm_041", "name": "Beckett Dunne LLP", "domain": "beckettdunne.com.au", "region": "QLD", "country": "Australia", "practice_areas": ["Employment Law", "Litigation"], "num_lawyers": 52},
    {"id": "firm_042", "name": "Graham & Lockhart", "domain": "grahamlockhart.com", "region": "AZ", "country": "US", "practice_areas": ["Real Estate", "Tax"], "num_lawyers": 97},
    {"id": "firm_043", "name": "Wong & Li Legal", "domain": "wongli.hk", "region": "HK", "country": "Hong Kong", "practice_areas": ["Corporate Law", "Litigation"], "num_lawyers": 163},
    {"id": "firm_044", "name": "Mercer & Vale", "domain": "mercervale.com", "region": "OR", "country": "US", "practice_areas": ["Environmental Law", "Real Estate"], "num_lawyers": 39},
    {"id": "firm_045", "name": "Sato & Yamamoto", "domain": "satoyamamoto.jp", "region": "JP", "country": "Japan", "practice_areas": ["Intellectual Property", "Corporate Law"], "num_lawyers": 71},
    {"id": "firm_046", "name": "Fielding Harper", "domain": "fieldingharper.com.au", "region": "VIC", "country": "Australia", "practice_areas": ["Banking & Finance", "Mergers & Acquisitions"], "num_lawyers": 128},
    {"id": "firm_047", "name": "Cooper & Wynn", "domain": "cooperwynn.com", "region": "NV", "country": "US", "practice_areas": ["Family Law", "Litigation"], "num_lawyers": 15},
    {"id": "firm_048", "name": "Blackstone Avery LLP", "domain": "blackstoneavery.com", "region": "NY", "country": "US", "practice_areas": ["Corporate Law", "Bankruptcy", "Mergers & Acquisitions"], "num_lawyers": 485},
    {"id": "firm_049", "name": "Ong & Ramirez", "domain": "ongramirez.sg", "region": "SG", "country": "Singapore", "practice_areas": ["Employment Law", "Corporate Law"], "num_lawyers": 29},
    {"id": "firm_050", "name": "Whitfield & Cross", "domain": "whitfieldcross.com", "region": "GA", "country": "US", "practice_areas": ["Litigation", "Healthcare"], "num_lawyers": 191},
    # Near-duplicates (same firms, different formatting/IDs — tests deduplication)
    {"id": "firm_051", "name": "Baker Sterling LLP", "domain": "bakersterling.com", "region": "CA", "country": "US", "practice_areas": ["Corporate Law", "Mergers & Acquisitions"], "num_lawyers": 210},
    {"id": "firm_052", "name": "Clarke Whitfield & Partners", "domain": "clarkewhitfield.com.au", "region": "NSW", "country": "Australia", "practice_areas": ["Real Estate", "Banking & Finance"], "num_lawyers": 88},
    {"id": "firm_053", "name": "Holt Jennings", "domain": "holtjennings.com", "region": "MA", "country": "US", "practice_areas": ["Litigation", "Insurance Defense"], "num_lawyers": 237},
    {"id": "firm_054", "name": "Wong Li Legal", "domain": "wongli.hk", "region": "HK", "country": "Hong Kong", "practice_areas": ["Corporate Law", "Litigation"], "num_lawyers": 163},
    {"id": "firm_055", "name": "Blackstone & Avery", "domain": "blackstoneavery.com", "region": "NY", "country": "US", "practice_areas": ["Corporate Law", "Bankruptcy"], "num_lawyers": 485},
]

FIRM_LOOKUP = {f["id"]: f for f in FIRMS}

# Pre-generated contact data per firm (seeded for consistency)
_rng = random.Random(42)

FIRST_NAMES = [
    "James", "Sarah", "Michael", "Jennifer", "Robert", "Linda", "David",
    "Elizabeth", "William", "Patricia", "Richard", "Barbara", "Thomas",
    "Susan", "Daniel", "Jessica", "Matthew", "Karen", "Andrew", "Nancy",
    "Kenji", "Yuki", "Wei", "Mei", "Raj", "Priya", "Liam", "Emma",
    "Oliver", "Charlotte", "Ethan", "Sophia", "Aiden", "Isabella",
    "Lucas", "Mia", "Hiroshi", "Sakura", "Jun", "Hana",
]

LAST_NAMES = [
    "Anderson", "Thompson", "Garcia", "Martinez", "Robinson", "Clark",
    "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young",
    "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green",
    "Tanaka", "Wong", "Singh", "Patel", "Chen", "Kim", "Nguyen",
    "Suzuki", "Yamamoto", "Watanabe", "Kobayashi", "Ito", "Takahashi",
    "Nakamura", "Mori", "Ishikawa", "Okada", "Hayashi", "Inoue", "Saito",
]

TITLES = [
    "Managing Partner", "Senior Partner", "Partner", "Of Counsel",
    "Chief Operating Officer", "Director of Business Development",
    "Head of Innovation", "General Counsel",
]

CONTACTS: Dict[str, Dict[str, Any]] = {}
for firm in FIRMS:
    first = _rng.choice(FIRST_NAMES)
    last = _rng.choice(LAST_NAMES)
    domain = firm["domain"]
    email = f"{first.lower()}.{last.lower()}@{domain}"
    linkedin = f"https://linkedin.com/in/{first.lower()}-{last.lower()}-{_rng.randint(1000, 9999)}"
    phone = f"+1-{_rng.randint(200,999)}-{_rng.randint(100,999)}-{_rng.randint(1000,9999)}"

    contact: Dict[str, Any] = {
        "firm_id": firm["id"],
        "name": f"{first} {last}",
        "title": _rng.choice(TITLES),
        "phone": phone,
        "email": email,
        "linkedin_url": linkedin,
    }

    # ~30% chance of null email or null linkedin
    if _rng.random() < 0.30:
        if _rng.random() < 0.5:
            contact["email"] = None
        else:
            contact["linkedin_url"] = None

    CONTACTS[firm["id"]] = contact


# ---------------------------------------------------------------------------
# Rate-limit tracking (enforced per-minute counter)
# ---------------------------------------------------------------------------
_request_times: list[float] = []
RATE_LIMIT = 20  # requests per minute


def _check_rate_limit() -> Dict[str, str]:
    """Enforce rate limit. Returns 429 when exceeded, otherwise returns headers."""
    now = time.time()
    # Prune older than 60s
    while _request_times and _request_times[0] < now - 60:
        _request_times.pop(0)
    remaining = max(0, RATE_LIMIT - len(_request_times))
    reset_at = int(now) + 60
    headers = {
        "X-RateLimit-Limit": str(RATE_LIMIT),
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Reset": str(reset_at),
    }
    if remaining == 0:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={**headers, "Retry-After": str(reset_at - int(now))},
        )
    _request_times.append(now)
    return headers


def _maybe_500():
    """~10% chance of a random 500 error."""
    if random.random() < 0.10:
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/firms")
async def get_firms(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
) -> JSONResponse:
    """
    Get paginated list of firms.

    Query params:
        page: page number (default 1)
        per_page: items per page (default 10, max 50)

    Returns ~10% random 500 errors. Includes rate-limit headers.
    """
    _maybe_500()
    headers = _check_rate_limit()

    start = (page - 1) * per_page
    end = start + per_page
    page_firms = FIRMS[start:end]

    # Return only basic info per firm
    items = [
        {"id": f["id"], "name": f["name"], "domain": f["domain"]}
        for f in page_firms
    ]

    body = {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": len(FIRMS),
        "total_pages": (len(FIRMS) + per_page - 1) // per_page,
    }
    return JSONResponse(content=body, headers=headers)


@app.get("/firms/{firm_id}/firmographic")
async def get_firmographic(firm_id: str) -> JSONResponse:
    """
    Get firmographic data for a specific firm.

    ~20% of responses have missing fields.
    Occasional schema inconsistency: "num_lawyers" sometimes returned as "lawyer_count".
    """
    _maybe_500()
    headers = _check_rate_limit()

    firm = FIRM_LOOKUP.get(firm_id)
    if not firm:
        raise HTTPException(status_code=404, detail="Firm not found")

    data: Dict[str, Any] = {
        "firm_id": firm["id"],
        "name": firm["name"],
        "domain": firm["domain"],
        "country": firm["country"],
        "region": firm["region"],
        "practice_areas": firm["practice_areas"],
    }

    # Schema inconsistency: ~25% of responses use "lawyer_count" instead of "num_lawyers"
    if random.random() < 0.25:
        data["lawyer_count"] = firm["num_lawyers"]
    else:
        data["num_lawyers"] = firm["num_lawyers"]

    # ~20% chance of dropping one or more optional fields
    if random.random() < 0.20:
        drop_candidates = ["region", "practice_areas", "domain"]
        to_drop = random.choice(drop_candidates)
        data.pop(to_drop, None)

    return JSONResponse(content=data, headers=headers)


@app.get("/firms/{firm_id}/contact")
async def get_contact(firm_id: str) -> JSONResponse:
    """
    Get contact information for a specific firm.

    ~30% of contacts have null email or null LinkedIn URL.
    """
    _maybe_500()
    headers = _check_rate_limit()

    contact = CONTACTS.get(firm_id)
    if not contact:
        raise HTTPException(status_code=404, detail="Firm not found")

    return JSONResponse(content=contact, headers=headers)


@app.post("/webhooks/crm")
async def webhook_crm(payload: Dict[str, Any]) -> JSONResponse:
    """
    Simulate CRM webhook endpoint.

    Accepts lead payloads. ~5% chance of 500 error.
    """
    if random.random() < 0.05:
        raise HTTPException(status_code=500, detail="CRM service unavailable")
    headers = _check_rate_limit()
    return JSONResponse(
        content={"status": "accepted", "id": f"crm_{random.randint(10000, 99999)}"},
        headers=headers,
    )


@app.post("/webhooks/email")
async def webhook_email(payload: Dict[str, Any]) -> JSONResponse:
    """
    Simulate email platform webhook endpoint.

    Accepts campaign payloads. ~5% chance of 500 error.
    """
    if random.random() < 0.05:
        raise HTTPException(status_code=500, detail="Email service unavailable")
    headers = _check_rate_limit()
    return JSONResponse(
        content={"status": "accepted", "id": f"email_{random.randint(10000, 99999)}"},
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
