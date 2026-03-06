"""
Main pipeline orchestrator for GTM data processing.

Processes firms through the complete GTM workflow:
1. Fetch all firms from the paginated API (handling errors and rate limits)
2. Deduplicate firms based on domain and name similarity
3. Enrich each firm with firmographic and contact data
4. Score firms against the ideal customer profile
5. Route leads into categories (high_priority / nurture / disqualified)
6. Assign experiment variants for qualified leads
7. Fire webhooks to CRM and email systems
"""
import time
import logging
from typing import Any, Dict, List, Optional
from difflib import SequenceMatcher

import yaml
import httpx

from enricher import Enricher
from scorer import ICPScorer
from router import LeadRouter
from experiment import ExperimentAssigner
from webhook import WebhookClient
from rate_limiter import RateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 1: Fetch all firms from the paginated /firms endpoint
# ---------------------------------------------------------------------------

def fetch_all_firms(base_url: str, max_retries: int = 3, rate_limiter: RateLimiter = None) -> List[Dict[str, Any]]:
    """
    Fetch every firm from the paginated /firms endpoint.

    Handles 429 rate limits, 500 server errors (exponential backoff),
    and connection failures. Paginates until all pages are retrieved.
    """
    all_firms: List[Dict[str, Any]] = []
    page = 1
    per_page = 10
    total_pages = None
    client = httpx.Client(timeout=30)

    try:
        while total_pages is None or page <= total_pages:
            response = _fetch_page_with_retry(client, base_url, page, per_page, max_retries, rate_limiter)

            if response is None:
                logger.error("Failed to fetch page %d after retries, skipping", page)
                page += 1
                continue

            data = response.json()
            all_firms.extend(data.get("items", []))
            total_pages = data.get("total_pages", 1)

            logger.info(
                "Fetched page %d/%d (%d firms so far)",
                page, total_pages, len(all_firms),
            )
            page += 1
    finally:
        client.close()

    return all_firms


def _fetch_page_with_retry(
    client: httpx.Client, base_url: str, page: int, per_page: int,
    max_retries: int, rate_limiter: RateLimiter = None,
) -> Optional[httpx.Response]:
    """Fetch a single page from /firms with retry logic."""
    url = f"{base_url}/firms"
    params = {"page": page, "per_page": per_page}

    for attempt in range(max_retries + 2):
        if rate_limiter:
            rate_limiter.wait_if_needed()

        try:
            resp = client.get(url, params=params)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 429:
                wait = min(int(resp.headers.get("retry-after", "5")), 5)
                logger.warning("Rate limited fetching page %d, waiting %ds", page, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 500:
                backoff = 2 ** attempt
                logger.warning("Server error on page %d, retrying in %ds", page, backoff)
                time.sleep(backoff)
                continue

        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            backoff = 2 ** attempt
            logger.warning("Connection error on page %d: %s, retrying in %ds", page, exc, backoff)
            time.sleep(backoff)

    return None


# ---------------------------------------------------------------------------
# Stage 2: Deduplication
# ---------------------------------------------------------------------------

def deduplicate_firms(firms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove duplicate firms based on domain (primary) and name similarity (secondary).

    The mock data includes near-duplicates with the same domain but slightly
    different names (e.g., "Baker & Sterling LLP" vs "Baker Sterling LLP").
    We keep the first occurrence and discard later duplicates.
    """
    seen_domains: Dict[str, Dict[str, Any]] = {}
    unique_firms: List[Dict[str, Any]] = []

    for firm in firms:
        domain = firm.get("domain", "").lower().strip()
        name = firm.get("name", "").strip()

        # Primary check: exact domain match
        if domain and domain in seen_domains:
            existing = seen_domains[domain]
            logger.info(
                "Duplicate detected: '%s' matches existing '%s' (domain: %s)",
                name, existing.get("name"), domain,
            )
            continue

        # Secondary check: high name similarity with any existing firm
        is_duplicate = False
        for existing in unique_firms:
            similarity = SequenceMatcher(
                None,
                _normalize_name(name),
                _normalize_name(existing.get("name", "")),
            ).ratio()
            if similarity > 0.85:
                logger.info(
                    "Near-duplicate detected: '%s' similar to '%s' (%.0f%% match)",
                    name, existing.get("name"), similarity * 100,
                )
                is_duplicate = True
                break

        if not is_duplicate:
            seen_domains[domain] = firm
            unique_firms.append(firm)

    logger.info(
        "Deduplication: %d firms -> %d unique firms (%d duplicates removed)",
        len(firms), len(unique_firms), len(firms) - len(unique_firms),
    )
    return unique_firms


def _normalize_name(name: str) -> str:
    """Normalize a firm name for comparison by removing common suffixes and punctuation."""
    name = name.lower().strip()
    for suffix in ["llp", "llc", "& partners", "& associates", "group", "& co"]:
        name = name.replace(suffix, "")
    name = "".join(c for c in name if c.isalnum() or c == " ")
    return " ".join(name.split())


# ---------------------------------------------------------------------------
# Stage 3-7: Enrich, Score, Route, Experiment, Webhook for each firm
# ---------------------------------------------------------------------------

def process_firm(
    firm: Dict[str, Any],
    enricher: Enricher,
    scorer: ICPScorer,
    router: LeadRouter,
    assigner: ExperimentAssigner,
    webhook_client: WebhookClient,
) -> Dict[str, Any]:
    """
    Process a single firm through the full pipeline.

    Returns a result dict summarizing what happened at each stage.
    """
    firm_id = firm["id"]
    firm_name = firm.get("name", "unknown")
    result: Dict[str, Any] = {"firm_id": firm_id, "name": firm_name}

    # --- Enrich ---
    firmographic = enricher.fetch_firmographic(firm_id)
    contact = enricher.fetch_contact(firm_id)

    if firmographic is None:
        logger.warning("No firmographic data for %s, using basic info only", firm_name)
        firmographic = {
            "num_lawyers": 0,
            "practice_areas": [],
            "country": "",
            "region": "",
        }

    # Merge enrichment data into a unified firm record
    enriched = {**firm, **firmographic}
    result["enriched"] = True
    result["contact"] = contact

    has_email = contact is not None and contact.get("email") is not None
    result["has_email"] = has_email

    # --- Score ---
    score = scorer.score(enriched)
    result["icp_score"] = score

    # --- Route ---
    route = router.route(enriched, score)
    result["route"] = route

    # --- Experiment & Webhook (only for qualified leads) ---
    if route in ("high_priority", "nurture"):
        variant = assigner.assign_variant(firm_id)
        result["experiment_variant"] = variant
        result["email_subject"] = assigner.get_variant_subject(variant)

        webhook_payload = {
            "firm_id": firm_id,
            "firm_name": firm_name,
            "domain": enriched.get("domain", ""),
            "country": enriched.get("country", ""),
            "num_lawyers": enriched.get("num_lawyers", 0),
            "icp_score": score,
            "route": route,
            "experiment_variant": variant,
            "email_subject": assigner.get_variant_subject(variant),
            "contact": contact,
        }

        crm_ok = webhook_client.fire(webhook_payload, target="crm")
        result["crm_webhook"] = "delivered" if crm_ok else "failed"

        if has_email:
            email_ok = webhook_client.fire(webhook_payload, target="email")
            result["email_webhook"] = "delivered" if email_ok else "failed"
        else:
            result["email_webhook"] = "skipped_no_email"
            logger.info("Skipping email webhook for %s (no email on contact)", firm_name)
    else:
        result["experiment_variant"] = None
        result["crm_webhook"] = "skipped_disqualified"
        result["email_webhook"] = "skipped_disqualified"

    return result


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(config_path: str) -> Any:
    """
    Run the complete GTM data pipeline.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Dict with pipeline summary and per-firm results
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    api_cfg = config.get("apis", {})
    enrichment_cfg = api_cfg.get("enrichment", {})
    base_url = enrichment_cfg.get("base_url", "http://localhost:8000")
    max_retries = enrichment_cfg.get("max_retries", 3)
    timeout = enrichment_cfg.get("timeout", 30)

    logger.info("=" * 60)
    logger.info("Starting GTM Data Pipeline")
    logger.info("=" * 60)

    # Create a shared rate limiter for all API calls (server limit is 20/min)
    # We use 18 to leave headroom and avoid edge-case 429s
    rate_limiter = RateLimiter(max_requests=18, window_seconds=60)

    # Stage 1: Fetch all firms
    logger.info("Stage 1: Fetching firms from API...")
    raw_firms = fetch_all_firms(base_url, max_retries=max_retries, rate_limiter=rate_limiter)
    logger.info("Fetched %d raw firm records", len(raw_firms))

    # Stage 2: Deduplicate
    logger.info("Stage 2: Deduplicating firms...")
    unique_firms = deduplicate_firms(raw_firms)

    # Initialize pipeline components with shared rate limiter
    enricher = Enricher(base_url, timeout=timeout, max_retries=max_retries, rate_limiter=rate_limiter)
    scorer = ICPScorer(config.get("icp_criteria", {}))
    router = LeadRouter(config)
    assigner = ExperimentAssigner(config)
    webhook_client = WebhookClient(api_cfg, rate_limiter=rate_limiter)

    # Stage 3-7: Process each firm
    logger.info("Stages 3-7: Enriching, scoring, routing, and delivering...")
    results = []
    for i, firm in enumerate(unique_firms, 1):
        logger.info("Processing firm %d/%d: %s", i, len(unique_firms), firm.get("name"))
        result = process_firm(firm, enricher, scorer, router, assigner, webhook_client)
        results.append(result)

    # Cleanup
    enricher.close()
    webhook_client.close()

    # Build summary
    route_counts = {"high_priority": 0, "nurture": 0, "disqualified": 0}
    variant_counts: Dict[str, int] = {}
    for r in results:
        route_counts[r["route"]] = route_counts.get(r["route"], 0) + 1
        v = r.get("experiment_variant")
        if v:
            variant_counts[v] = variant_counts.get(v, 0) + 1

    summary = {
        "total_raw_firms": len(raw_firms),
        "duplicates_removed": len(raw_firms) - len(unique_firms),
        "unique_firms_processed": len(unique_firms),
        "route_distribution": route_counts,
        "experiment_distribution": variant_counts,
        "results": results,
    }

    logger.info("=" * 60)
    logger.info("Pipeline Complete")
    logger.info("  Raw firms fetched:    %d", summary["total_raw_firms"])
    logger.info("  Duplicates removed:   %d", summary["duplicates_removed"])
    logger.info("  Unique firms:         %d", summary["unique_firms_processed"])
    logger.info("  High priority leads:  %d", route_counts["high_priority"])
    logger.info("  Nurture leads:        %d", route_counts["nurture"])
    logger.info("  Disqualified:         %d", route_counts["disqualified"])
    logger.info("  Experiment split:     %s", variant_counts)
    logger.info("=" * 60)

    return summary


if __name__ == "__main__":
    import json

    results = run_pipeline("config.yaml")

    with open("pipeline_output.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nResults written to pipeline_output.json")