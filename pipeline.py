#!/usr/bin/env python3
"""
pipeline.py  —  Raw contact CSV  ->  final.csv  (A to Z, single script)
------------------------------------------------------------------------
Reads a raw CSV (Apollo / BRYCOOL / sac2 format), then uses Tavily to
find the CEO for every unique email domain. Assigns the found CEO to
ALL contacts at that domain.

Key design:
  - One Tavily search per unique domain  (not per row)
  - CEO name assigned to every row sharing that domain
  - ALL input rows appear in output: final.csv + left.csv = total input rows
  - No peer-lookup (avoids assigning CFOs/Finance Directors as CEO)
  - Progress saved every 25 domains — safe to interrupt and resume
  - Re-running automatically continues from the last processed domain

Outputs  (output/result/):
  final.csv   — rows with a Tavily-verified valid CEO name
  left.csv    — rows where no CEO name was found
  lefts.csv   — mirror of left.csv

Usage:
  python -u pipeline.py --input "MyFile.csv"
  python -u pipeline.py --input "MyFile.csv" --no-tavily
  python -u pipeline.py --input "MyFile.csv" --limit 500
  python -u pipeline.py --input "MyFile.csv" --reset
  python -u pipeline.py --clean
  python -u pipeline.py --strict-clean   # re-validate cache + outputs (no API)
  python -u pipeline.py --input X.csv --lenient   # disable strict CEO rules

Strict mode (default): tighter name checks, phrase filters, and removal of CEO
strings reused across too many unrelated domains (search-snippet pollution).
"""

import argparse
import csv
import json
import os
import re
import socket
import sys
import time
from pathlib import Path
from datetime import date

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
RESULT_DIR = SCRIPT_DIR / "output" / "result"
FINAL_CSV = RESULT_DIR / "final.csv"
LEFT_CSV = RESULT_DIR / "left.csv"
LEFTS_CSV = RESULT_DIR / "lefts.csv"
CACHE_FILE = RESULT_DIR / "tavily_domain_cache.json"
PROGRESS_FILE = RESULT_DIR / "rescan_progress.json"
SESSION_FILE = RESULT_DIR / "pipeline_session.json"

OUT_COLS = ["First name last name", "Email address of HR", "First name of HR"]

socket.setdefaulttimeout(25)

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv

    load_dotenv(SCRIPT_DIR / ".env")
except ImportError:
    pass

try:
    from tavily import TavilyClient

    HAS_TAVILY = True
except ImportError:
    HAS_TAVILY = False

# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------
_NAME_SUFFIXES = {
    "jr",
    "jr.",
    "sr",
    "sr.",
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "phd",
    "ph.d",
    "md",
    "m.d",
    "dds",
    "esq",
    "cpa",
    "pe",
    "rn",
}
_NAME_PARTICLES = {
    "de",
    "van",
    "von",
    "del",
    "della",
    "di",
    "da",
    "du",
    "le",
    "la",
    "los",
    "las",
    "el",
    "al",
    "bin",
    "binti",
    "ibn",
    "o",
    "y",
}
# Covers ASCII + Latin Extended (accented European names: Adrián, Jörg, Teixiné, etc.)
_LATIN_UPPER = r"A-Z\u00C0-\u024F"
_LATIN_ANY = r"A-Za-z\u00C0-\u024F"
_TOKEN_RE = re.compile(rf"^[{_LATIN_UPPER}][{_LATIN_ANY}'\-\.]*$")

_BAD_WORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "for",
    "nor",
    "so",
    "yet",
    "in",
    "on",
    "at",
    "to",
    "of",
    "by",
    "as",
    "up",
    "if",
    "it",
    "its",
    "via",
    "per",
    "re",
    "vs",
    "etc",
    "eg",
    "ie",
    "about",
    "above",
    "across",
    "after",
    "against",
    "along",
    "also",
    "although",
    "amid",
    "among",
    "around",
    "before",
    "behind",
    "below",
    "beneath",
    "beside",
    "between",
    "beyond",
    "down",
    "during",
    "except",
    "from",
    "how",
    "including",
    "into",
    "let",
    "like",
    "near",
    "off",
    "onto",
    "out",
    "over",
    "past",
    "since",
    "than",
    "that",
    "through",
    "under",
    "until",
    "with",
    "within",
    "without",
    "who",
    "whom",
    "whose",
    "which",
    "what",
    "when",
    "where",
    "why",
    "he",
    "she",
    "they",
    "we",
    "us",
    "our",
    "his",
    "her",
    "their",
    "my",
    "your",
    # job titles / roles
    "ceo",
    "coo",
    "cfo",
    "cto",
    "cmo",
    "chro",
    "cpo",
    "vp",
    "svp",
    "evp",
    "avp",
    "president",
    "vice",
    "executive",
    "chief",
    "officer",
    "manager",
    "supervisor",
    "administrator",
    "coordinator",
    "specialist",
    "consultant",
    "analyst",
    "advisor",
    "associate",
    "assistant",
    "representative",
    "chairman",
    "chairwoman",
    "chairperson",
    "chair",
    "treasurer",
    "controller",
    "secretary",
    "principal",
    "partner",
    "interim",
    "acting",
    "emeritus",
    "non",
    "non-executive",
    # org words
    "company",
    "corp",
    "corporation",
    "inc",
    "llc",
    "ltd",
    "plc",
    "lp",
    "llp",
    "pllc",
    "group",
    "partners",
    "foundation",
    "association",
    "organization",
    "institute",
    "university",
    "college",
    "school",
    "hospital",
    "hospitals",
    "clinic",
    "clinics",
    "children",
    "childrens",
    "pediatric",
    "pediatrics",
    "memorial",
    "general",
    "services",
    "solutions",
    "systems",
    "industries",
    "holdings",
    "ventures",
    "department",
    "agency",
    "authority",
    "office",
    "press",
    "media",
    "news",
    "network",
    "centre",
    "studio",
    "labs",
    "technologies",
    "tech",
    "health",
    "care",
    "dental",
    "staffing",
    "consulting",
    "management",
    "medical",
    "financial",
    "capital",
    "global",
    "national",
    "regional",
    "international",
    "manufacturing",
    "construction",
    "distribution",
    "logistics",
    "realty",
    "investments",
    "investment",
    "properties",
    "property",
    "insurance",
    "automotive",
    "aerospace",
    "industrial",
    "engineering",
    "infrastructure",
    "telecom",
    "broadband",
    "wireless",
    "digital",
    "software",
    "hardware",
    "cybersecurity",
    "fintech",
    "edtech",
    "healthtech",
    "agritech",
    "biotech",
    "biotechnology",
    "pharmaceutical",
    "pharmaceuticals",
    "diagnostics",
    "healthcare",
    "homecare",
    "wellness",
    "pharmacy",
    "surgical",
    "rehabilitation",
    "therapy",
    "nutrition",
    "nutritional",
    "agribusiness",
    # NOTE: church, bank, center, page, north, south, east, west, london,
    # washington, dallas, austin, paris, kent — kept out (valid surnames)
    # web / document / UI artifacts
    "blog",
    "breadcrumb",
    "crunchbase",
    "analytics",
    "photo",
    "image",
    "picture",
    "video",
    "file",
    "logo",
    "headshot",
    "bio",
    "about",
    "contact",
    "home",
    "page",
    "spotlight",
    "board",
    "report",
    "annual",
    "introduction",
    "sustainability",
    "policy",
    "plan",
    "plans",
    "circle",
    "profile",
    "overview",
    "summary",
    "review",
    "download",
    "search",
    "sign",
    "login",
    "register",
    "submit",
    "click",
    "view",
    "enter",
    "read",
    "send",
    "edit",
    "update",
    "confirm",
    "subscribe",
    "reload",
    # publication fragments
    "journal",
    "magazine",
    "publishing",
    "newsletter",
    "times",
    "gazette",
    "herald",
    "tribune",
    "wire",
    "bulletin",
    "digest",
    # place names that are clearly never surnames
    "baltimore",
    "germany",
    "chicago",
    "denver",
    "atlanta",
    "seattle",
    "portland",
    "africa",
    "americas",
    "apac",
    "emea",
    "latam",
    "mena",
    "area",
    "region",
    "district",
    "zone",
    "dc",
    # narrative / context words
    "says",
    "said",
    "according",
    "announces",
    "announced",
    "announcing",
    "joins",
    "joined",
    "leaves",
    "left",
    "steps",
    "stepped",
    "elected",
    "promoted",
    "named",
    "appointed",
    "called",
    "known",
    "retired",
    "fellow",
    "succession",
    "leadership",
    "leaders",
    "team",
    "staff",
    "employees",
    "basic",
    "fit",
    "job",
    "posting",
    "listing",
    # single-letter noise
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "j",
    "k",
    "l",
    "m",
    "n",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "z",
}

_BAD_PHRASES = re.compile(
    r"(?i)\b("
    r"succession\s+(plan|policy|planning)|"
    r"board\s+spotlight|"
    r"non.?executive\s+director|"
    r"chief\s+executive\s*officer|"
    r"executive\s+job|"
    r"leadership\s+(team|circle|page)|"
    r"our\s+leadership|"
    r"and\s+(ceo|leadership|the)|"
    r"houston\s+business\s+journal|"
    r"washington\s+(dc|d\.c\.)|"
    r"dc.?baltimore\s+area|"
    r"introduction\s+sustainability|"
    r"sustainability\s+report|"
    r"annual\s+report|"
    r"publishing\s+germany|"
    r"when\s+\w+\s+\w+|"
    r"palo\s+alto|real\s+estate|credit\s+union|"
    r"fitness\s+center|big\s+loss|step(s|ped)?\s+down|"
    r"learn\s+more|dear\s+friend"
    r")\b",
    re.I,
)

# Strict: tokens / labels that are almost never a personal name in CEO extractions.
_BAD_WORDS_STRICT = {
    "while",
    "gaining",
    "continue",
    "reading",
    "dear",
    "customers",
    "customer",
    "friends",
    "friend",
    "subscribe",
    "loading",
    "sidebar",
    "navigation",
    "cookie",
    "cookies",
    "mentor",
    "corporate",
    "citizenship",
    "strategy",
    "podium",
    "affiliated",
    "londres",
    "nominated",
    "winning",
    "podcast",
    "episode",
    "chapter",
    "article",
    "author",
    "editor",
    "contributor",
    "guest",
    "speaker",
    "host",
    "moderator",
    "interim",
    "acting",
    "former",
    "incoming",
    "outgoing",
    "named",
    "joining",
    "resources",
    "division",
    "department",
    "committee",
    "council",
    "trustees",
    "trustee",
    "volunteers",
    "supporters",
    "stakeholders",
    "shareholders",
    "investors",
    "community",
    "partnership",
    "programs",
    "initiative",
    "solutions",
    "platform",
    "website",
    "portal",
    "newsroom",
    "disclaimer",
    "copyright",
    "trademark",
    "privacy",
    "terms",
    "conditions",
    "policies",
    "compliance",
    "sustainability",
    "esg",
    "csr",
    "overview",
    "mission",
    "vision",
    "values",
    "history",
    "timeline",
    "directory",
    "locations",
    "careers",
    "jobs",
    "apply",
    "applynow",
    "continuing",
    "education",
    "governance",
    "relations",
    "representative",
    "representatives",
    "assistant",
    "assistants",
    "administrative",
    "virtual",
    "success",
    "development",
    "management",
    "operations",
    "communications",
    "marketing",
    "financial",
    "clinical",
    "medical",
    "patient",
    "patients",
    "service",
    "services",
    "support",
}

_BAD_PHRASES_STRICT = re.compile(
    r"(?i)\b("
    r"while\s+gaining|continue\s+reading|dear\s+customers|dear\s+client|"
    r"dear\s+friend|human\s+resources|corporate\s+citizenship|"
    r"mentor\.?\s*founder|podium\s+strategy|full\s+focus|board\s+spo|"
    r"\band\s+ceo\b|our\s+team|meet\s+the\s+team|executive\s+team|"
    r"leadership\s+team|management\s+team|customer\s+service|"
    r"patient\s+care|health\s+care|care\s+team|"
    r"clay\.\s*clay|faheem\.\s*faheem|oates\.\s*faheem|"
    r"londres\.\s*|affiliated\.\s*|continue\s+to\s+read|"
    r"learn\s+more|read\s+more|click\s+here|sign\s+up|"
    r"corporate\s+governance|social\s+responsibility|"
    r"public\s+relations|investor\s+relations|media\s+relations|"
    r"customer\s+success|business\s+development|help\s+desk|"
    r"call\s+center|contact\s+center|data\s+entry"
    r")\b",
    re.I,
)

# Full-name strings that often appear in generic leadership snippets (not domain CEO).
_STRICT_EXACT_DENYLIST = frozenset(
    {
        "michael hyatt",
    }
)

# If the same CEO full name appears for more than this many domains, treat as
# search-snippet pollution and clear it everywhere (strict cleanup).
STRICT_MAX_SAME_CEO_DOMAINS = 5


def _norm_meaningful_token(w: str) -> str:
    return w.strip("\"'").lower().rstrip(".")


def is_valid_name(name: str, strict: bool = False) -> tuple:
    n = (name or "").strip().strip("\"'.,;:")
    if not n:
        return False, "empty"
    if any(c.isdigit() for c in n):
        return False, "contains digit"
    if "\n" in n or "\r" in n:
        return False, "contains newline"
    if _BAD_PHRASES.search(n):
        return False, "bad phrase"
    if strict and _BAD_PHRASES_STRICT.search(n):
        return False, "strict phrase"
    parts = n.split()
    if len(parts) < 2:
        return False, "single word"
    meaningful = [p for p in parts if p.lower().rstrip(".") not in _NAME_SUFFIXES]
    if len(meaningful) < 2:
        return False, "only one meaningful word"
    if len(meaningful) > 4:
        return False, f"too many words ({len(meaningful)})"
    norm_keys = [_norm_meaningful_token(p) for p in meaningful]
    if len(norm_keys) != len(set(norm_keys)):
        return False, "repeated word"
    n_lower = n.lower()
    if strict and n_lower in _STRICT_EXACT_DENYLIST:
        return False, "strict denylist"
    real_tokens = 0  # count of non-initial meaningful tokens
    for word in meaningful:
        wl = word.lower().rstrip(".")
        # Single-letter middle initial (e.g. "S." or bare "S" between names) — skip validation
        # but don't count toward real_tokens so "D P Saraswat" still fails (only 1 real token)
        if len(wl) == 1 and word[0].isupper():
            continue
        if wl in _BAD_WORDS:
            return False, f"bad word: '{word}'"
        if strict:
            if _norm_meaningful_token(word) in _BAD_WORDS_STRICT:
                return False, f"strict bad word: '{word}'"
            if len(word) > 2 and word.endswith("."):
                return False, "trailing period on token"
        if wl in _NAME_PARTICLES and word[0].islower():
            if word == meaningful[0]:
                return False, "starts with particle"
            real_tokens += 1
            continue
        if not _TOKEN_RE.match(word):
            return False, f"invalid token: '{word}'"
        real_tokens += 1
    if real_tokens < 2:
        return False, "fewer than 2 real name tokens"
    return True, ""


def scrub_cache_strict(cache: dict) -> dict:
    """
    Clear cache values that fail strict validation or appear on too many domains.
    Mutates cache in place. Returns stats.
    """
    from collections import defaultdict

    stats = {
        "invalid_strict": 0,
        "frequency_wipe": 0,
        "over_repeated_names": 0,
    }
    for d in list(cache.keys()):
        v = cache.get(d)
        if not v or not str(v).strip():
            continue
        ok, _ = is_valid_name(str(v).strip(), strict=True)
        if not ok:
            cache[d] = ""
            stats["invalid_strict"] += 1

    by_name: dict = defaultdict(list)
    for d, v in cache.items():
        s = (v or "").strip()
        if s:
            by_name[s.lower()].append(d)
    for _nl, doms in by_name.items():
        if len(doms) > STRICT_MAX_SAME_CEO_DOMAINS:
            stats["over_repeated_names"] += 1
            stats["frequency_wipe"] += len(doms)
            for d in doms:
                cache[d] = ""
    return stats


# ---------------------------------------------------------------------------
# Tavily name extraction
# ---------------------------------------------------------------------------
_EXTRACT_BAD = re.compile(
    r"(?i)\b(inc\.?|llc|ltd|corp|corporation|university|college|hospital|"
    r"healthcare|wellness|pharmacy|department|linkedin|website|bio|logo|"
    r"headshot|photo|officer|chairman|chairwoman|director|manager|"
    r"specialist|dr\.?|mr\.?|mrs\.?|ms\.?|non\.?)\b"
)
_TV_TOK = r"(?:Mc|Mac)?[A-Z][a-z]{1,}[A-Za-z.'-]*"
_TV_NAME = re.compile(rf"\b({_TV_TOK}(?:\s+{_TV_TOK}){{1,3}})\b")
_TV_PATS = [
    (
        re.compile(
            rf"{_TV_NAME.pattern}\s+(?:is|was|has been|serves as|appointed as)\s+(?:the\s+)?"
            r"(CEO|Chief Executive Officer|Chief Executive|President|Founder)\b",
            re.I,
        ),
        "after_name",
    ),
    (re.compile(r"(CEO|Chief Executive Officer|Chief Executive|President|Founder)\s+" + rf"{_TV_NAME.pattern}", re.I), "title_first"),
    (re.compile(rf"{_TV_NAME.pattern},\s*(CEO|Chief Executive Officer|Chief Executive|President|Founder)\b", re.I), "comma_title"),
    (
        re.compile(r"(?:^|[\n\.])\s*(CEO|President|Founder)\s*[:|\-]\s*" + rf"{_TV_NAME.pattern}", re.I | re.M),
        "label_colon",
    ),
]
_TITLE_RANK = {"CEO": 0, "President": 1, "Founder": 2}
_QUOTA_MARKS = (
    "usage limit",
    "exceeds your plan",
    "rate limit",
    "too many requests",
    "quota",
    "429",
)
_FORMER_ROLE_RE = re.compile(r"(?i)\b(former|previous|ex[-\s]|retired|stepped down|resigned|past)\b")


def _context_window(blob: str, start: int, end: int, radius: int = 120) -> str:
    lo = max(0, start - radius)
    hi = min(len(blob), end + radius)
    return blob[lo:hi]


def _is_current_role_context(ctx: str) -> bool:
    # Reject obviously stale role references.
    if _FORMER_ROLE_RE.search(ctx):
        return False
    return True


def _tv_plausible(name: str, strict: bool = True) -> bool:
    if "\n" in name or "\r" in name:
        return False
    if len(name) < 5 or len(name) > 50:
        return False
    if _EXTRACT_BAD.search(name):
        return False
    parts = name.split()
    if len(parts) < 2 or len(parts) > 3:
        return False
    tok = re.compile(r"^[A-Z][a-z]{1,}[A-Za-z.'-]*$")
    if not (tok.match(parts[0]) and tok.match(parts[-1])):
        return False
    return is_valid_name(name, strict=strict)[0]


def _tv_extract(blob: str, strict: bool = True) -> list:
    out = []
    if not blob or len(blob) < 20:
        return out
    for pat, method in _TV_PATS:
        for m in pat.finditer(blob):
            g = m.groups()
            if method == "title_first":
                name, raw = g[1], g[0]
            elif method == "after_name":
                name, raw = g[0], g[1]
            elif method == "comma_title":
                name, raw = g[0], g[1]
            elif method == "label_colon":
                name, raw = g[1], g[0]
            else:
                continue
            if not _is_current_role_context(_context_window(blob, m.start(), m.end())):
                continue
            if not _tv_plausible(name.strip(), strict=strict):
                continue
            u = raw.strip().upper()
            if "FOUNDER" in u:
                title = "Founder"
            elif "CHIEF EXECUTIVE" in u or u == "CEO":
                title = "CEO"
            elif "PRESIDENT" in u:
                title = "President"
            else:
                title = raw.strip()[:40]
            out.append((name.strip(), title, _TITLE_RANK.get(title, 6)))
    by_key: dict = {}
    for name, title, rank in out:
        k = (name.lower(), title)
        if k not in by_key or rank < by_key[k][2]:
            by_key[k] = (name, title, rank)
    return sorted(by_key.values(), key=lambda x: x[2])


def tv_lookup(client, domain: str, delay: float = 1.25, strict: bool = True) -> str:
    today_iso = date.today().isoformat()
    queries = [
        (
            f"Who is the current CEO, President, or Founder of the company at {domain} as of {today_iso}? "
            "Return full name and title only.",
            None,
        ),
        (f"site:{domain} (current CEO OR current president OR current founder OR \"chief executive\")", [domain]),
        (f"\"current\" (CEO OR president OR founder) \"{domain}\" site:linkedin.com", ["linkedin.com", domain]),
    ]
    all_cands = []
    for q, inc in queries:
        try:
            kw = {"query": q, "search_depth": "advanced", "include_answer": True, "max_results": 10}
            if inc:
                kw["include_domains"] = inc
            resp = client.search(**kw)
        except Exception as e:
            err = str(e).lower()
            if any(m in err for m in _QUOTA_MARKS):
                raise RuntimeError(f"Quota: {e}")
            time.sleep(0.15)
            continue
        if not isinstance(resp, dict):
            time.sleep(0.15)
            continue
        # Only use content from results that are actually about this domain
        answer = resp.get("answer") or ""
        results = resp.get("results") or []
        on_domain = [
            (r.get("content") or "") + " " + (r.get("title") or "")
            for r in results
            if isinstance(r, dict) and domain in (r.get("url") or "").lower()
        ]
        # Always include the direct answer blob; add domain-matched results
        blob = answer + "\n" + "\n".join(on_domain)
        # If nothing domain-matched, fall back to all results but only use
        # the Tavily answer field (which is a direct synthesis, not noise)
        if not on_domain:
            blob = answer
        all_cands.extend(_tv_extract(blob, strict=strict))
        time.sleep(0.15)
    time.sleep(delay)
    return all_cands[0][0] if all_cands else ""


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------


def valid_email(email: str) -> bool:
    return bool(
        email
        and re.match(
            r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?"
            r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}$",
            email.strip(),
        )
    )


def email_domain(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.strip().lower().split("@", 1)[1]


def is_gov_mil(email: str) -> bool:
    if not email or "@" not in email:
        return False
    d = email.strip().lower().split("@", 1)[1]
    return (
        d.endswith(".gov")
        or ".gov." in d
        or d.endswith(".mil")
        or ".mil." in d
        or ".govt.nz" in d
        or d == "goarmy.com"
    )


# ---------------------------------------------------------------------------
# Input format detection
# ---------------------------------------------------------------------------


def detect_format(fieldnames: list) -> str:
    low = {h.strip().lower() for h in (fieldnames or [])}
    if "ceo_full_name" in low and "hr_email_address" in low:
        return "sac2"
    if "name" in low and "email" in low and "employer name" in low:
        return "brycool"
    if "name" in low and "email" in low:
        return "brycool"
    if "first name" in low and "email" in low and ("company name" in low or "full name" in low):
        return "apollo"
    return "unknown"


def _get(row, *keys):
    for k in keys:
        for attempt in (k, k.title(), k.lower(), k.upper()):
            v = row.get(attempt, "")
            if v and str(v).strip():
                return str(v).strip()
    return ""


def get_email(row, fmt):
    if fmt == "sac2":
        return _get(row, "hr_email_address")
    return _get(row, "Email", "email")


def get_hr_first(row, fmt):
    if fmt == "sac2":
        return _get(row, "hr_first_name")
    fn = _get(row, "First Name", "first name")
    if fn:
        return fn
    full = _get(row, "Full Name", "Name", "name")
    return full.split(None, 1)[0] if full else ""


# ---------------------------------------------------------------------------
# CSV output helpers  (", " separator)
# ---------------------------------------------------------------------------


def _field(s: str) -> str:
    s = s or ""
    if "," in s or '"' in s or "\n" in s or "\r" in s:
        return '"' + s.replace('"', '""') + '"'
    return s


def write_header(fh) -> None:
    fh.write(", ".join(_field(c) for c in OUT_COLS) + "\n")
    fh.flush()


def write_row(fh, ceo: str, email: str, hr_first: str) -> None:
    fh.write(", ".join(_field(v) for v in [ceo, email, hr_first]) + "\n")
    fh.flush()


def write_outputs_from_cache(contacts: list, cache: dict, strict: bool = True) -> tuple:
    """
    Rebuild final/left outputs from the current in-memory cache state.
    Returns (final_count, left_count).
    """
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with (
        open(FINAL_CSV, "w", encoding="utf-8", newline="") as final_fh,
        open(LEFT_CSV, "w", encoding="utf-8", newline="") as left_fh,
        open(LEFTS_CSV, "w", encoding="utf-8", newline="") as lefts_fh,
    ):
        write_header(final_fh)
        write_header(left_fh)
        write_header(lefts_fh)

        final_count = left_count = 0
        for email, hr_first in contacts:
            domain = email_domain(email)
            ceo = cache.get(domain, "")
            ok, _ = is_valid_name(ceo, strict=strict) if ceo else (False, "")
            if not ok:
                ceo = ""

            if ceo:
                write_row(final_fh, ceo, email, hr_first)
                final_count += 1
            else:
                write_row(left_fh, "", email, hr_first)
                write_row(lefts_fh, "", email, hr_first)
                left_count += 1

    return final_count, left_count


def read_output_csv(path: Path) -> list:
    if not path.is_file():
        return []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fn = [k.strip() for k in (reader.fieldnames or [])]
        rows = []
        for row in reader:
            s = {k.strip(): (v or "").strip() for k, v in row.items()}
            out = {}
            for i, col in enumerate(OUT_COLS):
                src = fn[i] if i < len(fn) else col
                out[col] = s.get(col) or s.get(src) or ""
            rows.append(out)
    return rows


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def load_cache(reset: bool = False) -> tuple:
    cache, done = {}, set()
    if not reset:
        if CACHE_FILE.is_file():
            with open(CACHE_FILE, encoding="utf-8") as f:
                cache = json.load(f)
        if PROGRESS_FILE.is_file():
            with open(PROGRESS_FILE, encoding="utf-8") as f:
                done = set(json.load(f))
    return cache, done


def save_cache(cache: dict, done: set) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(done), f)


# ---------------------------------------------------------------------------
# --clean mode
# ---------------------------------------------------------------------------


def load_contacts_from_outputs() -> list:
    """Unique (email, hr_first) from final.csv + left.csv for cache rebuild."""
    rows = read_output_csv(FINAL_CSV) + read_output_csv(LEFT_CSV)
    seen: set = set()
    contacts: list = []
    for r in rows:
        email = (r.get(OUT_COLS[1]) or "").strip()
        el = email.lower()
        if el and el not in seen:
            seen.add(el)
            contacts.append((email, (r.get(OUT_COLS[2]) or "").strip()))
    return contacts


def strict_clean_mode():
    """Re-validate tavily cache with strict rules, save, rebuild CSVs (no API)."""
    print("=== Strict cache clean (no API) ===")
    cache, done = load_cache(reset=False)
    if not cache and not CACHE_FILE.is_file():
        sys.exit("No cache found at " + str(CACHE_FILE))
    stats = scrub_cache_strict(cache)
    save_cache(cache, done)
    contacts = load_contacts_from_outputs()
    if not contacts:
        sys.exit("No rows in final.csv + left.csv — run pipeline with --input first.")
    fc, lc = write_outputs_from_cache(contacts, cache, strict=True)
    print(f"  Cache entries cleared (invalid strict): {stats['invalid_strict']:,}")
    print(
        f"  Domains cleared (name on >{STRICT_MAX_SAME_CEO_DOMAINS} domains): "
        f"{stats['frequency_wipe']:,}  ({stats['over_repeated_names']:,} names)"
    )
    print(f"  final.csv : {fc:,} rows")
    print(f"  left.csv  : {lc:,} rows")
    print("Done.")


def clean_mode(strict: bool = True):
    print("Re-validating final.csv ...")
    rows = read_output_csv(FINAL_CSV)
    left = read_output_csv(LEFT_CSV)
    valid, moved = [], []
    for r in rows:
        ok, reason = is_valid_name(r.get(OUT_COLS[0], ""), strict=strict)
        if ok:
            valid.append(r)
        else:
            print(f"  removed: {repr(r.get(OUT_COLS[0],'')):40} — {reason}")
            r[OUT_COLS[0]] = ""
            moved.append(r)
    combined_left = left + moved
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    for path, data in [(FINAL_CSV, valid), (LEFT_CSV, combined_left), (LEFTS_CSV, combined_left)]:
        with open(path, "w", encoding="utf-8", newline="") as f:
            write_header(f)
            for r in data:
                write_row(f, r[OUT_COLS[0]], r[OUT_COLS[1]], r[OUT_COLS[2]])
    print(f"  Kept  : {len(valid):,}")
    print(f"  Moved : {len(moved):,}")
    print("Done.")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Raw contact CSV -> clean final.csv via Tavily CEO lookup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--input", type=Path, default=None, metavar="FILE")
    ap.add_argument("--no-tavily", action="store_true")
    ap.add_argument("--limit", type=int, default=0, metavar="N")
    ap.add_argument("--delay", type=float, default=1.25, metavar="SEC")
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Clear Tavily cache and reprocess all domains",
    )
    ap.add_argument(
        "--clean",
        action="store_true",
        help="Re-validate output files only, no API calls",
    )
    ap.add_argument(
        "--strict-clean",
        action="store_true",
        help="Strict-validate cache + rebuild final/left (no API)",
    )
    ap.add_argument(
        "--lenient",
        action="store_true",
        help="Looser CEO name rules (default is strict)",
    )
    args = ap.parse_args()

    strict_mode = not args.lenient

    if args.strict_clean:
        strict_clean_mode()
        return

    if args.clean:
        clean_mode(strict=strict_mode)
        return

    if not args.input:
        ap.error("--input FILE is required")

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    # ------------------------------------------------------------------ #
    # STEP 1 — Load raw CSV                                               #
    # ------------------------------------------------------------------ #
    print("\n=== Step 1: Load ===")
    src = args.input.resolve()
    if not src.is_file():
        sys.exit(f"Not found: {src}")

    with open(src, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fmt = detect_format(reader.fieldnames or [])
        if fmt == "unknown":
            sys.exit("Unrecognised CSV format. Need Apollo, BRYCOOL, or sac2 columns.")
        raw_rows = [dict(r) for r in reader]

    print(f"  Format   : {fmt}")
    print(f"  Raw rows : {len(raw_rows):,}")

    # ------------------------------------------------------------------ #
    # STEP 2 — Filter: valid email + not gov/mil + dedupe                 #
    # ------------------------------------------------------------------ #
    print("\n=== Step 2: Filter ===")
    seen_emails: set = set()
    contacts: list = []  # list of (email, hr_first)

    invalid_email = gov_mil = duplicate = 0
    for r in raw_rows:
        email = get_email(r, fmt)
        hr_first = get_hr_first(r, fmt)
        if not valid_email(email):
            invalid_email += 1
            continue
        if is_gov_mil(email):
            gov_mil += 1
            continue
        e_low = email.lower()
        if e_low in seen_emails:
            duplicate += 1
            continue
        seen_emails.add(e_low)
        contacts.append((email, hr_first))

    total_contacts = len(contacts)
    print(f"  Invalid email  : {invalid_email:,}")
    print(f"  Gov/mil removed: {gov_mil:,}")
    print(f"  Duplicates     : {duplicate:,}")
    print(f"  Clean contacts : {total_contacts:,}  <- this is your final row count")

    # ------------------------------------------------------------------ #
    # STEP 3 — Build domain index                                         #
    # ------------------------------------------------------------------ #
    # domain -> list of (email, hr_first)
    domain_contacts: dict = {}
    no_domain = 0
    for email, hr_first in contacts:
        d = email_domain(email)
        if d:
            domain_contacts.setdefault(d, []).append((email, hr_first))
        else:
            no_domain += 1

    all_domains = sorted(domain_contacts.keys())
    total_domains = len(all_domains)
    print("\n=== Step 3: Domain index ===")
    print(f"  Unique domains : {total_domains:,}  (Tavily will search each once)")
    print(f"  Avg contacts/domain: {total_contacts / total_domains:.1f}")

    # ------------------------------------------------------------------ #
    # STEP 4 — Tavily enrichment                                          #
    # ------------------------------------------------------------------ #
    print("\n=== Step 4: Tavily enrichment ===")

    api_key = (os.getenv("TAVILY_API_KEY") or os.getenv("TAVILY_API_KEYS", "").split(",")[0].strip())
    use_tavily = HAS_TAVILY and bool(api_key) and not args.no_tavily

    cache, done = load_cache(reset=args.reset)
    if args.reset:
        print("  --reset: cache cleared")
    else:
        print(f"  Cache   : {len(cache):,} domains already processed")
        cached_with_ceo = sum(1 for v in cache.values() if v)
        cached_without_ceo = len(cache) - cached_with_ceo
        print(f"            {cached_with_ceo:,} with CEO | {cached_without_ceo:,} without")

    if strict_mode and cache:
        st = scrub_cache_strict(cache)
        if st["invalid_strict"] or st["frequency_wipe"]:
            save_cache(cache, done)
            print(
                f"  Strict scrub: removed {st['invalid_strict']:,} invalid + "
                f"{st['frequency_wipe']:,} over-repeated-name domains "
                f"({st['over_repeated_names']:,} names)"
            )

    remaining = [d for d in all_domains if d not in cache and d not in done]
    if args.limit:
        remaining = remaining[: args.limit]

    quota_hit = False
    if not use_tavily:
        reason = (
            "--no-tavily flag"
            if args.no_tavily
            else "no TAVILY_API_KEY in .env"
            if not api_key
            else "tavily-python not installed  (pip install tavily-python)"
        )
        print(f"  Skipped — {reason}")
        # Mark all remaining as not found (empty string)
        for d in remaining:
            cache[d] = ""
            done.add(d)
    else:
        client = TavilyClient(api_key)
        print(f"  Live lookups   : {len(remaining):,}")
        if remaining:
            print(f"\n  {'#':<8} {'Domain':<35} Result")
            print(f"  {'-' * 70}")

        live_hits = live_calls = save_ctr = 0
        t_tv = time.time()
        interrupted = False

        for domain in remaining:
            if quota_hit:
                break

            if live_calls > 0:
                eta = (time.time() - t_tv) / live_calls * (len(remaining) - live_calls)
                h, rem = divmod(int(eta), 3600)
                m2, s2 = divmod(rem, 60)
                eta_str = f"{h}h{m2:02d}m" if h else f"{m2}m{s2:02d}s"
            else:
                eta_str = "—"

            print(f"  [{live_calls + 1}/{len(remaining)}] {domain:<35} ETA {eta_str}", end=" ... ", flush=True)

            try:
                found = tv_lookup(client, domain, delay=args.delay, strict=strict_mode)
                live_calls += 1
            except KeyboardInterrupt:
                print("\n  Interrupted by user — saving progress and refreshing outputs...")
                interrupted = True
                break
            except RuntimeError as e:
                print(f"\n  Quota hit: {e}")
                quota_hit = True
                break
            except Exception as e:
                print(f"error ({e})")
                live_calls += 1
                found = ""

            ok, reason = is_valid_name(found, strict=strict_mode) if found else (False, "not found")
            if not ok:
                if found:
                    print(f"(rejected: {repr(found)} — {reason})", end=" ")
                found = ""

            cache[domain] = found
            done.add(domain)
            if found:
                live_hits += 1
                print(found)
                # Instant visibility: update output files as soon as a CEO is found.
                write_outputs_from_cache(contacts, cache, strict=strict_mode)
            else:
                print("(not found)")

            save_ctr += 1
            if save_ctr >= 25:
                save_cache(cache, done)
                save_ctr = 0

        save_cache(cache, done)
        write_outputs_from_cache(contacts, cache, strict=strict_mode)
        m2, s2 = divmod(int(time.time() - t_tv), 60)
        print(f"\n  Live calls  : {live_calls:,}  |  Found CEO : {live_hits:,}  ({m2}m{s2:02d}s)")
        if interrupted:
            print("  Run interrupted — outputs were updated from latest cache state.")
        if quota_hit:
            print("  Quota hit — update TAVILY_API_KEY in .env and re-run to continue.")
            print("  Progress is saved; already-processed domains will be skipped.")

    # ------------------------------------------------------------------ #
    # STEP 5 — Write output  (ALL contacts, one row each)                 #
    # ------------------------------------------------------------------ #
    print("\n=== Step 5: Write output ===")

    final_count, left_count = write_outputs_from_cache(contacts, cache, strict=strict_mode)

    m2, s2 = divmod(int(time.time() - t0), 60)
    covered = len([d for d in all_domains if cache.get(d)])
    missing_dom = len([d for d in all_domains if not (cache.get(d) or "").strip()])
    print(f"  Domains with CEO   : {covered:,} / {total_domains:,}")
    print(f"  Domains missing CEO (no/empty cache): {missing_dom:,}")
    print(f"  final.csv          : {final_count:,} rows")
    print(f"  left.csv           : {left_count:,} rows")
    print(f"  Total output rows  : {final_count + left_count:,}  (should equal {total_contacts:,})")
    if use_tavily:
        print(
            "  Note: Tavily usage dashboards count **API search requests**. "
            "This pipeline runs **up to 3 searches per domain** (tv_lookup). "
            "That total will be much larger than 'domains processed' or 'domains with CEO'."
        )

    print(f"\n{'=' * 65}")
    print(f"Pipeline complete  ({m2}m{s2:02d}s)")
    print(f"  final.csv : {final_count:,}  rows with verified CEO name")
    print(f"  left.csv  : {left_count:,}  rows without CEO name")
    if use_tavily and quota_hit:
        domains_left = len([d for d in all_domains if not cache.get(d) and d not in done])
        print(f"  Re-run with a fresh key — {domains_left:,} domains still to search")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()

