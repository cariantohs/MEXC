#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-shot STRONG-IMPACT Gold/XAU/XAUT news fetcher (Google News RSS)
Period: 2025-01-24 .. 2025-08-18 (weekly windows)
Queries: CPI/PCE/PPI, NFP, FOMC/Fed, DXY & yields, GLD ETF, WGC/central banks, Geopolitics/safe-haven, XAUT/Tether Gold
- Respects robots.txt (no paywall bypass)
- Extracts article text (trafilatura)
- Captures publish time to seconds (UTC) via meta/JSON-LD, fallback to feed time
- Derives 5m bar alignment: bar_5m_epoch, bar_5m_iso
Outputs:
  gold_news_strong/
    ├─ index.csv
    ├─ articles.jsonl
    └─ md/*.md
Env overrides (optional):
  START_DATE, END_DATE, WINDOW_DAYS, ALLOWED_LANGS (e.g. "en,id"), MIN_CHARS, FETCH_SLEEP, MAX_PER_WIN
"""

import sys, subprocess
def _pip_install(pkgs):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q"] + pkgs)

# Auto-install dependencies
for mod, pkgs in [
    ("feedparser", ["feedparser"]),
    ("trafilatura", ["trafilatura"]),
    ("bs4", ["beautifulsoup4"]),
    ("langdetect", ["langdetect"]),
    ("tldextract", ["tldextract"]),
    ("pandas", ["pandas"]),
    ("requests", ["requests"]),
]:
    try:
        __import__(mod)
    except Exception:
        _pip_install(pkgs)

import os, re, csv, json, time, hashlib, urllib.parse, datetime as dt
from pathlib import Path
from typing import Optional, Tuple, Set, Any, List
from email.utils import parsedate_to_datetime

import requests
import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException
import tldextract
import trafilatura
from urllib.robotparser import RobotFileParser

# ---------------- Config ----------------
START_DATE = os.getenv("START_DATE", "2025-01-24")   # inclusive
END_DATE   = os.getenv("END_DATE",   "2025-08-18")   # inclusive
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "7"))     # weekly
OUTDIR = Path(os.getenv("OUTDIR", "gold_news_strong"))
MAX_PER_WIN = int(os.getenv("MAX_PER_WIN", "40"))    # cap entries per query per window

_langs = os.getenv("ALLOWED_LANGS", "en,id").strip()
ALLOWED_LANGS = set([s.strip() for s in _langs.split(",") if s.strip()]) if _langs else set()

UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) GoldStrongNewsBot/1.0 Safari/537.36"
FETCH_SLEEP = float(os.getenv("FETCH_SLEEP", "0.6"))
MIN_CHARS = int(os.getenv("MIN_CHARS", "700"))

# Deny-list (social/UGC/paywall-heavy)
DENY_DOMAINS = {
    "facebook.com","twitter.com","x.com","linkedin.com","instagram.com","reddit.com",
    "youtube.com","tiktok.com","pinterest.com","quora.com","telegram.org"
}

# Strong-impact queries only
QUERIES = [
    # Inflation
    '("US CPI" OR "core CPI" OR "headline CPI" OR "PCE price index" OR "core PCE" OR "PPI")',
    # Labor market
    '("nonfarm payrolls" OR NFP OR "unemployment rate" OR "jobless claims")',
    # The Fed
    '(FOMC OR "Fed decision" OR "interest rate decision" OR "Fed minutes" OR "dot plot" OR "Powell speech")',
    # USD & Yields
    '("US dollar index" OR DXY OR "10-year Treasury yield" OR "real yields" OR TIPS)',
    # ETF flows (GLD)
    '("GLD holdings" OR "SPDR Gold Shares" OR "gold ETF flows")',
    # Central bank / WGC
    '("World Gold Council" OR "central bank gold purchases" OR "gold reserves")',
    # Geopolitics / Safe-haven
    '(Ukraine OR "Middle East" OR Gaza OR Iran OR Israel OR Taiwan OR "South China Sea" OR sanctions OR escalation OR "safe haven")',
    # Crypto gold (XAUT)
    '("XAUT" OR "XAUT/USDT" OR "Tether Gold")',
]

# Content relevance filters (regex must hit to keep)
CONTENT_KEYWORDS = [
    r"\bXAU\b", r"\bXAUUSD\b", r"\bgold\b", r"\bemas\b", r"\bbullion\b",
    r"\bCPI\b|\bPCE\b|\bPPI\b|\binflation\b",
    r"\bNFP\b|\bnonfarm\b|\bunemployment\b|\bjobless\b",
    r"\bFOMC\b|\bFed\b|\binterest rate\b|\bminutes\b|\bdot plot\b|\bPowell\b",
    r"\bDXY\b|\bdollar index\b|\bTreasury\b|\byield\b|\breal yield\b|\bTIPS\b",
    r"\bGLD\b|\bSPDR\b|\bETF\b|\bWorld Gold Council\b|\bcentral bank\b|\breserves\b",
    r"\bUkraine\b|\bGaza\b|\bIran\b|\bIsrael\b|\bTaiwan\b|\bsanctions\b|\bescalation\b|\bsafe haven\b",
    r"\bXAUT\b|\bXAUT/USDT\b|\bTether Gold\b",
]

# Topic flags (binary) for modeling later
TOPIC_FLAGS = {
    "flag_cpi": r"\bCPI\b|\bPCE\b|\bPPI\b|\binflation\b",
    "flag_fed": r"\bFOMC\b|\bFed\b|\binterest rate\b|\bminutes\b|\bdot plot\b|\bPowell\b",
    "flag_jobs": r"\bNFP\b|\bnonfarm\b|\bunemployment\b|\bjobless\b",
    "flag_dxy_yield": r"\bDXY\b|\bdollar index\b|\bTreasury\b|\byield\b|\breal yield\b|\bTIPS\b",
    "flag_etf": r"\bGLD\b|\bSPDR\b|\bETF\b",
    "flag_cbgold": r"\bWorld Gold Council\b|\bcentral bank\b|\breserves\b",
    "flag_geopol": r"\bUkraine\b|\bGaza\b|\bIran\b|\bIsrael\b|\bTaiwan\b|\bsanctions\b|\bescalation\b|\bsafe haven\b",
    "flag_xaut": r"\bXAUT\b|\bXAUT/USDT\b|\bTether Gold\b",
}

# ---------------- Helpers ----------------
def iter_windows(start_date: str, end_date: str, step_days: int = 7):
    s = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    e = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    cur = s
    while cur <= e:
        nxt = cur + dt.timedelta(days=step_days-1)
        if nxt > e: nxt = e
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + dt.timedelta(days=1)

def google_news_rss_url(query: str, after: str, before: str, hl="en", gl="US", ceid="US:en") -> str:
    q = f'{query} after:{after} before:{before}'
    params = {"q": q, "hl": hl, "gl": gl, "ceid": ceid}
    return "https://news.google.com/rss/search?" + urllib.parse.urlencode(params)

def get_domain(url: str) -> str:
    ext = tldextract.extract(url)
    return ".".join([p for p in [ext.domain, ext.suffix] if p]).lower()

def robots_ok(url: str) -> bool:
    try:
        p = urllib.parse.urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        rp = RobotFileParser()
        rp.set_url(urllib.parse.urljoin(base, "/robots.txt"))
        rp.read()
        return rp.can_fetch(UA, url)
    except Exception:
        return False

def clean_url(u: str) -> str:
    try:
        p = urllib.parse.urlparse(u)
        q = urllib.parse.parse_qs(p.query)
        q2 = {k:v for k,v in q.items() if not (k.startswith("utm_") or k in {"fbclid","gclid","igshid"})}
        new_q = urllib.parse.urlencode({k:(v[0] if isinstance(v,list) and len(v)==1 else ",".join(v)) for k,v in q2.items()})
        p = p._replace(query=new_q)
        return urllib.parse.urlunparse(p)
    except Exception:
        return u

def extract_text(url: str) -> Optional[str]:
    if not robots_ok(url):
        return None
    try:
        downloaded = trafilatura.fetch_url(url, user_agent=UA)
        if not downloaded:
            return None
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False, favor_recall=True)
        return text
    except Exception:
        return None

def parse_html(url: str) -> Optional[BeautifulSoup]:
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html"}, timeout=20)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type",""):
            return BeautifulSoup(r.text, "html.parser")
    except Exception:
        return None
    return None

def to_utc_iso_unix(s: str):
    try:
        dtiso = None
        try:
            dtiso = dt.datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception:
            pass
        if dtiso is None:
            dtiso = parsedate_to_datetime(s)
        if dtiso.tzinfo is None:
            dtiso = dtiso.replace(tzinfo=dt.timezone.utc)
        dtutc = dtiso.astimezone(dt.timezone.utc)
        return dtutc.isoformat(timespec="seconds"), int(dtutc.timestamp())
    except Exception:
        return None

def extract_published_dt(url: str, feed_published: Optional[str]):
    soup = parse_html(url)
    candidates = []
    if soup:
        metas = [
            ("meta", {"property":"article:published_time"}, "content"),
            ("meta", {"property":"og:published_time"}, "content"),
            ("meta", {"name":"pubdate"}, "content"),
            ("meta", {"name":"parsely-pub-date"}, "content"),
            ("meta", {"itemprop":"datePublished"}, "content"),
            ("meta", {"name":"date"}, "content"),
        ]
        for tag, attrs, attr_name in metas:
            el = soup.find(tag, attrs=attrs)
            if el and el.get(attr_name):
                candidates.append(("meta_"+list(attrs.values())[0], el.get(attr_name)))
        for t in soup.find_all("time"):
            if t.get("datetime"):
                candidates.append(("time_datetime", t.get("datetime")))
        for sc in soup.find_all("script", {"type":"application/ld+json"}):
            try:
                data = json.loads(sc.string or "")
            except Exception:
                continue
            def scan(obj: Any):
                if isinstance(obj, dict):
                    typ = obj.get("@type","")
                    if isinstance(typ, list): typ = ",".join(typ)
                    if any(k in str(typ).lower() for k in ["article","newsarticle","report","blogposting"]):
                        for k in ["datePublished","dateCreated","uploadDate"]:
                            if k in obj and obj[k]:
                                candidates.append(("jsonld_"+k, obj[k]))
                    for v in obj.values(): scan(v)
                elif isinstance(obj, list):
                    for it in obj: scan(it)
            scan(data)

    for source, val in candidates:
        parsed = to_utc_iso_unix(val)
        if parsed:
            iso, uni = parsed
            return iso, uni, source

    if feed_published:
        parsed = to_utc_iso_unix(feed_published)
        if parsed:
            iso, uni = parsed
            return iso, uni, "feed_published"

    return None, None, "unknown"

def relevant_text(text: str) -> bool:
    if not text: return False
    t = text.lower()
    return any(re.search(pat, t, flags=re.IGNORECASE) for pat in CONTENT_KEYWORDS)

def topic_flags(text: str) -> dict:
    t = (text or "").lower()
    return {k: 1 if re.search(pat, t, flags=re.IGNORECASE) else 0 for k, pat in TOPIC_FLAGS.items()}

# ---------------- Main ----------------
def main():
    (OUTDIR / "md").mkdir(parents=True, exist_ok=True)
    index_csv = OUTDIR / "index.csv"
    index_jsonl = OUTDIR / "articles.jsonl"

    csv_exists = index_csv.exists()
    cf = open(index_csv, "a", encoding="utf-8", newline="")
    fieldnames = [
        "published_iso_utc","published_unix_utc","published_source",
        "bar_5m_epoch","bar_5m_iso",
        "title","url","domain","chars","lang",
        "query","window_start","window_end","saved_md"
    ] + list(TOPIC_FLAGS.keys())
    cw = csv.DictWriter(cf, fieldnames=fieldnames)
    if not csv_exists:
        cw.writeheader()

    jf = open(index_jsonl, "a", encoding="utf-8")

    seen: Set[str] = set()
    saved = 0

    def bar5m(unix):
        if unix is None: return None, ""
        e = int(unix // 300 * 300)
        iso = dt.datetime.utcfromtimestamp(e).isoformat(timespec="seconds") + "Z"
        return e, iso

    for (win_a, win_b) in iter_windows(START_DATE, END_DATE, WINDOW_DAYS):
        print(f"\n== Window {win_a} .. {win_b} ==")
        for q in QUERIES:
            rss = google_news_rss_url(q, win_a, win_b, hl="en", gl="US", ceid="US:en")
            feed = feedparser.parse(rss)
            entries = getattr(feed, "entries", [])
            print(f"  - Query: {q[:58]}... → {len(entries)} items (capped {MAX_PER_WIN})")

            count = 0
            for e in entries:
                if count >= MAX_PER_WIN: break
                link = e.get("link") or e.get("id")
                if not link: continue
                link = clean_url(link)
                if link in seen: continue
                seen.add(link)

                dom = get_domain(link)
                if dom in DENY_DOMAINS: continue

                feed_pub = e.get("published") or e.get("updated") or None
                pub_iso, pub_unix, pub_src = extract_published_dt(link, feed_pub)

                text = extract_text(link)
                if not text or len(text) < MIN_CHARS:
                    time.sleep(FETCH_SLEEP); continue
                if not relevant_text(text):
                    time.sleep(FETCH_SLEEP); continue

                try:
                    lang = detect(text[:2000])
                except LangDetectException:
                    lang = None
                if ALLOWED_LANGS and lang and lang not in ALLOWED_LANGS:
                    time.sleep(FETCH_SLEEP); continue

                title = e.get("title") or dom
                slug = re.sub(r"[^\w\s-]+","", title).strip()
                slug = re.sub(r"\s+","-", slug)[:80] or "untitled"
                hsh = hashlib.md5(link.encode("utf-8")).hexdigest()[:10]
                md_path = OUTDIR / "md" / f"{slug}-{hsh}.md"

                b5e, b5i = bar5m(pub_unix)
                header = "\n".join([
                    f"# {title}",
                    f"URL: {link}",
                    f"Domain: {dom}",
                    f"Published(UTC): {pub_iso or ''}",
                    f"PublishedUnix: {pub_unix or ''}",
                    f"Bar5mEpoch: {b5e if b5e is not None else ''}",
                    f"Bar5mISO: {b5i}",
                    f"PublishedSource: {pub_src}",
                    f"Window: {win_a}..{win_b}",
                    f"Query: {q}",
                    ""
                ])
                md_path.write_text(header + text, encoding="utf-8")

                flags = topic_flags(text)
                row = {
                    "published_iso_utc": pub_iso or "",
                    "published_unix_utc": pub_unix or "",
                    "published_source": pub_src,
                    "bar_5m_epoch": b5e if b5e is not None else "",
                    "bar_5m_iso": b5i,
                    "title": title,
                    "url": link,
                    "domain": dom,
                    "chars": len(text),
                    "lang": lang or "",
                    "query": q,
                    "window_start": win_a,
                    "window_end": win_b,
                    "saved_md": str(md_path)
                }
                row.update(flags)
                cw.writerow(row)
                jf.write(json.dumps({**row, "md": str(md_path)}, ensure_ascii=False) + "\n")

                saved += 1
                count += 1
                time.sleep(FETCH_SLEEP)

    cf.close(); jf.close()
    print(f"\nDone. Saved {saved} articles.")
    print(f"Index CSV : {OUTDIR/'index.csv'}")
    print(f"JSONL     : {OUTDIR/'articles.jsonl'}")
    print(f"Markdown  : {len(list((OUTDIR/'md').glob('*.md')))} files in {OUTDIR/'md'}")

if __name__ == "__main__":
    main()
