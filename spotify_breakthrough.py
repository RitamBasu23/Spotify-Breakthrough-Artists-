#!/usr/bin/env python3
"""
Spotify Breakthrough Artists 2025 — REAL DATA pipeline
======================================================

Which niche artists broke through to mass popularity in 2025?

WHY TWO SOURCES
---------------
Since Feb 2026, Development Mode apps no longer get artist/track `popularity`
or `followers` from the Spotify Web API, and the API never had history anyway.
So this pipeline uses:

  1. Spotify Charts weekly CSVs (charts.spotify.com) -> REAL weekly ranks and
     stream counts for 2024-2025, straight from Spotify. This is the
     popularity signal.
  2. Spotify Web API -> artist IDs (correct splitting of "A, B" credits),
     genres, artist photos, album/release dates.

QUICK START
-----------
  pip install pandas numpy openpyxl requests

  1. Download weekly chart CSVs (see README in the reply / --help-charts)
     into ./charts/  e.g. regional-global-weekly-2025-01-02.csv
  2. Spotify Developer Dashboard -> your app -> add Redirect URI
         http://127.0.0.1:8888/callback
  3. export SPOTIFY_CLIENT_ID=your_client_id
  4. python spotify_breakthrough.py --check-api          # see what your app gets
  5. python spotify_breakthrough.py --charts charts      # full run

  No API access? Add --no-api (charts only; no genres/photos/release dates).
  Test the pipeline with fake files: --make-sample-charts sample_charts

OUTPUT
  spotify_breakthrough_2025.xlsx  (Power BI star schema, one table per sheet)
  powerbi_theme_breakthrough.json
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import http.server
import json
import os
import re
import secrets
import sys
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# =============================================================================
# CONFIG
# =============================================================================
@dataclass
class Config:
    analysis_year: int = 2025
    mass_rank: int = 50          # reaching this rank or better = mass popularity
    min_weeks: int = 8           # charting weeks in year needed for "Breakthrough"
    sustain_min: float = 0.60    # share of weeks after breakout still on the chart
    rising_weeks: int = 4        # charting weeks for "Rising" (without top-50)

    @property
    def year_start(self):
        return pd.Timestamp(self.analysis_year, 1, 1)

    @property
    def year_end(self):
        return pd.Timestamp(self.analysis_year, 12, 31)


CATEGORY_ORDER = {"Breakthrough": 1, "Flash Breakout": 2, "Rising": 3,
                  "Chart Entry": 4, "Returning": 5, "Established": 6}
CATEGORY_COLORS = {"Breakthrough": "#3DF5A0", "Flash Breakout": "#FF4FD8", "Rising": "#FFB347",
                   "Chart Entry": "#B388FF", "Returning": "#7A8199", "Established": "#4FA3FF"}
REGION_NAMES = {
    "global": "Global", "au": "Australia", "us": "United States", "gb": "United Kingdom",
    "ca": "Canada", "nz": "New Zealand", "ie": "Ireland", "de": "Germany", "fr": "France",
    "es": "Spain", "it": "Italy", "nl": "Netherlands", "se": "Sweden", "no": "Norway",
    "br": "Brazil", "mx": "Mexico", "ar": "Argentina", "co": "Colombia", "cl": "Chile",
    "in": "India", "jp": "Japan", "kr": "South Korea", "ph": "Philippines", "id": "Indonesia",
    "ng": "Nigeria", "za": "South Africa", "pl": "Poland", "tr": "Türkiye",
}


def stable_id(text: str) -> str:
    return "x" + hashlib.md5(text.strip().lower().encode()).hexdigest()[:21]


# =============================================================================
# 1) SPOTIFY CHARTS CSV LOADER
# =============================================================================
FNAME_RE = re.compile(r"regional-([a-z]{2,6})-weekly-(\d{4}-\d{2}-\d{2})", re.I)
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def read_chart_file(path: Path, default_region: str):
    m = FNAME_RE.search(path.name)
    if m:
        region, d = m.group(1).lower(), m.group(2)
    else:
        dm = DATE_RE.search(path.name)
        if not dm:
            print(f"  ! skipped {path.name}: no YYYY-MM-DD date in file name")
            return None
        region, d = default_region, dm.group(1)

    with open(path, encoding="utf-8-sig") as f:
        first = f.readline().lower()
    skip = 0 if ("rank" in first or "position" in first) else 1  # legacy files had a note line
    df = pd.read_csv(path, skiprows=skip)
    cols = {c.lower().strip(): c for c in df.columns}

    def pick(*names):
        for n in names:
            if n in cols:
                return df[cols[n]]
        return None

    rank, uri = pick("rank", "position"), pick("uri", "url", "track_uri")
    artists, track = pick("artist_names", "artist", "artists"), pick("track_name", "track name", "track")
    streams = pick("streams")
    if rank is None or uri is None or artists is None:
        print(f"  ! skipped {path.name}: unexpected columns {list(df.columns)}")
        return None
    out = pd.DataFrame({
        "region": region,
        "chart_week": pd.Timestamp(d),
        "rank": pd.to_numeric(rank, errors="coerce"),
        "track_id": uri.astype(str).str.extract(r"track[:/]([A-Za-z0-9]{22})")[0],
        "track_name": track if track is not None else "",
        "chart_artist_names": artists.astype(str),
        "streams": pd.to_numeric(streams, errors="coerce") if streams is not None else np.nan,
    })
    for c in ("weeks_on_chart", "peak_rank", "previous_rank"):
        s = pick(c)
        out[c] = pd.to_numeric(s, errors="coerce") if s is not None else np.nan
    return out.dropna(subset=["rank", "track_id"])


def load_charts(folder: str, default_region: str) -> pd.DataFrame:
    files = sorted(Path(folder).glob("*.csv"))
    if not files:
        sys.exit(f"No .csv files in '{folder}'. Download weekly charts first (see --help-charts).")
    frames = [f for f in (read_chart_file(p, default_region) for p in files) if f is not None]
    if not frames:
        sys.exit("No readable chart files.")
    ch = pd.concat(frames, ignore_index=True).drop_duplicates(["region", "chart_week", "rank"])
    ch["rank"] = ch["rank"].astype(int)

    for reg, g in ch.groupby("region"):
        weeks = sorted(g["chart_week"].unique())
        expected = pd.date_range(weeks[0], weeks[-1], freq="7D")
        missing = sorted(set(expected) - set(weeks))
        print(f"  {REGION_NAMES.get(reg, reg.upper())}: {len(weeks)} weeks "
              f"({pd.Timestamp(weeks[0]).date()} -> {pd.Timestamp(weeks[-1]).date()})"
              + (f", {len(missing)} missing e.g. {pd.Timestamp(missing[0]).date()}" if missing else ""))
    return ch


# =============================================================================
# 2) SPOTIFY WEB API (PKCE login, or client credentials fallback)
# =============================================================================
class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    result: dict = {}

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if "code" in q or "error" in q:
            _CallbackHandler.result.update({k: v[0] for k, v in q.items()})
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h2 style='font-family:sans-serif'>Spotify login done. You can close this tab.</h2>")

    def log_message(self, *a):
        pass


class SpotifyAPI:
    BASE = "https://api.spotify.com/v1"
    AUTH_URL = "https://accounts.spotify.com/authorize"
    TOKEN_URL = "https://accounts.spotify.com/api/token"

    def __init__(self, client_id, client_secret=None, auth="pkce",
                 redirect_uri="http://127.0.0.1:8888/callback", token_cache=Path(".spotify_token.json"),
                 pause=0.12):
        try:
            import requests
        except ImportError:
            sys.exit("pip install requests")
        self.rq = requests
        self.client_id, self.client_secret, self.auth = client_id, client_secret, auth
        self.redirect_uri, self.token_cache, self.pause = redirect_uri, Path(token_cache), pause
        self.tok: dict = {}
        self.calls = 0

    # ---- auth ----
    def _save(self, js):
        js["expires_at"] = time.time() + js.get("expires_in", 3600) - 60
        if "refresh_token" not in js and "refresh_token" in self.tok:
            js["refresh_token"] = self.tok["refresh_token"]
        self.tok = js
        if self.auth == "pkce":
            self.token_cache.write_text(json.dumps(js))

    def _client_credentials(self):
        if not self.client_secret:
            sys.exit("--auth client needs SPOTIFY_CLIENT_SECRET.")
        r = self.rq.post(self.TOKEN_URL, data={"grant_type": "client_credentials"},
                         auth=(self.client_id, self.client_secret), timeout=30)
        r.raise_for_status()
        self._save(r.json())

    def _pkce_login(self):
        verifier = secrets.token_urlsafe(64)[:100]
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        state = secrets.token_urlsafe(12)
        url = self.AUTH_URL + "?" + urllib.parse.urlencode({
            "client_id": self.client_id, "response_type": "code", "redirect_uri": self.redirect_uri,
            "code_challenge_method": "S256", "code_challenge": challenge, "state": state})
        p = urllib.parse.urlparse(self.redirect_uri)
        server = http.server.HTTPServer((p.hostname, p.port or 80), _CallbackHandler)
        server.timeout = 300
        print("\nOpening Spotify login in your browser. If it doesn't open, visit:\n" + url + "\n")
        webbrowser.open(url)
        _CallbackHandler.result = {}
        deadline = time.time() + 300
        while not _CallbackHandler.result and time.time() < deadline:
            server.handle_request()
        server.server_close()
        res = _CallbackHandler.result
        if "code" not in res or res.get("state") != state:
            sys.exit(f"Login failed: {res.get('error', 'no code received / state mismatch')}")
        r = self.rq.post(self.TOKEN_URL, timeout=30, data={
            "grant_type": "authorization_code", "code": res["code"], "redirect_uri": self.redirect_uri,
            "client_id": self.client_id, "code_verifier": verifier})
        r.raise_for_status()
        self._save(r.json())

    def _refresh(self):
        r = self.rq.post(self.TOKEN_URL, timeout=30, data={
            "grant_type": "refresh_token", "refresh_token": self.tok["refresh_token"],
            "client_id": self.client_id})
        if r.ok:
            self._save(r.json())
            return True
        return False

    def _ensure_token(self):
        if self.tok and time.time() < self.tok["expires_at"]:
            return
        if self.auth == "client":
            return self._client_credentials()
        if not self.tok and self.token_cache.exists():
            self.tok = json.loads(self.token_cache.read_text())
            if time.time() < self.tok.get("expires_at", 0):
                return
        if self.tok.get("refresh_token") and self._refresh():
            return
        self._pkce_login()

    # ---- requests ----
    def get(self, path, params=None, retries=6):
        url = path if path.startswith("http") else self.BASE + path
        for attempt in range(retries):
            self._ensure_token()
            r = self.rq.get(url, params=params, timeout=30,
                            headers={"Authorization": f"Bearer {self.tok['access_token']}"})
            self.calls += 1
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5)) + 1
                print(f"    rate limited -> waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 401:
                self.tok["expires_at"] = 0
                continue
            if r.status_code == 403:
                print(f"    403 Forbidden on {path} (endpoint not available to this app?)")
                return None
            if r.status_code == 404:
                return None
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            time.sleep(self.pause)
            return r.json()
        return None


def _json_cache(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _save_cache(path: Path, data):
    path.write_text(json.dumps(data), encoding="utf-8")


def enrich_with_api(api: SpotifyAPI, charts: pd.DataFrame, cache_dir: Path, fetch_catalog: bool):
    """Per-track and per-artist lookups (batch endpoints are gone -> one call each, cached)."""
    cache_dir.mkdir(exist_ok=True)
    tpath, apath, cpath = cache_dir / "tracks.json", cache_dir / "artists.json", cache_dir / "catalog.json"
    tracks, artists, catalog = _json_cache(tpath), _json_cache(apath), _json_cache(cpath)

    todo = [t for t in charts["track_id"].unique() if t not in tracks]
    print(f"API: {len(todo)} tracks to look up ({len(tracks)} cached)")
    for i, tid in enumerate(todo, 1):
        js = api.get(f"/tracks/{tid}")
        if js:
            al = js.get("album") or {}
            tracks[tid] = {
                "name": js.get("name"),
                "artists": [{"id": a["id"], "name": a["name"]} for a in js.get("artists", []) if a.get("id")],
                "album_id": al.get("id"), "album_name": al.get("name"), "album_type": al.get("album_type"),
                "release_date": al.get("release_date"), "release_precision": al.get("release_date_precision"),
                "explicit": js.get("explicit"), "duration_ms": js.get("duration_ms"),
                "popularity": js.get("popularity"),  # None in Development Mode
            }
        else:
            tracks[tid] = None
        if i % 100 == 0:
            _save_cache(tpath, tracks)
            print(f"    {i}/{len(todo)} tracks")
    _save_cache(tpath, tracks)

    artist_ids = {a["id"] for t in tracks.values() if t for a in t["artists"]}
    todo = [a for a in artist_ids if a not in artists]
    print(f"API: {len(todo)} artists to look up ({len(artists) - 0} cached)")
    for i, aid in enumerate(todo, 1):
        js = api.get(f"/artists/{aid}")
        if js:
            imgs = js.get("images") or []
            artists[aid] = {"name": js.get("name"), "genres": js.get("genres") or [],
                            "image_url": imgs[0]["url"] if imgs else "",
                            "spotify_url": (js.get("external_urls") or {}).get("spotify", ""),
                            "popularity": js.get("popularity"),
                            "followers": (js.get("followers") or {}).get("total")}
        else:
            artists[aid] = None
        if i % 100 == 0:
            _save_cache(apath, artists)
            print(f"    {i}/{len(todo)} artists")
    _save_cache(apath, artists)
    return tracks, artists, catalog, cpath


def fetch_artist_catalogs(api: SpotifyAPI, artist_ids, catalog: dict, cpath: Path, max_pages=6):
    """Albums/singles per candidate artist -> debut date + releases around the breakout."""
    todo = [a for a in artist_ids if a not in catalog]
    print(f"API: catalogs for {len(todo)} candidate artists")
    for i, aid in enumerate(todo, 1):
        items, url, params, pages = [], f"/artists/{aid}/albums", {"include_groups": "album,single", "limit": 10}, 0
        while url and pages < max_pages:
            js = api.get(url, params)
            if not js:
                break
            for a in js.get("items", []):
                items.append({"id": a["id"], "name": a["name"], "type": a.get("album_type"),
                              "date": a.get("release_date"), "precision": a.get("release_date_precision"),
                              "total_tracks": a.get("total_tracks")})
            url, params, pages = js.get("next"), None, pages + 1
        catalog[aid] = items
        if i % 50 == 0:
            _save_cache(cpath, catalog)
    _save_cache(cpath, catalog)
    return catalog


def _parse_release(d, precision):
    if not d:
        return pd.NaT
    if precision == "year" or len(d) == 4:
        d += "-01-01"
    elif precision == "month" or len(d) == 7:
        d += "-01"
    return pd.to_datetime(d, errors="coerce")


def check_api(api: SpotifyAPI, sample_track="4cOdK2wGLETKBW3PvgPWqT"):
    print("\nTesting your Spotify app ...")
    t = api.get(f"/tracks/{sample_track}")
    if not t:
        sys.exit("Track lookup failed. Check client ID, redirect URI and that your account is Premium.")
    aid = t["artists"][0]["id"]
    a = api.get(f"/artists/{aid}") or {}
    alb = api.get(f"/artists/{aid}/albums", {"limit": 5})
    print(f"  GET /tracks/{{id}}          OK  ({t['name']} - {t['artists'][0]['name']})")
    print(f"  track.popularity           {'AVAILABLE' if t.get('popularity') is not None else 'not returned'}")
    print(f"  GET /artists/{{id}}         {'OK' if a else 'FAILED'}")
    print(f"  artist.genres              {a.get('genres') or 'empty'}")
    print(f"  artist.popularity          {'AVAILABLE' if a.get('popularity') is not None else 'not returned'}")
    print(f"  artist.followers           {'AVAILABLE' if (a.get('followers') or {}).get('total') is not None else 'not returned'}")
    print(f"  GET /artists/{{id}}/albums  {'OK' if alb else 'FAILED'}")
    print(f"  API calls made: {api.calls}\n")


# =============================================================================
# 3) BUILD MODEL TABLES
# =============================================================================
GENRE_RULES = [
    ("Hyperpop", ["hyperpop", "digicore", "glitch"]), ("Latin", ["reggaeton", "latin", "corrido", "urbano", "sierreño", "banda", "cumbia"]),
    ("Afrobeats", ["afrobeat", "afropop", "alté", "naija"]), ("Amapiano", ["amapiano", "afro house", "gqom"]),
    ("K-Pop", ["k-pop", "korean"]), ("UK Rap", ["drill", "grime", "uk rap"]), ("Phonk", ["phonk"]),
    ("Country", ["country", "americana", "red dirt"]), ("R&B / Soul", ["r&b", "soul"]),
    ("Shoegaze / Dream Pop", ["shoegaze", "dream pop"]), ("Folk", ["folk"]),
    ("Hip-Hop", ["hip hop", "rap", "trap"]), ("Electronic", ["house", "garage", "edm", "techno", "electronic", "dance"]),
    ("Rock / Indie", ["rock", "punk", "metal", "indie", "alternative"]), ("Pop", ["pop"]),
]


def genre_group(genres) -> str:
    g = " | ".join(genres).lower() if isinstance(genres, list) else str(genres or "").lower()
    if not g.strip():
        return "Unknown"
    for name, keys in GENRE_RULES:
        if any(k in g for k in keys):
            return name
    return "Other"


def build_bridge(charts, tracks_api):
    """track -> artist bridge (API credits if available, else split chart names)."""
    rows, track_rows = [], []
    first_meta = charts.drop_duplicates("track_id").set_index("track_id")
    for tid, meta in first_meta.iterrows():
        t = (tracks_api or {}).get(tid)
        if t and t.get("artists"):
            credits = [(a["id"], a["name"]) for a in t["artists"]]
            rel = _parse_release(t.get("release_date"), t.get("release_precision"))
            name, album, atype = t.get("name") or meta["track_name"], t.get("album_name"), t.get("album_type")
            album_id = t.get("album_id")
        else:
            names = [n.strip() for n in str(meta["chart_artist_names"]).split(",") if n.strip()]
            credits = [(stable_id(n), n) for n in names]
            rel, name, album, atype, album_id = pd.NaT, meta["track_name"], None, None, None
        for pos, (aid, an) in enumerate(credits, 1):
            rows.append({"track_id": tid, "artist_id": aid, "artist_name": an, "credit_position": pos})
        track_rows.append({"track_id": tid, "track_name": name, "primary_artist_id": credits[0][0],
                           "artist_names": ", ".join(c[1] for c in credits), "album_id": album_id,
                           "album_name": album, "album_type": atype, "release_date": rel,
                           "spotify_url": f"https://open.spotify.com/track/{tid}"})
    return pd.DataFrame(rows), pd.DataFrame(track_rows)


def analyze(charts, bridge, tracks, cfg: Config):
    # Artist-week facts: every credited artist gets the track's streams (features included)
    ce = charts.merge(bridge[["track_id", "artist_id"]], on="track_id")
    weekly = (ce.groupby(["artist_id", "region", "chart_week"], as_index=False)
                .agg(streams=("streams", "sum"), best_rank=("rank", "min"),
                     charting_tracks=("track_id", "nunique")))
    if weekly["streams"].fillna(0).eq(0).all():
        weekly["streams"] = np.nan

    rows = []
    for reg, wr in weekly.groupby("region"):
        all_weeks = np.array(sorted(charts.loc[charts.region == reg, "chart_week"].unique()))
        base_weeks = all_weeks[all_weeks < np.datetime64(cfg.year_start)]
        year_weeks = all_weeks[(all_weeks >= np.datetime64(cfg.year_start)) & (all_weeks <= np.datetime64(cfg.year_end))]
        if len(year_weeks) == 0:
            print(f"  ! {reg}: no chart weeks inside {cfg.analysis_year}, skipped")
            continue
        if len(base_weeks) < 13:
            print(f"  ! {REGION_NAMES.get(reg, reg)}: only {len(base_weeks)} baseline weeks before "
                  f"{cfg.analysis_year}. Download 2024 charts too, or established artists may look 'new'.")
        reg_entries = ce[ce.region == reg]
        for aid, g in wr.groupby("artist_id"):
            yr = g[(g.chart_week >= cfg.year_start) & (g.chart_week <= cfg.year_end)].sort_values("chart_week")
            if yr.empty:
                continue
            base_n = int((g.chart_week < cfg.year_start).sum())
            first_entry = yr.chart_week.iloc[0]
            # prior history outside our window: track already had weeks_on_chart > 1 at first sighting
            first_seen = reg_entries[(reg_entries.artist_id == aid) & (reg_entries.chart_week == g.chart_week.min())]
            returning = bool(base_n == 0 and first_seen["weeks_on_chart"].fillna(1).max() > 1
                             and g.chart_week.min() > all_weeks[0])
            best_rank = int(yr.best_rank.min())
            best_week = yr.loc[yr.best_rank.idxmin(), "chart_week"]
            hits = yr[yr.best_rank <= cfg.mass_rank]
            breakout = hits.chart_week.iloc[0] if len(hits) else pd.NaT
            weeks_on = len(yr)
            sustain = np.nan
            if pd.notna(breakout):
                remaining = int((year_weeks >= np.datetime64(breakout)).sum())
                sustain = (yr.chart_week >= breakout).sum() / max(remaining, 1)
            after_first = int((year_weeks >= np.datetime64(first_entry)).sum())
            presence = weeks_on / max(after_first, 1)

            if base_n > 0:
                cat = "Established"
            elif returning:
                cat = "Returning"
            elif pd.notna(breakout) and weeks_on >= cfg.min_weeks and sustain >= cfg.sustain_min:
                cat = "Breakthrough"
            elif pd.notna(breakout):
                cat = "Flash Breakout"
            elif weeks_on >= cfg.rising_weeks:
                cat = "Rising"
            else:
                cat = "Chart Entry"

            yr_tracks = reg_entries[(reg_entries.artist_id == aid) & (reg_entries.chart_week.between(cfg.year_start, cfg.year_end))]
            top = yr_tracks.sort_values(["rank", "streams"], ascending=[True, False]).iloc[0]
            rows.append({
                "artist_id": aid, "region": reg, "category": cat,
                "category_sort": CATEGORY_ORDER[cat], "category_color": CATEGORY_COLORS[cat],
                "is_new_to_chart": cat not in ("Established", "Returning"),
                "baseline_chart_weeks": base_n,
                "first_chart_week": first_entry, "breakout_week": breakout,
                "breakout_month": breakout.strftime("%Y-%m") if pd.notna(breakout) else None,
                "weeks_to_breakout": int((breakout - first_entry).days // 7) if pd.notna(breakout) else np.nan,
                "best_rank": best_rank, "best_rank_week": best_week,
                "weeks_on_chart": weeks_on, "weeks_in_top_mass": len(hits),
                "chart_presence": round(presence, 3),
                "sustain_ratio": round(sustain, 3) if pd.notna(sustain) else np.nan,
                "total_streams": yr.streams.sum(min_count=1),
                "peak_week_streams": yr.streams.max(),
                "charting_tracks": int(yr_tracks.track_id.nunique()),
                "top_track_id": top.track_id, "top_track": top.track_name,
                "_weeks_to_best": int((best_week - first_entry).days // 7),
            })

    summ = pd.DataFrame(rows)
    if summ.empty:
        sys.exit("No artists charted in the analysis year.")

    # Top-track release info + breakthrough score
    tmeta = tracks.set_index("track_id")
    summ["top_track_release_date"] = summ["top_track_id"].map(tmeta["release_date"])
    summ["days_release_to_first_chart"] = (summ["first_chart_week"] - summ["top_track_release_date"]).dt.days

    new = summ["is_new_to_chart"]
    lg = np.log10(summ["total_streams"].astype(float).clip(lower=1))
    norm = summ.assign(lg=lg).groupby("region")["lg"].transform(lambda s: (s - s.min()) / (s.max() - s.min() or 1))
    comp = (0.30 * ((201 - summ["best_rank"]) / 200).clip(0, 1)
            + 0.20 * norm.fillna(0)
            + 0.25 * summ["sustain_ratio"].fillna(summ["chart_presence"] * 0.5)
            + 0.10 * (1 - summ["_weeks_to_best"].clip(upper=26) / 26)
            + 0.15 * (summ["charting_tracks"].clip(upper=5) / 5))
    summ["breakthrough_score"] = np.where(new, (100 * comp).round(1), np.nan)
    summ["breakthrough_rank"] = summ[new].groupby("region")["breakthrough_score"].rank(ascending=False, method="min")
    summ = summ.drop(columns="_weeks_to_best").sort_values(["region", "category_sort", "breakthrough_score"],
                                                           ascending=[True, True, False])

    # Weekly fact enrichment
    bw = summ.set_index(["artist_id", "region"])["breakout_week"]
    weekly = weekly.merge(bw.reset_index(), on=["artist_id", "region"], how="left")
    weekly["weeks_since_breakout"] = (weekly["chart_week"] - weekly["breakout_week"]).dt.days // 7
    weekly["in_top_mass"] = weekly["best_rank"] <= cfg.mass_rank
    weekly = weekly.drop(columns="breakout_week")
    weekly = weekly[weekly.set_index(["artist_id", "region"]).index.isin(summ.set_index(["artist_id", "region"]).index)]
    return summ, weekly


def build_artists(bridge, summ, artists_api, catalog, cfg):
    names = bridge.groupby("artist_id")["artist_name"].first()
    ids = summ["artist_id"].unique()
    rows = []
    for aid in ids:
        a = (artists_api or {}).get(aid) or {}
        genres = a.get("genres") or []
        cat_items = (catalog or {}).get(aid) or []
        dates = [_parse_release(c["date"], c["precision"]) for c in cat_items]
        dates = [d for d in dates if pd.notna(d)]
        rows.append({
            "artist_id": aid, "artist_name": a.get("name") or names.get(aid, aid),
            "genres": ", ".join(genres), "genre_group": genre_group(genres),
            "image_url": a.get("image_url", ""),
            "spotify_url": a.get("spotify_url") or (f"https://open.spotify.com/artist/{aid}" if not aid.startswith("x") else ""),
            "first_release_date": min(dates) if dates else pd.NaT,
            "releases_before_year": sum(d < cfg.year_start for d in dates) if dates else np.nan,
            "api_popularity_now": a.get("popularity"),
            "api_followers_now": a.get("followers"),
        })
    df = pd.DataFrame(rows)
    df["career_years_at_breakout"] = ((cfg.year_start - df["first_release_date"]).dt.days / 365.25).round(1)
    best = summ.sort_values("breakthrough_score", ascending=False).drop_duplicates("artist_id")
    df = df.merge(best[["artist_id", "category", "category_sort"]].rename(
        columns={"category": "best_category", "category_sort": "best_category_sort"}), on="artist_id", how="left")
    return df.dropna(axis=1, how="all")


def build_releases(tracks, summ, catalog, cfg):
    cand = set(summ.loc[summ.is_new_to_chart, "artist_id"])
    rows = []
    for aid, items in (catalog or {}).items():
        if aid not in cand:
            continue
        for c in items:
            d = _parse_release(c["date"], c["precision"])
            if pd.notna(d) and pd.Timestamp(cfg.analysis_year - 1, 7, 1) <= d <= cfg.year_end:
                rows.append({"release_id": c["id"], "artist_id": aid, "release_name": c["name"],
                             "release_type": c["type"], "release_date": d, "total_tracks": c["total_tracks"]})
    rel = pd.DataFrame(rows)
    if rel.empty:  # fall back to albums of charting tracks
        t = tracks.dropna(subset=["album_id"])
        t = t[t.primary_artist_id.isin(cand)]
        rel = t.drop_duplicates("album_id").rename(columns={
            "album_id": "release_id", "primary_artist_id": "artist_id", "album_name": "release_name",
            "album_type": "release_type"})[["release_id", "artist_id", "release_name", "release_type", "release_date"]]
    if rel.empty:
        return rel
    rel = rel.drop_duplicates(["release_id", "artist_id"])
    charted_albums = set(tracks["album_id"].dropna())
    rel["has_charting_track"] = rel["release_id"].isin(charted_albums)
    # trigger = latest release on/before breakout within 8 weeks
    bo = summ.dropna(subset=["breakout_week"]).groupby("artist_id")["breakout_week"].min()
    rel["is_breakout_trigger"] = False
    for aid, b in bo.items():
        m = (rel.artist_id == aid) & (rel.release_date <= b) & (rel.release_date >= b - pd.Timedelta(days=56))
        if m.any():
            rel.loc[rel[m].release_date.idxmax(), "is_breakout_trigger"] = True
    return rel.sort_values("release_date")


def build_date_table(start, cfg):
    d = pd.DataFrame({"date": pd.date_range(start, cfg.year_end, freq="D")})
    d["year"] = d["date"].dt.year
    d["quarter"] = "Q" + d["date"].dt.quarter.astype(str)
    d["month_num"] = d["date"].dt.month
    d["month_name"] = d["date"].dt.strftime("%b")
    d["year_month"] = d["date"].dt.strftime("%Y-%m")
    d["iso_week"] = d["date"].dt.isocalendar().week.astype(int)
    d["is_analysis_year"] = d["year"] == cfg.analysis_year
    return d


# =============================================================================
# 4) EXPORT
# =============================================================================
def readme_rows(cfg, info):
    return [
        ("Spotify Niche-to-Mainstream Breakthroughs", ""),
        ("Analysis year", str(cfg.analysis_year)),
        ("Popularity source", "Spotify Charts weekly Top 200 (charts.spotify.com): real ranks + streams"),
        ("Metadata source", info["meta"]),
        ("Regions", info["regions"]),
        ("Chart weeks loaded", info["weeks"]),
        ("Generated", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("", ""),
        ("TABLES", ""),
        ("ArtistSummary", "One row per artist per region: category, best rank, breakout week, score."),
        ("Artists", "One row per artist: name, genres, photo, career info."),
        ("WeeklyTrend", "One row per artist per region per chart week (streams summed over their charting tracks)."),
        ("ChartEntries", "Raw chart rows: region, week, rank, track, streams."),
        ("Tracks", "One row per charting track with credits, album and release date."),
        ("Releases", "Releases around the year for new-to-chart artists; flags the breakout trigger."),
        ("Regions / DateTable", "Slicer dimensions. Mark DateTable as date table."),
        ("", ""),
        ("RELATIONSHIPS", ""),
        ("Artists[artist_id] 1 -> * ArtistSummary / WeeklyTrend / Releases [artist_id]", "single direction"),
        ("Regions[region] 1 -> * ArtistSummary / WeeklyTrend / ChartEntries [region]", "single direction"),
        ("Tracks[track_id] 1 -> * ChartEntries[track_id]", "single direction"),
        ("DateTable[date] 1 -> * WeeklyTrend[chart_week] and ChartEntries[chart_week]", "single direction"),
        ("", ""),
        ("DEFINITIONS (per region)", ""),
        ("Niche / new-to-chart", f"Not on that region's Top 200 at any point in the loaded weeks before {cfg.analysis_year}"),
        ("Mass popularity", f"Reached rank {cfg.mass_rank} or better"),
        ("Breakthrough", f"New-to-chart, reached top {cfg.mass_rank}, charted >= {cfg.min_weeks} weeks in "
                         f"{cfg.analysis_year}, and stayed on chart >= {cfg.sustain_min:.0%} of weeks after breakout"),
        ("Flash Breakout", f"New-to-chart, reached top {cfg.mass_rank}, but didn't stick"),
        ("Rising", f"New-to-chart, >= {cfg.rising_weeks} weeks on chart, never top {cfg.mass_rank}"),
        ("Chart Entry", f"New-to-chart, fewer than {cfg.rising_weeks} weeks, never top {cfg.mass_rank}"),
        ("Returning", "Not in loaded baseline, but a track already had chart history before the window"),
        ("Established", "Charted before the analysis year (not ranked)"),
        ("breakthrough_score", "0-100: 30% best rank, 20% total streams (log, within region), "
                               "25% sustain, 10% speed to peak, 15% number of charting tracks"),
        ("Credits", "Featured artists are credited with the full streams of tracks they appear on."),
        ("", ""),
        ("SUGGESTED DAX MEASURES", ""),
        ("Breakthrough Artists", 'CALCULATE(DISTINCTCOUNT(ArtistSummary[artist_id]), ArtistSummary[category] = "Breakthrough")'),
        ("New-to-Chart Artists", "CALCULATE(DISTINCTCOUNT(ArtistSummary[artist_id]), ArtistSummary[is_new_to_chart] = TRUE())"),
        ("Breakthrough Rate", "DIVIDE([Breakthrough Artists], [New-to-Chart Artists])"),
        ("Total Streams", "SUM(WeeklyTrend[streams])"),
        ("Best Rank", "MIN(WeeklyTrend[best_rank])"),
        ("Median Weeks to Top 50", "MEDIANX(FILTER(ArtistSummary, NOT ISBLANK(ArtistSummary[weeks_to_breakout])), ArtistSummary[weeks_to_breakout])"),
        ("", ""),
        ("TIPS", ""),
        ("Rank charts", "Line chart of Best Rank: Format > Y axis > Invert range ON so #1 is at the top."),
        ("Category colours", "Sort category by category_sort; hex codes are in category_color."),
        ("Artist photos", "Artists[image_url] -> Column tools > Data category = Image URL."),
        ("Aligned trajectories", "X = WeeklyTrend[weeks_since_breakout], Y = Total Streams, Legend = artist_name."),
    ]


def style_and_save(sheets: dict, readme, path: Path):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.table import Table, TableStyleInfo

    wb = Workbook()
    ws = wb.active
    ws.title = "README"
    for r, (k, v) in enumerate(readme, 1):
        ws.cell(r, 1, k).font = Font(name="Arial", size=10, bold=bool(k and not v))
        c = ws.cell(r, 2, v)
        c.font = Font(name="Arial", size=10)
        c.alignment = Alignment(wrap_text=True, vertical="top")
    ws["A1"].font = Font(name="Arial", size=14, bold=True)
    ws.column_dimensions["A"].width = 60
    ws.column_dimensions["B"].width = 110

    hdr_font = Font(name="Arial", size=10, bold=True, color="FFFFFF")
    hdr_fill = PatternFill("solid", start_color="1A1D24")
    body = Font(name="Arial", size=10)
    for name, df in sheets.items():
        if df is None or df.empty:
            continue
        ws = wb.create_sheet(name)
        cols = list(df.columns)
        ws.append(cols)
        for row in df.itertuples(index=False):
            out = []
            for v in row:
                if v is None or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
                    out.append(None)
                elif isinstance(v, pd.Timestamp):
                    out.append(v.to_pydatetime().date())
                elif isinstance(v, np.integer):
                    out.append(int(v))
                elif isinstance(v, np.floating):
                    out.append(None if np.isnan(v) else float(v))
                elif isinstance(v, np.bool_):
                    out.append(bool(v))
                else:
                    out.append(v)
            ws.append(out)
        for j, c in enumerate(cols, 1):
            fmt = None
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                fmt = "yyyy-mm-dd"
            elif c in ("sustain_ratio", "chart_presence"):
                fmt = "0.0%"
            elif "streams" in c or "followers" in c:
                fmt = "#,##0"
            h = ws.cell(1, j)
            h.font, h.fill = hdr_font, hdr_fill
            for i in range(2, len(df) + 2):
                cell = ws.cell(i, j)
                cell.font = body
                if fmt:
                    cell.number_format = fmt
            width = max([len(str(c))] + [len(str(x)) for x in df[c].head(200)]) + 2
            ws.column_dimensions[get_column_letter(j)].width = min(max(width, 10), 45)
        t = Table(displayName=name, ref=f"A1:{get_column_letter(len(cols))}{len(df) + 1}")
        t.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
        ws.add_table(t)
        ws.freeze_panes = "B2"
    wb.save(path)


def write_theme(path: Path):
    theme = {
        "name": "Breakthrough Night",
        "dataColors": ["#3DF5A0", "#FF4FD8", "#FFB347", "#B388FF", "#7A8199",
                       "#4FA3FF", "#00E5FF", "#FF6B6B", "#C6FF00", "#F5F5F5"],
        "background": "#121419", "foreground": "#EDEFF5", "tableAccent": "#3DF5A0",
        "good": "#3DF5A0", "neutral": "#FFB347", "bad": "#FF4F6D",
        "maximum": "#3DF5A0", "center": "#4FA3FF", "minimum": "#1E2230",
        "textClasses": {
            "callout": {"fontSize": 30, "fontFace": "Segoe UI Semibold", "color": "#FFFFFF"},
            "title": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": "#EDEFF5"},
            "header": {"fontSize": 11, "fontFace": "Segoe UI Semibold", "color": "#EDEFF5"},
            "label": {"fontSize": 10, "fontFace": "Segoe UI", "color": "#A8AEC0"},
        },
        "visualStyles": {
            "*": {"*": {"background": [{"show": True, "color": {"solid": {"color": "#1A1D24"}}, "transparency": 0}],
                        "border": [{"show": False}], "dropShadow": [{"show": False}]}},
            "page": {"*": {"background": [{"color": {"solid": {"color": "#0D0F13"}}, "transparency": 0}],
                           "outspace": [{"color": {"solid": {"color": "#0D0F13"}}}]}},
        },
    }
    path.write_text(json.dumps(theme, indent=2), encoding="utf-8")


# =============================================================================
# SAMPLE CHART GENERATOR (fake files for testing the pipeline only)
# =============================================================================
def make_sample_charts(folder: str, seed=7):
    rng = np.random.default_rng(seed)
    out = Path(folder)
    out.mkdir(parents=True, exist_ok=True)
    weeks = pd.date_range("2024-01-04", "2025-12-25", freq="7D")
    abc = list("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
    artists = [f"Sample Artist {i:03d}" for i in range(260)]
    songs = []
    for i in range(520):
        a = artists[rng.integers(len(artists))]
        if rng.random() < 0.12:
            a += f", {artists[rng.integers(len(artists))]}"
        debut = rng.integers(-30, len(weeks))
        peak = 10 ** rng.uniform(5.6, 7.6)
        songs.append(("".join(rng.choice(abc, 22)), f"Sample Song {i:03d}", a, debut, peak,
                      rng.uniform(4, 30)))
    for region, scale in (("global", 1.0), ("au", 0.04)):
        history = {}
        for wi, w in enumerate(weeks):
            rows = []
            for tid, name, a, debut, peak, life in songs:
                age = wi - debut
                if age < 0:
                    continue
                s = peak * scale * np.exp(-age / life) * min(1, (age + 1) / 2) * rng.lognormal(0, 0.08)
                rows.append((s, tid, name, a))
            rows.sort(reverse=True)
            recs = []
            for r, (s, tid, name, a) in enumerate(rows[:200], 1):
                h = history.setdefault(tid, {"weeks": 0, "peak": 999, "prev": None})
                h["weeks"] += 1
                h["peak"] = min(h["peak"], r)
                recs.append({"rank": r, "uri": f"spotify:track:{tid}", "artist_names": a, "track_name": name,
                             "source": "Sample", "peak_rank": h["peak"], "previous_rank": h["prev"] or -1,
                             "weeks_on_chart": h["weeks"], "streams": int(s)})
            on = {x["uri"].split(":")[-1]: x["rank"] for x in recs}
            for tid, h in history.items():
                h["prev"] = on.get(tid)
            pd.DataFrame(recs).to_csv(out / f"regional-{region}-weekly-{w.date()}.csv", index=False)
    print(f"Wrote {len(weeks) * 2} FAKE chart files to {out}/ (for testing only)")


# =============================================================================
# MAIN
# =============================================================================
HELP_CHARTS = """
Downloading Spotify weekly charts
---------------------------------
1. Go to https://charts.spotify.com and log in with your (free or Premium) Spotify account.
2. Choose: Top Songs > Weekly > Global (and/or Australia, etc.).
3. Pick a week with the date selector and click the download (CSV) icon.
4. Repeat for every week from early 2024 through the last week of 2025
   (2024 is the baseline that tells "niche" from "already big").
5. Put all files in one folder without renaming them,
   e.g. charts/regional-global-weekly-2025-01-02.csv
"""


def parse_args():
    p = argparse.ArgumentParser(description="Find niche artists who broke through in Spotify charts.")
    p.add_argument("--charts", default="charts", help="Folder of weekly chart CSVs")
    p.add_argument("--region", default="global", help="Region code if file names don't contain one")
    p.add_argument("--year", type=int, default=2025)
    p.add_argument("--mass-rank", type=int, default=50)
    p.add_argument("--min-weeks", type=int, default=8)
    p.add_argument("--sustain", type=float, default=0.60)
    p.add_argument("--out", default=None)
    p.add_argument("--csv-dir", default=None, help="Also write every table as CSV")
    p.add_argument("--no-api", action="store_true", help="Skip Spotify API enrichment")
    p.add_argument("--auth", choices=["pkce", "client"], default="pkce",
                   help="pkce = log in once in browser (recommended); client = client credentials")
    p.add_argument("--redirect-uri", default="http://127.0.0.1:8888/callback")
    p.add_argument("--cache-dir", default="api_cache")
    p.add_argument("--no-catalog", action="store_true", help="Skip per-artist album lookups")
    p.add_argument("--check-api", action="store_true", help="Test which fields your app receives, then exit")
    p.add_argument("--make-sample-charts", metavar="DIR", help="Write FAKE chart files for testing, then exit")
    p.add_argument("--help-charts", action="store_true", help="How to download the chart CSVs")
    return p.parse_args()


def main():
    args = parse_args()
    if args.help_charts:
        print(HELP_CHARTS)
        return
    if args.make_sample_charts:
        make_sample_charts(args.make_sample_charts)
        return
    cfg = Config(analysis_year=args.year, mass_rank=args.mass_rank,
                 min_weeks=args.min_weeks, sustain_min=args.sustain)

    api = None
    if not args.no_api:
        cid = os.getenv("SPOTIFY_CLIENT_ID")
        if not cid:
            sys.exit("Set SPOTIFY_CLIENT_ID (and SPOTIFY_CLIENT_SECRET for --auth client), or use --no-api.")
        api = SpotifyAPI(cid, os.getenv("SPOTIFY_CLIENT_SECRET"), auth=args.auth, redirect_uri=args.redirect_uri)
        if args.check_api:
            check_api(api)
            return

    print("Loading charts ...")
    charts = load_charts(args.charts, args.region)

    tracks_api = artists_api = catalog = None
    meta_source = "None (--no-api): artist credits split from chart text"
    if api:
        cache = Path(args.cache_dir)
        tracks_api, artists_api, catalog, cpath = enrich_with_api(api, charts, cache, not args.no_catalog)
        meta_source = "Spotify Web API (tracks, artists" + ("" if args.no_catalog else ", artist albums") + ")"

    bridge, tracks = build_bridge(charts, tracks_api)
    print("Analysing ...")
    summ, weekly = analyze(charts, bridge, tracks, cfg)

    if api and not args.no_catalog:
        cand = summ.loc[summ.is_new_to_chart, "artist_id"].unique()
        catalog = fetch_artist_catalogs(api, [c for c in cand if not c.startswith("x")], catalog, cpath)

    artists = build_artists(bridge, summ, artists_api, catalog, cfg)
    releases = build_releases(tracks, summ, catalog, cfg)
    chart_entries = charts[["region", "chart_week", "rank", "track_id", "streams",
                            "weeks_on_chart", "peak_rank", "previous_rank"]].dropna(axis=1, how="all")
    tracks = tracks[tracks.track_id.isin(chart_entries.track_id)].dropna(axis=1, how="all")
    regions = pd.DataFrame({"region": sorted(charts.region.unique())})
    regions["region_name"] = regions.region.map(lambda r: REGION_NAMES.get(r, r.upper()))
    dates = build_date_table(charts.chart_week.min(), cfg)

    out = Path(args.out or f"spotify_breakthrough_{cfg.analysis_year}.xlsx")
    sheets = {"ArtistSummary": summ.drop(columns=["top_track_id"]), "Artists": artists,
              "WeeklyTrend": weekly, "ChartEntries": chart_entries, "Tracks": tracks,
              "Releases": releases, "Regions": regions, "DateTable": dates}
    info = {"meta": meta_source, "regions": ", ".join(regions.region_name),
            "weeks": f"{charts.chart_week.nunique()} ({charts.chart_week.min().date()} to {charts.chart_week.max().date()})"}
    style_and_save(sheets, readme_rows(cfg, info), out)
    write_theme(out.with_name("powerbi_theme_breakthrough.json"))
    if args.csv_dir:
        d = Path(args.csv_dir)
        d.mkdir(parents=True, exist_ok=True)
        for n, df in sheets.items():
            if df is not None and not df.empty:
                df.to_csv(d / f"{n}.csv", index=False)

    print(f"\nWrote {out}" + (f"  (API calls this run: {api.calls})" if api else ""))
    name = artists.set_index("artist_id")["artist_name"]
    for reg, g in summ.groupby("region"):
        print(f"\n{REGION_NAMES.get(reg, reg)}:")
        print("  " + "  ".join(f"{k}: {v}" for k, v in g.category.value_counts().reindex(CATEGORY_ORDER).fillna(0).astype(int).items()))
        top = g[g.category == "Breakthrough"].nsmallest(5, "breakthrough_rank")
        for _, r in top.iterrows():
            print(f"  #{int(r.breakthrough_rank):<3}{name.get(r.artist_id, r.artist_id):<28} best #{r.best_rank:<4}"
                  f"score {r.breakthrough_score:<6} breakout {r.breakout_month}  ({r.top_track})")


if __name__ == "__main__":
    main()
