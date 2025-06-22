#!/usr/bin/env python
"""
sync.py · K-Pop Bangers audio→video helper (v0.7.0)
───────────────────────────────────────────────────────────────────────────────
Features
========
* Debug mode to log raw vs. canonical keys for Apple & Plex items
* Audio-only / Video-only modes
* OVERRIDES mapping for manual corrections
* Fuzzy matching fallback (rapidfuzz)
* YouTube-DL workflow for missing clips
* Dry-run mode to preview adds/moves without applying them
* Report mode to only diff and exit without touching Plex
"""

import argparse, os, re, subprocess, sys, plistlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from dotenv import load_dotenv
from plexapi.server import PlexServer
from prompt_toolkit import prompt
from rich import box
from rich.console import Console
from rich.table import Table

# ── optional deps ───────────────────────────────────────────
try:
    from rapidfuzz import process  # type: ignore
except ImportError:
    process = None

try:
    from unidecode import unidecode  # type: ignore
except ImportError:
    unidecode = lambda s: s

# ── load .env ───────────────────────────────────────────────
load_dotenv()
CFG = {
    "soobin":  {"url": os.getenv("SOOBIN_URL"),  "token": os.getenv("SOOBIN_TOKEN")},
    "picard": {"url": os.getenv("PICARD_URL"),  "token": os.getenv("PICARD_TOKEN")},
}
AUDIO_PLAYLIST = int(os.getenv("AUDIO_PLAYLIST", "0"))
VIDEO_PLAYLIST = int(os.getenv("VIDEO_PLAYLIST", "0"))
MUSIC_SECTION   = int(os.getenv("MUSIC_SECTION",   "0"))
DOWNLOAD_DIR    = Path(os.getenv("DOWNLOAD_DIR", "."))
YTDLP_BIN       = os.getenv("YTDLP", "yt-dlp")

# ── manual override corrections ─────────────────────────────
OVERRIDES: Dict[str, str] = {
    "ji hoon": "jihoon",
    " jonas brothers": "",
    "g i dle": "i dle",
    "cant we just leave the monster alive": "cant we leave the monster alive",
}

# ── canonicalisation helpers ───────────────────────────────
_CANON_RE = re.compile(r"(\([^)]*\)|\[[^\]]*\]|[^0-9A-Za-z\s])", flags=re.UNICODE)
def canonical(raw: str) -> str:
    txt = unidecode(raw).lower()
    for bad, good in OVERRIDES.items():
        txt = txt.replace(bad, good)
    txt = _CANON_RE.sub(" ", txt)
    return re.sub(r"\s+", " ", txt).strip()

# ── variant-tag helper for videos ───────────────────────────
_VARIANT_RE = re.compile(
    r"(?P<tag>[\[\(].*?(?:mv|official|dance|live|perf|inkigayo)[^\]\)]*[\]\)])\s*$", re.I
)
def split_variant(title: str) -> Tuple[str, str]:
    m = _VARIANT_RE.search(title)
    if not m:
        return title.strip(), ""
    start, end = m.span("tag")
    return title[:start].rstrip(" -–:_"), title[start:end]

# ── Plex key functions ─────────────────────────────────────
def _safe_attr(itm: Any, *names: str) -> str:
    for n in names:
        v = getattr(itm, n, None)
        if callable(v):
            try: v = v()
            except: v = None
        if v:
            if not isinstance(v, str) and hasattr(v, "title"):
                v = str(v.title)
            return str(v)
    return ""

def plex_key_audio(itm) -> Tuple[str, str]:
    artist = _safe_attr(itm, "grandparentTitle","artist","parentTitle") or "<Unknown>"
    title  = getattr(itm, "title", "<Untitled>")
    canon  = canonical(f"{artist} {title}")
    label  = f"{artist} – {title}"
    return canon, label

_VIDEO_FALLBACK = re.compile(r"\s*(?P<artist>.+?)\s*[-_–]\s*(?P<title>.+)")
def plex_key_video(itm) -> Tuple[str, str]:
    artist = _safe_attr(itm, "grandparentTitle","parentTitle")
    raw    = getattr(itm, "title", "")
    if artist and raw:
        base, tag = split_variant(raw)
    else:
        try:
            stem = Path(itm.locations[0]).stem
        except:
            stem = raw
        m = _VIDEO_FALLBACK.match(stem)
        if m:
            artist, base = m.group("artist"), m.group("title")
        else:
            artist, base = "<Unknown>", stem
        base, tag = split_variant(base)
    canon = canonical(f"{artist} {base}")
    label = f"{artist} – {base}{(' ' + tag) if tag else ''}"
    return canon, label

# ── Apple XML loader ───────────────────────────────────────
def load_xml(path: Path) -> List[Dict[str, str]]:
    print(f"📥 Parsing Apple XML ({path})")
    data   = plistlib.load(path.open("rb"))
    tracks = {int(k): v for k, v in data["Tracks"].items()}
    order  = [d["Track ID"] for d in data["Playlists"][0]["Playlist Items"]]
    out: List[Dict[str, str]] = []
    for idx, tid in enumerate(order, 1):
        t = tracks.get(tid, {})
        a = t.get("Artist", "<Unknown>")
        n = t.get("Name",   "<Untitled>")
        out.append({
            "idx":   idx,
            "artist":a,
            "title": n,
            "canon": canonical(f"{a} {n}"),
            "label": f"{a} – {n}",
        })
    print(f" → {len(out)} tracks\n")
    return out

# ── diff & pretty-print ───────────────────────────────────
THRESH = 90
def ledger(src, items, title: str, key_fn, mode: str):
    src_map = {t["canon"]: t["idx"] for t in src}
    seen: Set[str] = set()
    rows, stats = [], dict(add=0, remove=0, up=0, down=0)
    dup = defaultdict(int)
    is_video = (key_fn is plex_key_video)

    if MODE.debug:
        Console().log(f"[bold cyan]Debug Apple→Plex ({title})[/]")
        for t in src[:MODE.debug_limit]:
            Console().log(f" APPLE raw={t['label']} → canon={t['canon']}")
        for itm in list(items)[:MODE.debug_limit]:
            k, l = key_fn(itm)
            Console().log(f" PLEX  raw={l} → canon={k}")

    for p_idx, itm in enumerate(items, 1):
        k, l = key_fn(itm)
        if is_video and k in src_map:
            dup[k] += 1
        if k in src_map:
            s = src_map[k]
            offs = p_idx - s
            stat = "·" if offs == 0 else ("↑"+str(abs(offs)) if offs>0 else "↓"+str(abs(offs)))
            rows.append((s, p_idx, stat, l, k))
            seen.add(k)
            if offs>0: stats["up"]+=1
            if offs<0: stats["down"]+=1
            continue

        if process:
            m = process.extractOne(k, src_map.keys())
            if m and m[1]>=THRESH and m[0] not in seen:
                s = src_map[m[0]]
                rows.append((s, p_idx, "≈", l, m[0]))
                seen.add(m[0])
                continue

        if mode!="audio-only":
            rows.append(("–", p_idx, "–", l, k))
            stats["remove"]+=1

    for t in src:
        if t["canon"] not in seen:
            rows.append((t["idx"], "–", "+", t["label"], t["canon"]))
            stats["add"]+=1

    if is_video:
        out, done = [], set()
        for s, p, st, lab, ck in rows:
            if ck in done: continue
            cnt = dup.get(ck,1)
            if cnt>1 and st!="+": lab+=f" (x{cnt} variants)"
            out.append((s,p,st,lab)); done.add(ck)
        rows_simple = out
    else:
        rows_simple = [(s,p,st,lab) for s,p,st,lab,_ in rows]

    rows_simple.sort(key=lambda r: (r[0] if r[0]!="–" else 1e9, r[1]))
    tbl = Table(title=title, box=box.SIMPLE_HEAVY)
    for c in ("#","Src","Plex","±","Track"): tbl.add_column(c, justify="right" if c!="Track" else "left")
    for i,(s,p,st,lab) in enumerate(rows_simple,1): tbl.add_row(str(i),str(s),str(p),st,lab)
    Console().print(tbl)
    return stats

# ── YouTube-DL helper ──────────────────────────────────────
def download(url: str, artist: str, title: str) -> bool:
    dest = DOWNLOAD_DIR / f"{artist} – {title}.%(ext)s"
    cmd  = [YTDLP_BIN, "-f", "bestvideo[ext=mp4]+bestaudio/best", "-o", str(dest), url]
    return subprocess.call(cmd)==0

# ── CLI & Modes ───────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("xml", help="Apple-Music XML export path")
parser.add_argument("--remote", choices=["soobin","picard"], default="soobin")
parser.add_argument("--report", action="store_true", help="diff only (no adds/moves)")
parser.add_argument("--audio-only", action="store_true")
parser.add_argument("--video-only", action="store_true")
parser.add_argument("--debug", action="store_true")
parser.add_argument("--debug-limit", type=int, default=10, help="items to log in debug")
parser.add_argument("--dry-run", action="store_true", help="Preview adds/moves without applying")
args = parser.parse_args()

class MODE: pass
MODE.audio_only  = args.audio_only
MODE.video_only  = args.video_only
MODE.debug       = args.debug
MODE.debug_limit = args.debug_limit
MODE.dry_run     = args.dry_run

# ── Main ───────────────────────────────────────────────────
def connect(key: str) -> PlexServer:
    cfg = CFG[key]
    print(f"🔌 Connecting to {key}… ", end="")
    try:
        p = PlexServer(cfg["url"], cfg["token"])
        print("OK")
        return p
    except Exception as e:
        print(f"FAILED ({e})")
        sys.exit(1)

PLEX      = connect(args.remote)
audio_pl  = PLEX.fetchItem(AUDIO_PLAYLIST)
video_pl  = PLEX.fetchItem(VIDEO_PLAYLIST)
print(f" Audio: {audio_pl.title} ({len(audio_pl.items())})")
print(f" Video: {video_pl.title} ({len(video_pl.items())})\n")

apple = load_xml(Path(args.xml))

if not MODE.video_only:
    audio_stats = ledger(apple, audio_pl.items(), "Audio diff", plex_key_audio, "audio-only")

    if not args.report:
        existing = {k for k,_ in (plex_key_audio(itm) for itm in audio_pl.items())}
        try:
            lib = PLEX.library.sectionByID(MUSIC_SECTION)
        except AttributeError:
            lib = next(s for s in PLEX.library.sections() if s.key == str(MUSIC_SECTION))
        track_map = {
            canonical(f"{_safe_attr(t, 'grandparentTitle','artist','parentTitle')} {t.title}"): t
            for t in lib.search(libtype='track')
        }

        for t in apple:
            if t["canon"] not in existing:
                itm = track_map.get(t["canon"])
                if itm:
                    if MODE.dry_run:
                        print(f"[DRY-RUN] ➕ Would add '{t['label']}'")
                    else:
                        audio_pl.add(itm)
                        print(f"➕ Added '{t['label']}'")
                else:
                    print(f"⚠️  No library match for '{t['label']}' (canon={t['canon']})")

        current_items = list(audio_pl.items())
        ordered = []
        for t in apple:
            for pi in current_items:
                if plex_key_audio(pi)[0] == t['canon']:
                    ordered.append(pi)
                    break

        for idx, pi in enumerate(ordered):
            if MODE.dry_run:
                print(f"[DRY-RUN] 🔀 Would move '{plex_key_audio(pi)[1]}' to pos {idx+1}")
            else:
                audio_pl.moveTrack(pi, idx)

        print("🔀 Audio playlist: preview complete." if MODE.dry_run else "🔀 Audio playlist: all missing tracks added & fully reordered.\n")

if not MODE.audio_only:
    video_stats = ledger(apple, video_pl.items(), "Video diff", plex_key_video, "")

if args.report:
    sys.exit(0)

if not MODE.audio_only:
    video_keys = {plex_key_video(v)[0] for v in video_pl.items()}
    missing = [t for t in apple if t['canon'] not in video_keys]
    for t in missing:
        print(f"\n🎬 {t['label']}")
        url = prompt(" Paste YouTube URL (blank=skip | all=quit) › ").strip()
        if url.lower() == "all": break
        if not url: continue
        print(" ↳ yt-dlp… ", end="")
        print("OK" if download(url, t['artist'], t['title']) else "FAIL")

print("\n✅ Done. Playlists left read-only.")
