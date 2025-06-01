#!/usr/bin/env python
# ──────────────────────────────────────────────────────────────
#  sync.py  ·  K‑Pop Bangers audio→video helper  ·  v0.3‑patched
# ──────────────────────────────────────────────────────────────
import os, sys, argparse, subprocess, textwrap
import plistlib
from pathlib import Path
from plexapi.server import PlexServer, BadRequest
from prompt_toolkit import prompt
from rich import box
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# ── configuration ─────────────────────────────────────────────
load_dotenv()                                   # read .env

CFG = {
    "soobin": {"url": os.getenv("SOOBIN_URL"),  "token": os.getenv("SOOBIN_TOKEN")},
    "picard": {"url": os.getenv("PICARD_URL"),  "token": os.getenv("PICARD_TOKEN")},
}

AUDIO_PLAYLIST = int(os.getenv("AUDIO_PLAYLIST"))
VIDEO_PLAYLIST = int(os.getenv("VIDEO_PLAYLIST"))
VIDEO_SECTION  = int(os.getenv("VIDEO_SECTION"))
DOWNLOAD_DIR   = Path(os.getenv("DOWNLOAD_DIR", "."))
YTDLP_BIN      = os.getenv("YTDLP", "yt-dlp")

# ── CLI args ─────────────────────────────────────────────────
parser = argparse.ArgumentParser(
    description="Diff Apple‑Music playlist against Plex and optionally "
                "download missing MVs.")
parser.add_argument("xml", help="Apple‑Music XML export path")
parser.add_argument("--remote", choices=["soobin", "picard"],
                    default="soobin", metavar="SERVER",
                    help="Which Plex box to talk to (default soobin)")
parser.add_argument("--report", action="store_true",
                    help="Diff only – no URL prompts, no downloads")
args = parser.parse_args()

# ── helpers ──────────────────────────────────────────────────
def connect(server_key: str) -> PlexServer:
    cfg = CFG[server_key]
    print(f"🔌  Connecting to {server_key.capitalize()} … ", end="")
    try:
        plex = PlexServer(cfg["url"], cfg["token"])
        print("OK")
        return plex
    except Exception as e:
        print(f"FAILED  ({e})")
        sys.exit(1)


def plex_key(itm):
    """Return `(key,label)` for a Plex item.

    * **key**   → "Artist‖Title"  (matching hash ‖ is safe in names)
    * **label** → "Artist – Title" (pretty‑print)
    Handles albums, tracks, *and* music‑video clips while stripping the
    annoying `bound method Track.artist` artefact we saw earlier.
    """
    title = getattr(itm, "title", "<Untitled>")

    # candidate attributes (first non‑empty wins)
    cand = (
        getattr(itm, "grandparentTitle", None),   # most music libs
        getattr(itm, "artist", None),             # some Track objects expose .artist()
        getattr(itm, "parentTitle", None),        # odd balls
    )

    artist = None
    for a in cand:
        if not a:
            continue
        if callable(a):          # bound method → call it to get Artist obj / str
            try:
                a = a()
                a = a.title if hasattr(a, "title") else str(a)
            except Exception:
                continue
        if a and a != "Various Artists":
            artist = a
            break

    if not artist:
        artist = "<Unknown>"

    return f"{artist}‖{title}", f"{artist} – {title}"


# ── Apple‑Music export loader ────────────────────────────────

def load_xml(path: Path):
    """Return Apple‑Music playlist as an ordered list of dicts."""
    print(f"📥  Parsing Apple XML  ({path})")
    with path.open("rb") as fp:
        data = plistlib.load(fp)

    # Track catalogue with **int** keys for speed
    tracks_dict = {int(k): v for k, v in data["Tracks"].items()}

    # Exported playlist is always the first one in the file
    id_order = [d["Track ID"] for d in data["Playlists"][0]["Playlist Items"]]

    ordered = []
    for idx, tid in enumerate(id_order, 1):
        t       = tracks_dict.get(tid, {})
        artist  = t.get("Artist", "<Unknown>")
        title   = t.get("Name",   "<Untitled>")
        ordered.append({
            "idx": idx,
            "artist": artist,
            "title":  title,
            "key":    f"{artist}‖{title}",
        })

    print(f"    → {len(ordered)} tracks\n")
    return ordered


# ── diff / ledger builder ───────────────────────────────────

def ledger(source, plex_items):
    """Pretty diff between Apple export and Plex playlist."""
    src_lookup = {t["key"]: t["idx"] for t in source}
    seen   = set()     # keys already matched
    rows   = []        # table rows
    stats  = dict(add=0, remove=0, up=0, down=0)

    # pass 1 – walk Plex order
    for p_idx, itm in enumerate(plex_items, 1):
        key, title = plex_key(itm)

        if key in src_lookup:            # same song exists in Apple list
            s_idx   = src_lookup[key]
            offset  = p_idx - s_idx
            if offset == 0:
                status = "·"
            elif offset > 0:
                status = f"↑{offset}"
                stats["up"] += 1
            else:
                status = f"↓{abs(offset)}"
                stats["down"] += 1
            rows.append((s_idx, p_idx, status, title))
            seen.add(key)
        else:                            # present in Plex but not in Apple
            rows.append(("–", p_idx, "–", title))
            stats["remove"] += 1

    # pass 2 – anything missing in Plex
    for t in source:
        if t["key"] not in seen:
            rows.append((t["idx"], "–", "+", f"{t['artist']} – {t['title']}"))
            stats["add"] += 1

    # sort by Apple order for readability
    rows.sort(key=lambda r: (r[0] if r[0] != "–" else 1e9, r[1]))

    # pretty‑print
    tbl = Table(box=box.SIMPLE_HEAVY)
    tbl.add_column("#",   justify="right")
    tbl.add_column("Src", justify="right")
    tbl.add_column("Plex",justify="right")
    tbl.add_column("±",   justify="center")
    tbl.add_column("Track")
    for a, b, stat, title in rows:
        tbl.add_row(str(a), str(b), stat, title)
    Console().print(tbl)

    return stats


# ── YouTube‑DL helper ───────────────────────────────────────

def download(url, artist, title):
    dest_tpl = DOWNLOAD_DIR / f"{artist} - {title}.%(ext)s"
    cmd = [YTDLP_BIN, "-f", "bestvideo[ext=mp4]+bestaudio/best",
           "-o", str(dest_tpl), url]
    return subprocess.call(cmd, shell=False) == 0


# ──────────── main ───────────────────────────────────────────
PLEX = connect(args.remote)

print("🎶  Fetching playlists …")
try:
    audio_pl = PLEX.fetchItem(AUDIO_PLAYLIST)
    video_pl = PLEX.fetchItem(VIDEO_PLAYLIST)
except BadRequest:
    print("✗  Invalid playlist IDs – check .env")
    sys.exit(1)
print(f"    Audio : {audio_pl.title}  ({len(audio_pl.items())} tracks)")
print(f"    Video : {video_pl.title}  ({len(video_pl.items())} items)\n")

apple = load_xml(Path(args.xml))

print("🔍  Building ledger …\n")
stats = ledger(apple, audio_pl.items())
print(f"\nSummary:  +{stats['add']}  –{stats['remove']}  "
      f"↑{stats['up']}  ↓{stats['down']}\n")

if args.report:
    print("📄  Report mode – nothing else to do.\n")
    sys.exit()

# ── interactive missing‑video loop ──────────────────────────
video_keys   = {plex_key(v)[0] for v in video_pl.items()}
missing_keys = {t["key"] for t in apple if t["key"] not in video_keys}

for t in apple:
    if t["key"] not in missing_keys:
        continue

    banner = textwrap.fill(f"{t['artist']} – {t['title']}",
                           width=78, subsequent_indent="  ")
    print(f"\n🎬  {banner}")
    url = prompt("  Paste YouTube URL (blank = skip | all = skip-all) › ").strip()
    if url.lower() == "all":
        print("⏩  Skipping the rest.")
        break
    if not url:
        continue
    print("      ↳ yt-dl …")
    if download(url, t["artist"], t["title"]):
        print("      ✓  saved → Plex scanner will ingest soon.")
    else:
        print("      ✗  download failed.")

print("\n✅  Finished. Plex playlists were still read‑only in this run.")