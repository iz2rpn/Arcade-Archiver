#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import html.parser
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import List, Dict, Optional
import zipfile
import shutil

from urllib.parse import unquote


class ZipLinkParser(html.parser.HTMLParser):
    """Collect `.zip` links from a simple index page."""
    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag.lower() != "a":
            return
        href = dict(attrs).get("href")
        if href and href.lower().endswith(".zip"):
            self.links.append(href.strip())


def http_head(url: str) -> Dict[str, str]:
    """Send HTTP HEAD request. -> headers dictionary"""
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req) as resp:
        return dict(resp.headers)


def fetch_html(url: str) -> str:
    """Fetch HTML content. -> decoded HTML string"""
    with urllib.request.urlopen(url) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def list_zip_urls(base_url: str) -> List[str]:
    """Find and return .zip file URLs from the directory page."""
    html = fetch_html(base_url)
    parser = ZipLinkParser()
    parser.feed(html)
    urls: List[str] = []
    seen = set()
    for href in parser.links:
        full = urllib.parse.urljoin(base_url, href)
        if full not in seen:
            seen.add(full)
            urls.append(full)
    return urls


def format_size(n: int) -> str:
    """Format bytes to a human-readable string. -> string"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def download_with_resume(url: str, dest: Path, retries: int = 3, timeout: int = 30) -> None:
    """Download a file with resume support into `dest`."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    attempt = 0
    while True:
        attempt += 1
        try:
            existing = tmp.stat().st_size if tmp.exists() else 0
            total_size: Optional[int] = None
            try:
                head = http_head(url)
                if "Content-Length" in head:
                    total_size = int(head["Content-Length"])
            except Exception:
                pass

            if existing and total_size and existing >= total_size:
                tmp.replace(dest)
                return

            headers: Dict[str, str] = {}
            if existing:
                headers["Range"] = f"bytes={existing}-"
            req = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(req, timeout=timeout) as resp, open(tmp, "ab" if existing else "wb") as f:
                if total_size is None:
                    cl = resp.headers.get("Content-Length")
                    if cl:
                        total_size = existing + int(cl)
                downloaded = existing
                last_print = time.time()
                while True:
                    chunk = resp.read(262144)  # 256 KB
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if now - last_print >= 0.5:
                        if total_size:
                            pct = downloaded / total_size * 100
                            sys.stdout.write(
                                f"\r⇣ {dest.name}: {format_size(downloaded)}/{format_size(total_size)} ({pct:5.1f}%)"
                            )
                        else:
                            sys.stdout.write(f"\r⇣ {dest.name}: {format_size(downloaded)}")
                        sys.stdout.flush()
                        last_print = now

            if total_size and tmp.stat().st_size != total_size:
                raise IOError("Download incomplete (size mismatch)")
            tmp.replace(dest)
            sys.stdout.write(f"\r✓ {dest.name} downloaded.\n")
            return

        except Exception as e:
            if attempt >= retries:
                raise
            wait = 2 ** attempt
            print(f"\n[Warn] Error downloading {dest.name} ({e}), retrying in {wait}s...")
            time.sleep(wait)


def safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Safely extract a ZIP file into `extract_dir`."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            p = Path(member.filename)
            if p.is_absolute() or ".." in p.parts:
                raise RuntimeError(f"Suspicious path in ZIP: {member.filename}")
        zf.extractall(path=extract_dir)


def main() -> None:
    """CLI: download all `.zip` files from `--base-url` and extract into `--dest`."""
    ap = argparse.ArgumentParser(
        description="Download all .zip files from an archive.org directory and extract each into its own folder."
    )
    ap.add_argument("--base-url", required=True, help="Archive.org directory URL (e.g. https://archive.org/download/SomeCollection/ )")
    ap.add_argument("--dest", required=True, help="Destination folder for downloads and extractions")
    ap.add_argument("--skip-existing", action="store_true", help="Skip ZIP files that already exist locally")
    ap.add_argument("--force-reextract", action="store_true", help="Re-extract even if the target folder already exists")
    args = ap.parse_args()

    dest_root = Path(args.dest).resolve()
    dest_root.mkdir(parents=True, exist_ok=True)

    print(f"[+] Scanning index: {args.base_url}")
    try:
        zip_urls = list_zip_urls(args.base_url)
    except Exception as e:
        print(f"[ERROR] Failed to read directory: {e}")
        sys.exit(1)

    if not zip_urls:
        print("[!] No .zip files found.")
        sys.exit(0)

    print(f"[+] Found {len(zip_urls)} zip files.")

    for url in zip_urls:
        fname_enc = os.path.basename(urllib.parse.urlparse(url).path)
        fname = unquote(fname_enc)

        # Minimal cross-platform sanitization
        invalid = '<>:"\\|?*'
        safe_fname = "".join(ch for ch in fname if ch not in invalid).strip()
        if not safe_fname:
            print(f"[ERROR] Invalid filename after cleaning: {fname!r}")
            continue

        zip_path = dest_root / safe_fname

        if zip_path.exists() and args.skip_existing:
            print(f"- Skipped (already exists): {safe_fname}")
        else:
            try:
                download_with_resume(url, zip_path)
            except Exception as e:
                print(f"[ERROR] Download failed for {safe_fname}: {e}")
                continue

        extract_dir = dest_root / zip_path.stem
        if extract_dir.exists() and not args.force_reextract:
            print(f"- Already extracted: {extract_dir.name}")
            continue

        if extract_dir.exists() and args.force_reextract:
            print(f"- Cleaning folder: {extract_dir}")
            shutil.rmtree(extract_dir, ignore_errors=True)

        try:
            print(f"* Extracting: {safe_fname} -> {extract_dir.name}")
            safe_extract_zip(zip_path, extract_dir)
            print(f"✓ Extracted: {extract_dir}")
        except Exception as e:
            print(f"[ERROR] Extraction failed for {safe_fname}: {e}")


if __name__ == "__main__":
    main()

    # Example usage:
    # python3 grab_archive.py --base-url "https://archive.org/download/GameboyAdvanceRomCollectionByGhostware/" --dest ./downloads --skip-existing
    # change this line