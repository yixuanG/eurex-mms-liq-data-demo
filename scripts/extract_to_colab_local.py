#!/usr/bin/env python3
"""
Extract a large Eurex tar.gz archive from Google Drive to Colab local storage
and summarize the extracted file structure.

Usage (in Colab):
  python scripts/extract_to_colab_local.py \
    --tar "/content/drive/MyDrive/00_EUREX/Sample_Eurex_20201201_10MktSegID.tar.gz" \
    --dest "/content/Sample_Eurex_20201201_10MktSegID" \
    --progress-every 10 \
    --show-tree \
    --list-top 20

Notes:
- This script is optimized for Colab: extracting to /content (local SSD) is faster and does not consume Drive quota.
- It prints disk usage, safe-extracts tar members (no path traversal), and emits a compact summary.
- For very large archives, prefer listing only top-N to keep output readable.
"""
import argparse
import os
import shutil
import sys
import tarfile
import time
from pathlib import Path
from typing import Iterable, List, Tuple


def format_size(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for u in units:
        if size < 1024.0:
            return f"{size:.2f} {u}"
        size /= 1024.0
    return f"{size:.2f} PB"


def disk_usage(path: str = "/content/") -> Tuple[str, str, str]:
    total, used, free = shutil.disk_usage(path)
    return format_size(total), format_size(used), format_size(free)


def is_within_directory(directory: str, target: str) -> bool:
    directory = os.path.abspath(directory)
    target = os.path.abspath(target)
    return os.path.commonpath([directory]) == os.path.commonpath([directory, target])


def safe_extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, dest: str) -> None:
    # Prevent path traversal (e.g., "../" in member names)
    member_path = os.path.join(dest, member.name)
    if not is_within_directory(dest, member_path):
        raise RuntimeError(f"Unsafe path detected in tar member: {member.name}")
    # Create parent directories as needed then extract the file object
    if member.isdir():
        Path(member_path).mkdir(parents=True, exist_ok=True)
        return
    parent = os.path.dirname(member_path)
    Path(parent).mkdir(parents=True, exist_ok=True)
    with tar.extractfile(member) as src, open(member_path, "wb") as out:
        if src is not None:
            shutil.copyfileobj(src, out)


def list_files(root: str) -> List[Tuple[str, int]]:
    files: List[Tuple[str, int]] = []
    for r, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(r, f)
            try:
                sz = os.path.getsize(p)
            except OSError:
                sz = 0
            files.append((p, sz))
    return files


def print_tree(root: str) -> None:
    print("\n=== Directory Tree ===")
    for r, dirs, files in os.walk(root):
        level = r.replace(root, "").count(os.sep)
        indent = "  " * level
        folder = os.path.basename(r) if level > 0 else root
        print(f"{indent}📁 {folder}/")
        for f in sorted(files):
            p = os.path.join(r, f)
            try:
                sz = os.path.getsize(p)
            except OSError:
                sz = 0
            print(f"{indent}  📄 {f:50s} ({format_size(sz):>9s})")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract Eurex tar.gz to Colab local and summarize files")
    parser.add_argument("--tar", required=True, help="Path to source .tar.gz on Google Drive (e.g., /content/drive/MyDrive/...) ")
    parser.add_argument("--dest", required=True, help="Destination folder on Colab local (e.g., /content/...) ")
    parser.add_argument("--progress-every", type=int, default=10, help="Print progress every N files")
    parser.add_argument("--list-top", type=int, default=20, help="List top-N files by size (set 0 to skip)")
    parser.add_argument("--list-all", action="store_true", help="List all files by size (overrides --list-top)")
    parser.add_argument("--show-tree", action="store_true", help="Print directory tree after extraction")

    args = parser.parse_args()

    tar_path = args.tar
    dest_dir = args.dest.rstrip("/")

    if not os.path.exists(tar_path):
        print(f"[ERROR] Tar file not found: {tar_path}", file=sys.stderr)
        return 1

    print("=" * 80)
    print("Disk usage on Colab local (/content/):")
    print("=" * 80)
    total, used, free = disk_usage("/content/")
    print(f"Total: {total}  |  Used: {used}  |  Free: {free}")

    tar_size = os.path.getsize(tar_path)
    est_need = tar_size * 4  # conservative estimate: 3-5x
    print(f"\nTar size: {format_size(tar_size)}  |  Estimated need: {format_size(est_need)}")

    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    start = time.time()

    print(f"\n[INFO] Extracting: {tar_path}")
    print(f"[INFO] Destination: {dest_dir}")

    with tarfile.open(tar_path, mode="r:gz") as tar:
        members = tar.getmembers()
        total_members = len(members)
        print(f"Members: {total_members}")
        print("=" * 80)
        for i, m in enumerate(members, 1):
            safe_extract_member(tar, m, dest_dir)
            if args.progress_every > 0 and (i % args.progress_every == 0 or i == total_members):
                elapsed = time.time() - start
                pct = i / total_members * 100
                print(f"\rProgress: {i}/{total_members} ({pct:.1f}%)  |  Elapsed: {elapsed:.1f}s  |  {m.name[:60]}", end="")
        print()
    print("=" * 80)
    print("[OK] Extraction completed")
    print(f"Elapsed: {time.time() - start:.1f} s")

    # Summaries
    files = list_files(dest_dir)
    total_sz = sum(sz for _, sz in files)
    print(f"\nExtracted files: {len(files)}  |  Total size: {format_size(total_sz)}")

    # Group by extension
    exts = {}
    for p, sz in files:
        ext = os.path.splitext(p)[1].lower()
        exts.setdefault(ext, {"count": 0, "size": 0})
        exts[ext]["count"] += 1
        exts[ext]["size"] += sz
    print("\nFile types:")
    for ext, info in sorted(exts.items(), key=lambda kv: kv[1]["size"], reverse=True):
        name = ext if ext else "(no ext)"
        print(f"  {name:8s}: {info['count']:4d} files, {format_size(info['size']):>10s}")

    # Lists by size
    files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
    if args.list_all:
        top = files_sorted
        title = "All files by size"
    elif args.list_top > 0:
        top = files_sorted[: args.list_top]
        title = f"Top {args.list_top} files by size"
    else:
        top = []
        title = None

    if title:
        print("\n" + "=" * 80)
        print(title + ":")
        print("=" * 80)
        for i, (p, sz) in enumerate(top, 1):
            rel = os.path.relpath(p, dest_dir)
            print(f"{i:3d}. {rel:70s} {format_size(sz):>10s}")

    if args.show_tree:
        print_tree(dest_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
