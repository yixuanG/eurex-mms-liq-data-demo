#!/usr/bin/env python3
import argparse
import os
import shutil
import sys
import tarfile
import time
from pathlib import Path
from typing import Iterable, List, Set, Tuple


def format_size(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for u in units:
        if size < 1024.0:
            return f"{size:.2f} {u}"
        size /= 1024.0
    return f"{size:.2f} PB"


def disk_usage(path: str) -> Tuple[str, str, str]:
    total, used, free = shutil.disk_usage(path)
    return format_size(total), format_size(used), format_size(free)


def is_within_directory(directory: str, target: str) -> bool:
    directory = os.path.abspath(directory)
    target = os.path.abspath(target)
    try:
        return os.path.commonpath([directory]) == os.path.commonpath([directory, target])
    except Exception:
        return False


def safe_extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, dest: str) -> None:
    member_path = os.path.join(dest, member.name)
    if not is_within_directory(dest, member_path):
        raise RuntimeError(f"Unsafe path detected in tar member: {member.name}")
    if member.isdir():
        Path(member_path).mkdir(parents=True, exist_ok=True)
        return
    parent = os.path.dirname(member_path)
    Path(parent).mkdir(parents=True, exist_ok=True)
    src = tar.extractfile(member)
    if src is not None:
        with src, open(member_path, "wb") as out:
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


def member_matches_segments(name: str, segments: Set[str]) -> bool:
    n = name.strip("/")
    if not n:
        return False
    parts = n.split("/")
    # Match directory itself (..../48) or any file whose parent directory equals the segment
    if len(parts) >= 1 and parts[-1] in segments and name.endswith("/"):
        return True
    if len(parts) >= 2 and parts[-2] in segments:
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract only selected MarketSegmentID folders from a Eurex .tar.gz into Google Drive (persistent)")
    ap.add_argument("--tar", required=True, help="Path to source .tar.gz (e.g., /content/drive/MyDrive/00_EUREX/sample_data/Sample_Eurex_20201201_10MktSegID.tar.gz)")
    ap.add_argument("--dest", required=True, help="Destination directory on Google Drive (e.g., /content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_raw)")
    ap.add_argument("--segments", nargs="+", required=True, help="Segment IDs to extract (e.g., 48 50)")
    ap.add_argument("--progress-every", type=int, default=20, help="Print progress every N members")
    ap.add_argument("--list-top", type=int, default=20, help="List top-N files by size (set 0 to skip)")
    ap.add_argument("--show-tree", action="store_true", help="Print directory tree after extraction")

    args = ap.parse_args()

    tar_path = args.tar
    dest_dir = args.dest.rstrip("/")
    segs = {str(s) for s in args.segments}

    if not os.path.exists(tar_path):
        print(f"[ERROR] Tar file not found: {tar_path}", file=sys.stderr)
        return 1

    Path(dest_dir).mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Destination (Drive) usage:")
    print("=" * 80)
    total, used, free = disk_usage(dest_dir)
    print(f"Total: {total}  |  Used: {used}  |  Free: {free}")

    tar_size = os.path.getsize(tar_path)
    print(f"\nTar size: {format_size(tar_size)}")

    start = time.time()
    extracted_count = 0
    scanned_count = 0

    print(f"\n[INFO] Extracting segments {sorted(segs)}")
    print(f"[INFO] From: {tar_path}")
    print(f"[INFO] To  : {dest_dir}")

    with tarfile.open(tar_path, mode="r:gz") as tar:
        members = tar.getmembers()
        total_members = len(members)
        for i, m in enumerate(members, 1):
            scanned_count += 1
            if member_matches_segments(m.name, segs):
                safe_extract_member(tar, m, dest_dir)
                extracted_count += 1
            if args.progress_every > 0 and (i % args.progress_every == 0 or i == total_members):
                elapsed = time.time() - start
                pct = i / total_members * 100
                print(f"\rProgress: {i}/{total_members} ({pct:.1f}%)  |  Extracted: {extracted_count}  |  Elapsed: {elapsed:.1f}s  ", end="")
        print()

    print("=" * 80)
    print("[OK] Extraction completed")
    print(f"Elapsed: {time.time() - start:.1f} s  |  Members scanned: {scanned_count}  |  Extracted: {extracted_count}")

    files = list_files(dest_dir)
    if files:
        total_sz = sum(sz for _, sz in files)
        print(f"\nExtracted files (under dest): {len(files)}  |  Total size: {format_size(total_sz)}")
        files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
        if args.list_top > 0:
            print("\n" + "=" * 80)
            print(f"Top {args.list_top} files by size:")
            print("=" * 80)
            for i, (p, sz) in enumerate(files_sorted[: args.list_top], 1):
                rel = os.path.relpath(p, dest_dir)
                print(f"{i:3d}. {rel:70s} {format_size(sz):>10s}")
        if args.show_tree:
            print_tree(dest_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
