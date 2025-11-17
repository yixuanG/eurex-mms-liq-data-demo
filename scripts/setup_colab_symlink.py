#!/usr/bin/env python3
"""
Create a symlink in the Drive repo pointing to the Colab local raw data folder.

This makes it easy to navigate and check the raw data structure from the Drive
repo directory, even though the actual data lives in Colab local SSD.

Usage (Colab):
  python scripts/setup_colab_symlink.py \
    --raw-local "/content/Sample_Eurex_20201201_10MktSegID" \
    --repo-drive "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo" \
    --link-name "data_raw_colab"
"""
import argparse
import os
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="Create symlink to Colab raw data in Drive repo")
    ap.add_argument("--raw-local", required=True, help="Path to raw data in Colab local SSD")
    ap.add_argument("--repo-drive", required=True, help="Path to repo in Google Drive")
    ap.add_argument("--link-name", default="data_raw_colab", help="Name of the symlink (default: data_raw_colab)")
    args = ap.parse_args()
    
    raw_local = Path(args.raw_local)
    repo_drive = Path(args.repo_drive)
    link_path = repo_drive / args.link_name
    
    if not raw_local.exists():
        print(f"[ERROR] Raw data path does not exist: {raw_local}")
        return 1
    
    if not repo_drive.exists():
        print(f"[ERROR] Repo path does not exist: {repo_drive}")
        return 1
    
    # Remove existing symlink if present
    if link_path.exists() or link_path.is_symlink():
        if link_path.is_symlink():
            print(f"[INFO] Removing existing symlink: {link_path}")
            link_path.unlink()
        else:
            print(f"[WARN] {link_path} exists but is not a symlink, skipping")
            return 1
    
    # Create symlink
    try:
        link_path.symlink_to(raw_local, target_is_directory=True)
        print(f"[OK] ✅ Created symlink:")
        print(f"  Link: {link_path}")
        print(f"  Target: {raw_local}")
        
        # Verify
        if link_path.is_symlink() and link_path.exists():
            print(f"[OK] ✅ Symlink verified and working")
            
            # List a few items to confirm
            try:
                items = list(link_path.iterdir())[:5]
                print(f"\n[INFO] First few items in raw data folder:")
                for item in items:
                    print(f"  - {item.name}")
            except Exception as e:
                print(f"[WARN] Could not list items: {e}")
        else:
            print(f"[ERROR] Symlink creation may have failed")
            return 1
            
    except Exception as e:
        print(f"[ERROR] Failed to create symlink: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
