# Colab Raw Data Structure

This file documents the structure of the raw Eurex sample data extracted to Colab local SSD.

## Location

**Colab local path:** `/content/Sample_Eurex_20201201_10MktSegID/`

**Symlink in repo:** `{REPO_DIR}/data_raw_colab/` → `/content/Sample_Eurex_20201201_10MktSegID/`

## Dataset Overview

- **Date:** December 1, 2020
- **Source:** Eurex T7 high-frequency market data
- **Market Segments:** 10 segments with varied activity levels
- **Total Size:** ~14 GB (compressed: 2.57 GB)

## Directory Structure

```
/content/Sample_Eurex_20201201_10MktSegID/
├── 48/                          # Segment 48 - FSTK-ADSG (Futures on Adidas AG)
│   ├── DI_48_20201201.csv      # Depth Incremental (1.22 MB)
│   ├── DS_48_20201201.csv      # Depth Snapshot (990.78 KB)
│   ├── IS_48_20201201.csv      # Instrument Snapshot (4.43 KB)
│   ├── ISC_48_20201201.csv     # Instrument State Change (254 B)
│   ├── PSC_48_20201201.csv     # Product State Change (352 B)
│   └── MISC_48_20201201.csv    # Miscellaneous (257 B)
│
├── 50/                          # Segment 50 - OSTK-ADS (Options on Adidas AG)
│   ├── DI_50_20201201.csv      # Depth Incremental (1.08 GB)
│   ├── DS_50_20201201.csv      # Depth Snapshot (204.71 MB)
│   ├── IS_50_20201201.csv      # Instrument Snapshot (146.84 KB)
│   ├── ISC_50_20201201.csv     # Instrument State Change (321 B)
│   ├── PSC_50_20201201.csv     # Product State Change (363 B)
│   ├── MISC_50_20201201.csv    # Miscellaneous (423 B)
│   ├── II_50_20201201.csv      # Instrument Information (1.00 KB)
│   ├── CIU_50_20201201.csv     # Complex Instrument Update (639 B)
│   └── CR_50_20201201.csv      # Cross Request (199 B)
│
├── 589/                         # Segment 589 - High activity
│   ├── DI_589_20201201.csv     # Depth Incremental (221.08 MB)
│   ├── DS_589_20201201.csv     # Depth Snapshot (2.83 GB)
│   ├── IS_589_20201201.csv     # Instrument Snapshot
│   ├── PSC_589_20201201.csv    # Product State Change
│   └── MISC_589_20201201.csv   # Miscellaneous
│
├── 688/                         # Segment 688 - High activity
│   ├── DI_688_20201201.csv     # Depth Incremental (366.51 MB)
│   ├── DS_688_20201201.csv     # Depth Snapshot (2.06 GB)
│   ├── IS_688_20201201.csv     # Instrument Snapshot
│   ├── ISC_688_20201201.csv    # Instrument State Change
│   ├── PSC_688_20201201.csv    # Product State Change
│   └── MISC_688_20201201.csv   # Miscellaneous
│
├── 702/                         # Segment 702
│   ├── DI_702_20201201.csv     # Depth Incremental
│   ├── DS_702_20201201.csv     # Depth Snapshot
│   ├── IS_702_20201201.csv     # Instrument Snapshot
│   ├── PSC_702_20201201.csv    # Product State Change
│   └── MISC_702_20201201.csv   # Miscellaneous
│
├── 821/                         # Segment 821 - Medium activity
│   ├── DI_821_20201201.csv     # Depth Incremental (54.77 MB)
│   ├── DS_821_20201201.csv     # Depth Snapshot (183.36 MB)
│   ├── IS_821_20201201.csv     # Instrument Snapshot
│   ├── ISC_821_20201201.csv    # Instrument State Change
│   ├── PSC_821_20201201.csv    # Product State Change
│   └── MISC_821_20201201.csv   # Miscellaneous
│
├── 1176/                        # Segment 1176 - Very high activity (likely DAX futures)
│   ├── DI_1176_20201201.csv    # Depth Incremental (9.61 GB) ⚡ LARGEST
│   ├── DS_1176_20201201.csv    # Depth Snapshot (4.58 GB)
│   ├── IS_1176_20201201.csv    # Instrument Snapshot
│   ├── ISC_1176_20201201.csv   # Instrument State Change
│   ├── PSC_1176_20201201.csv   # Product State Change
│   ├── MISC_1176_20201201.csv  # Miscellaneous
│   ├── II_1176_20201201.csv    # Instrument Information
│   ├── CIU_1176_20201201.csv   # Complex Instrument Update
│   └── QR_1176_20201201.csv    # Quote Request
│
├── 1209/                        # Segment 1209
│   ├── DI_1209_20201201.csv    # Depth Incremental
│   ├── DS_1209_20201201.csv    # Depth Snapshot
│   ├── IS_1209_20201201.csv    # Instrument Snapshot
│   ├── PSC_1209_20201201.csv   # Product State Change
│   └── MISC_1209_20201201.csv  # Miscellaneous
│
├── 1373/                        # Segment 1373 - High activity
│   ├── DI_1373_20201201.csv    # Depth Incremental (921.57 MB)
│   ├── DS_1373_20201201.csv    # Depth Snapshot (1.11 GB)
│   ├── IS_1373_20201201.csv    # Instrument Snapshot
│   ├── ISC_1373_20201201.csv   # Instrument State Change
│   ├── PSC_1373_20201201.csv   # Product State Change
│   ├── MISC_1373_20201201.csv  # Miscellaneous
│   ├── II_1373_20201201.csv    # Instrument Information
│   ├── CIU_1373_20201201.csv   # Complex Instrument Update
│   └── QR_1373_20201201.csv    # Quote Request
│
├── 1374/                        # Segment 1374 - Low activity
│   ├── DI_1374_20201201.csv    # Depth Incremental (0.38 MB)
│   ├── DS_1374_20201201.csv    # Depth Snapshot (21.02 MB)
│   ├── IS_1374_20201201.csv    # Instrument Snapshot
│   ├── ISC_1374_20201201.csv   # Instrument State Change
│   ├── PSC_1374_20201201.csv   # Product State Change
│   ├── MISC_1374_20201201.csv  # Miscellaneous
│   ├── II_1374_20201201.csv    # Instrument Information
│   └── CIU_1374_20201201.csv   # Complex Instrument Update
│
├── DataSample.xlsx              # Data dictionary and sample overview
└── PS_20201201.csv              # Product Snapshot (cross-segment)
```

## File Types Explained

### Core Market Data Files

| File Type | Description | Update Frequency | Critical for |
|-----------|-------------|------------------|--------------|
| **DI** | Depth Incremental | Event-driven | Order book reconstruction |
| **DS** | Depth Snapshot | Periodic | Reconciliation, full book state |
| **IS** | Instrument Snapshot | Start of day | Instrument metadata |
| **PS** | Product Snapshot | Start of day | Product metadata |

### State Change Files

| File Type | Description | Purpose |
|-----------|-------------|---------|
| **ISC** | Instrument State Change | Trading halts, circuit breakers |
| **PSC** | Product State Change | Product-level state updates |
| **MISC** | Miscellaneous | Various system messages |

### Options/Complex Instruments (segments 50, 1176, 1373, 1374)

| File Type | Description | Purpose |
|-----------|-------------|---------|
| **II** | Instrument Information | Options-specific metadata |
| **CIU** | Complex Instrument Update | Complex instrument parameters |
| **CR** | Cross Request | Cross orders (block trades) |
| **QR** | Quote Request | Request for quote (RFQ) |

## Segment Characteristics

| Segment | Product Type | Activity | DI Size | DS Size | Depth Est. |
|---------|-------------|----------|---------|---------|------------|
| **48** | Futures (Adidas) | Low | 1.22 MB | 0.97 MB | L4-L5 |
| **50** | Options (Adidas) | High | 1.08 GB | 204 MB | L10-L15 |
| 589 | Unknown | High | 221 MB | 2.83 GB | L15+ |
| 688 | Unknown | High | 366 MB | 2.06 GB | L15+ |
| 702 | Unknown | Medium | Small | Small | L5-L10 |
| 821 | Unknown | Medium | 54.77 MB | 183 MB | L10+ |
| **1176** | Likely DAX Futures | Very High | 9.61 GB | 4.58 GB | L18-L20 |
| 1209 | Unknown | Low-Medium | Small | Small | L5-L10 |
| 1373 | Unknown | High | 921 MB | 1.11 GB | L15+ |
| 1374 | Unknown | Low | 0.38 MB | 21 MB | L5-L10 |

## File Format Details

### DI (Depth Incremental) Format
Nested CSV structure with multiple updates per line:
```
X,SeqNum,61,MktSegID,[{MDUpdateAction,MDEntryType,PriceLevel,SecurityID,Source,Price,Size,NumOrders,Side,Timestamp_ns},{...}]
```

### Parsing Notes

- **Timestamps:** Nanosecond precision (16-20 digit integers)
- **Nested structure:** Each line contains multiple `{...}` entries
- **MDUpdateAction:** 0=New, 1=Change, 2=Delete, 5=Overlay
- **MDEntryType:** 0=Bid, 1=Ask
- **PriceLevel:** 0=Best, 1=Second best, etc.

## Storage Considerations

### Why Colab Local SSD?

| Aspect | Colab Local | Google Drive |
|--------|-------------|--------------|
| **Size** | ~100 GB free | Limited quota |
| **Speed** | Very fast I/O | Slower, sync delays |
| **Persistence** | ❌ Session-only | ✅ Permanent |
| **Cost** | Free | Quota usage |

**Strategy:** Keep raw data in Colab local during processing, save only processed outputs to Drive.

### Expected Processing Times

| Segment | Processing | Output Size |
|---------|-----------|-------------|
| 48 | ~30 seconds | ~500 KB |
| 821 | ~5 minutes | ~10 MB |
| 589, 688 | ~20-25 min | ~100-150 MB |
| 1373 | ~40 minutes | ~80 MB |
| **1176** | **~2 hours** | **~800 MB** |

**Total for all 10 segments:** ~3-4 hours sequential, ~1.5-2 hours with `--parallel 2`

## Accessing the Data

### Via Symlink (after setup)
```python
# In Drive repo
import os
symlink = "/content/drive/MyDrive/00_EUREX/eurex-liquidity-demo/data_raw_colab"
segments = os.listdir(symlink)
print(segments)
```

### Direct Access
```python
# Direct path to Colab local
raw_local = "/content/Sample_Eurex_20201201_10MktSegID"
import glob
di_files = glob.glob(f"{raw_local}/**/DI_*.csv", recursive=True)
```

### List All Segments
```bash
# Shell command
ls -lh /content/Sample_Eurex_20201201_10MktSegID/
```

## Next Steps

1. ✅ Extract raw data to Colab local (completed)
2. ✅ Create symlink for navigation
3. 📊 **Check maximum depth per segment** → See notebook cell 13-15
4. 🚀 **Process segments** → `4_batch_process_all_segments.ipynb`
5. 💾 **Load to DuckDB** → `3_db_setup.ipynb`

## References

- Full pipeline guide: `PIPELINE_GUIDE.md`
- Data format details: `materials/eurofidai_sampledata_interpretation.md`
- Processing scripts: `scripts/process_*.py`

---

**Note:** This raw data is temporary and will be cleared when the Colab session ends. All processed outputs are automatically saved to Google Drive for persistence.
