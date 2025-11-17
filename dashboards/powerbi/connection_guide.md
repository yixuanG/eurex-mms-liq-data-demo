# Power BI Connection Guide

## Prerequisites

✅ Power BI Desktop installed on Windows
✅ CSV files exported from notebook (on Mac)
✅ USB drive or network for file transfer

**Note:** You do NOT need Google Drive on Windows

---

## Method 1: CSV Files (Recommended)

### Step 1: Transfer Files from Mac to Windows

**On Mac:**
1. Navigate to project folder in Google Drive
2. Copy the entire `export_powerbi/` folder
3. Transfer via:
   - USB drive
   - Network file share
   - WeChat/Email (if files are small)
   - Cloud service (Dropbox, OneDrive, etc.)

**On Windows:**
1. Create folder: `C:\EUREX\`
2. Paste `export_powerbi\` folder inside
3. Final path: `C:\EUREX\export_powerbi\`

**Verify files exist:**
```
C:\EUREX\export_powerbi\kyle_lambda_segment.csv
C:\EUREX\export_powerbi\spread_decomposition.csv
C:\EUREX\export_powerbi\amihud_illiquidity.csv
C:\EUREX\export_powerbi\segment_summary.csv
C:\EUREX\export_powerbi\security_summary.csv
C:\EUREX\export_powerbi\kyle_lambda_security.csv
```

### Step 2: Import in Power BI

**For Each Table:**

1. Open Power BI Desktop
2. Home → Get Data → Text/CSV
3. Navigate to CSV location
4. Select file (e.g., `kyle_lambda_segment.csv`)
5. Click "Load" (not "Transform")

**Files to Load:**
- ✅ `kyle_lambda_segment.csv`
- ✅ `kyle_lambda_security.csv`
- ✅ `amihud_illiquidity.csv`
- ✅ `spread_decomposition.csv`
- ✅ `segment_summary.csv`
- ✅ `security_summary.csv`

### Step 3: Verify Data Types

1. Transform Data
2. For each numeric column, set type to "Decimal Number"
3. For segment_id, set type to "Whole Number"
4. Close & Apply

### Step 4: Create Relationships

1. Go to Model view (left sidebar)
2. Drag from `segment_summary[segment_id]` to `kyle_lambda_segment[segment_id]`
3. Set cardinality: One to Many (1:*)
4. Repeat for other tables:
   - `segment_summary` → `spread_decomposition`
   - `segment_summary` → `amihud_illiquidity`
   - `security_summary` → `kyle_lambda_security`

---

## Method 2: DuckDB Direct Connection (Optional)

**Only use this if:** You want live connection and don't mind extra setup

### Step 1: Copy Database File

**On Mac:**
1. Locate: `warehouse/eurex.duckdb` (~82MB)
2. Copy to USB drive or network share

**On Windows:**
1. Create folder: `C:\EUREX\warehouse\`
2. Paste `eurex.duckdb` inside
3. Final path: `C:\EUREX\warehouse\eurex.duckdb`

### Step 2: Install DuckDB ODBC Driver

1. Download from: https://github.com/duckdb/duckdb/releases/latest
   - Look for: `duckdb_odbc-windows-amd64.zip`
2. Extract zip file
3. Run installer: `duckdb_odbc_setup.exe`
4. Follow installation wizard
5. Restart computer if prompted

### Step 3: Configure ODBC Data Source

1. Press `Win + R`
2. Type `odbcad32.exe`
3. Press Enter
4. Go to "System DSN" tab
5. Click "Add"
6. Select "DuckDB Driver"
7. Configure:
   ```
   Data Source Name: EUREX_Warehouse
   Database: C:\EUREX\warehouse\eurex.duckdb
   Description: EUREX Liquidity Analysis
   ```
8. Click "Test Connection"
9. Should see: "Connection successful"
10. Click "OK" to save

### Step 4: Connect from Power BI

1. Open Power BI Desktop
2. Home → Get Data → More
3. Search "ODBC"
4. Select "ODBC" → Connect
5. Data source name (DSN): Select "EUREX_Warehouse"
6. Click "OK"
7. Select tables to load:
   - ✅ kyle_lambda_segment
   - ✅ spread_decomposition
   - ✅ amihud_illiquidity
   - ✅ segment_summary
8. Click "Load"

---

## Method 3: Python Script in Power BI

If you have Python installed:

1. Get Data → Python script
2. Paste this:

```python
import duckdb
import pandas as pd

# Connect to database
conn = duckdb.connect(r'C:\EUREX\warehouse\eurex.duckdb')

# Load tables
kyle_lambda_segment = conn.execute("SELECT * FROM kyle_lambda_segment").df()
spread_decomposition = conn.execute("SELECT * FROM spread_decomposition").df()
amihud_illiquidity = conn.execute("SELECT * FROM amihud_illiquidity").df()
segment_summary = conn.execute("SELECT * FROM segment_summary").df()

conn.close()
```

3. Click "OK"
4. Select which dataframes to load

---

## Refresh Strategy

### CSV Method (Your Setup)

**When to Refresh:**
- After running notebook on Mac
- After new data export

**How to Refresh:**
1. **On Mac:**
   - Run notebook: `3_advanced_model_prep.ipynb`
   - CSV files updated in `export_powerbi/`
   
2. **Transfer to Windows:**
   - Copy updated CSV files via USB/network
   - Overwrite files in `C:\EUREX\export_powerbi\`
   
3. **In Power BI:**
   - Open your `.pbix` file
   - Home → Refresh
   - Or: Right-click each table → Refresh

**Tips:**
- Use `Ctrl+A` to select all CSVs for batch copy
- Date stamp your .pbix files: `eurex_2024-11-16.pbix`
- Keep last 3 versions as backup

### DuckDB Method (Optional/Advanced)

**When to Refresh:**
- After running notebook on Mac
- After copying new `eurex.duckdb` to Windows

**How to Refresh:**
1. Copy updated `eurex.duckdb` to Windows
2. In Power BI: Home → Refresh
3. Data updates automatically

### Scheduled Refresh (Power BI Service)

1. Publish report to Power BI Service
2. Go to dataset settings
3. Set refresh schedule:
   - Frequency: Daily
   - Time: After notebook run (e.g., 8 AM)
4. Configure gateway if using DuckDB

---

## Troubleshooting

### CSV Import Issues

**Problem:** "File not found"
```
Solution:
1. Verify path is correct
2. Check file isn't open in Excel
3. Try copying file to C:\EUREX\export_powerbi\
```

**Problem:** "Column type mismatch"
```
Solution:
1. Open Transform Data
2. Detect data types automatically
3. Manually set segment_id to Whole Number
4. Set metric columns to Decimal Number
```

### ODBC Connection Issues

**Problem:** "Driver not found"
```
Solution:
1. Reinstall DuckDB ODBC driver
2. Use 64-bit version (match Power BI)
3. Restart Power BI Desktop
```

**Problem:** "Cannot connect to database"
```
Solution:
1. Check database file path
2. Ensure file isn't locked
3. Verify Google Drive sync is complete
4. Try absolute path: C:\Users\[User]\Google Drive\...
```

**Problem:** "DSN not found"
```
Solution:
1. Use System DSN (not User DSN)
2. Run odbcad32.exe as Administrator
3. Recreate the DSN
```

### Relationship Issues

**Problem:** "Relationships not creating"
```
Solution:
1. Check both columns have same data type
2. Ensure no NULL values in key columns
3. Verify column names match exactly
4. Use "Manage Relationships" to create manually
```

### Performance Issues

**Problem:** "Slow loading"
```
Solution:
1. Use Import mode (not DirectQuery)
2. Filter unnecessary columns in Transform Data
3. Aggregate at segment level
4. Close unused applications
```

---

## Data Update Workflow

### Full Refresh (Weekly/Monthly)

```
[Mac]
1. Run notebook: 3_advanced_model_prep.ipynb
2. CSVs auto-exported to: export_powerbi/
3. (Optional) eurex.duckdb updated in: warehouse/

[Transfer]
4. Copy export_powerbi/ folder to USB/network
5. Transfer to Windows

[Windows]  
6. Paste CSVs to C:\EUREX\export_powerbi\ (overwrite)
7. Open Power BI → Refresh
8. Verify all visuals updated
9. Save new version: eurex_2024-11-16.pbix
```

### Incremental Update (if supported)

Configure in Power BI Service:
1. Set up incremental refresh policy
2. Define RangeStart and RangeEnd parameters
3. Configure partition by date (if applicable)

---

## Best Practices

### Connection Method Selection

| Method | Speed | Ease | Auto-Refresh |
|--------|-------|------|--------------|
| CSV    | Fast  | Easy | Manual       |
| ODBC   | Medium| Medium| Yes         |
| Python | Slow  | Hard | Yes          |

**Recommendation:** Start with CSV, move to ODBC if you need auto-refresh

### File Organization

```
Windows Structure:
C:\EUREX\
├── warehouse\
│   └── eurex.duckdb
├── export_powerbi\
│   ├── kyle_lambda_segment.csv
│   ├── spread_decomposition.csv
│   └── ...
└── dashboards\
    └── eurex_liquidity.pbix
```

### Version Control

1. Save .pbix file with date: `eurex_liquidity_2024-11-16.pbix`
2. Keep last 3 versions
3. Document major changes

---

## Quick Reference

### File Locations

| What | Mac Path | Windows Path (After Transfer) |
|------|----------|-------------------------------|
| DuckDB | `warehouse/eurex.duckdb` | `C:\EUREX\warehouse\eurex.duckdb` |
| CSVs | `export_powerbi/*.csv` | `C:\EUREX\export_powerbi\*.csv` |
| Dashboard | - | `C:\EUREX\dashboards\eurex_liquidity.pbix` |

**Transfer Methods:**
- USB drive
- Network share
- WeChat file transfer
- Email (for small files)
- Cloud storage (OneDrive, Dropbox)

### Key Steps Checklist

- [ ] CSV files exported on Mac
- [ ] Files transferred to Windows
- [ ] Files in C:\EUREX\export_powerbi\
- [ ] Power BI Desktop installed
- [ ] Data loaded from CSVs
- [ ] Relationships created
- [ ] DAX measures added
- [ ] Visuals built
- [ ] Report saved as .pbix

---

## Support

**Issues with:**
- CSV import → Check `QUICKSTART.md`
- DAX formulas → Check `dax_measures.txt`
- Visual layout → Check `dashboard_layout.json`
- Connection → Thisfile

**Still stuck?**
1. Check Power BI Community forums
2. Verify Python environment (if using Python script)
3. Test with smaller CSV files first
