# Power BI Dashboard Quick Start Guide

## Files in This Folder

- `QUICKSTART.md` - This file
- `dax_measures.txt` - All DAX formulas to copy-paste
- `dashboard_layout.json` - Dashboard structure reference
- `connection_guide.md` - Step-by-step connection instructions

## 5-Minute Setup

### Step 0: Transfer Files to Windows

**From Mac to Windows (via USB/Network):**

1. On Mac, copy these folders:
   ```
   export_powerbi/          ← Essential CSV files
   warehouse/eurex.duckdb   ← Optional (for future ODBC use)
   ```

2. On Windows, paste to:
   ```
   C:\EUREX\export_powerbi\     ← Put CSV files here
   C:\EUREX\warehouse\          ← Optional: Put .duckdb here
   ```

**Essential Files (Must copy):**
- `export_powerbi/kyle_lambda_segment.csv`
- `export_powerbi/kyle_lambda_security.csv` 
- `export_powerbi/amihud_illiquidity.csv`
- `export_powerbi/spread_decomposition.csv`
- `export_powerbi/segment_summary.csv`
- `export_powerbi/security_summary.csv`

**Optional File (For advanced ODBC method):**
- `warehouse/eurex.duckdb` (~82MB)
  - Only copy if you plan to use DuckDB ODBC connection
  - CSV method does NOT need this file

### Step 1: Open Power BI Desktop
Download from: https://powerbi.microsoft.com/desktop/

### Step 2: Load CSV Files
1. Get Data → Text/CSV
2. Navigate to `C:\EUREX\export_powerbi\`
3. Load each CSV file:
   - `kyle_lambda_segment.csv`
   - `kyle_lambda_security.csv` 
   - `amihud_illiquidity.csv`
   - `spread_decomposition.csv`
   - `segment_summary.csv`
   - `security_summary.csv`
4. Click "Load" for each file

### Step 3: Create Relationships
In Model view:
```
segment_summary (1) → (*) kyle_lambda_segment
    └─ segment_id ─────────► segment_id

segment_summary (1) → (*) spread_decomposition
    └─ segment_id ─────────► segment_id

segment_summary (1) → (*) amihud_illiquidity
    └─ segment_id ─────────► segment_id
```

### Step 4: Add Measures
1. Click on any table
2. Modeling → New Measure
3. Copy-paste from `dax_measures.txt`

### Step 5: Build Visuals
Follow the layouts in `dashboard_layout.json`

## Dashboard Pages

### Page 1: Executive Summary
- 3 card visuals: Segments count, Avg Lambda, Avg Spread
- 1 bar chart: Liquidity ranking
- 1 table: Segment metrics

### Page 2: Kyle's Lambda Analysis
- 1 bar chart: Lambda by segment (horizontal)
- 1 scatter plot: Lambda vs Volume
- 1 line chart: Lambda trend (if time data available)

### Page 3: Spread Decomposition
- 1 stacked bar: Components by segment
- 1 pie chart: Average composition
- 1 matrix: Segment × Component

### Page 4: Amihud Illiquidity
- 1 heatmap: Illiquidity by segment
- 1 histogram: Distribution
- 1 table: Detailed metrics

## Quick Tips

**Color Scheme**
- Green (#28a745): Good liquidity (lambda < 0.0005)
- Yellow (#ffc107): Fair liquidity (0.0005 - 0.001)
- Red (#dc3545): Poor liquidity (> 0.001)

**Filters**
Add these as page-level filters:
- Segment ID (multi-select)
- Lambda range (slider)
- Date range (if available)

**Performance**
- Use DirectQuery only for live data
- Import mode is faster for pre-computed tables
- Limit visuals to 3-5 per page

## Troubleshooting

**Issue: Tables not loading**
- Check CSV file paths
- Ensure files are not open in Excel

**Issue: Relationships not working**
- Verify segment_id data types match
- Check for NULL values

**Issue: Measures showing errors**
- Ensure table names match exactly
- Check column names are correct

## Next Steps

1. Save as `eurex_liquidity.pbix`
2. Publish to Power BI Service (if available)
3. Set up scheduled refresh
4. Share with stakeholders

---

**Need Help?**
- Check `dax_measures.txt` for all formulas
- See `connection_guide.md` for detailed setup
- Refer to `dashboard_layout.json` for visual specs
