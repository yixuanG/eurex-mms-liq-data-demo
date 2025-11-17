# Power BI Dashboard Package

## 📦 What's in This Folder

| File | Purpose | When to Use |
|------|---------|-------------|
| `QUICKSTART.md` | **Start here!** | First-time setup |
| `connection_guide.md` | Detailed connection steps | If you have connection issues |
| `dax_measures.txt` | All DAX formulas | Copy-paste into Power BI |
| `dashboard_layout.json` | Visual specifications | Reference for building dashboards |
| `PYTHON_VISUAL_GUIDE.md` | **Python visuals setup** | Order Flow charts |
| `python_order_flow_plotly.py` | Plotly interactive chart | For dynamic visualizations |
| `python_order_flow_heatmap.py` | Matplotlib heatmap | For traditional order flow |
| `README.md` | This file | Overview |

---

## 🚀 Quick Start (10 Minutes)

### You Need:
- ✅ Windows PC
- ✅ Power BI Desktop (free download)
- ✅ USB drive or network for file transfer
- ✅ CSV files exported from Mac

**Note:** No need to install Google Drive on Windows

### Steps:
1. **Transfer** CSV files from Mac to Windows (USB/network)
2. **Read** `QUICKSTART.md` for detailed steps
3. **Load** CSV files into Power BI from `C:\EUREX\export_powerbi\`
4. **Copy** DAX measures from `dax_measures.txt`
5. **Build** visuals using `dashboard_layout.json` as reference
6. **Save** as `.pbix` file

---

## 📊 Dashboard Preview

### Page 1: Executive Summary
```
┌────────────┬────────────┬────────────┐
│ Total Segs │ Avg Lambda │  Status    │
│     5      │  0.3606    │  🟡 Fair   │
└────────────┴────────────┴────────────┘

┌────────────────────────────────────────┐
│  Liquidity Ranking by Segment         │
│  ===================================   │
│  48  ████                              │
│  702 ████                              │
│  821 ████████                          │
│  688 ███████████████████████           │
│  589 ████████████████████████████████  │
└────────────────────────────────────────┘
```

### Page 2: Kyle's Lambda
- Price impact analysis
- R-squared quality metrics
- Liquidity score gauge

### Page 3: Spread Decomposition
- Adverse selection vs transient costs
- Component breakdown
- Quality indicators

### Page 4: Amihud Illiquidity
- Cross-segment heatmap
- Distribution analysis
- Statistical summaries

---

## 🎨 Dashboard Features

### Interactive Elements
- **Slicers**: Filter by segment, risk level
- **Drill-through**: Click segment → see securities
- **Tooltips**: Hover for detailed metrics
- **Cross-filtering**: Click visual → others update

### Key Metrics
- Kyle's Lambda (price impact)
- Spread components (adverse selection/transient)
- Amihud illiquidity
- Liquidity rankings
- Quality scores

### Color Coding
- 🟢 Green: Good liquidity (lambda < 0.0005)
- 🟡 Yellow: Fair liquidity (0.0005 - 0.001)
- 🔴 Red: Poor liquidity (> 0.001)

---

## 📁 File Structure

```
powerbi/
├── README.md                      ← You are here
├── QUICKSTART.md                  ← Start here
├── FILE_TRANSFER_CHECKLIST.md    ← Mac→Windows transfer
├── connection_guide.md            ← Detailed setup
├── dax_measures.txt               ← All formulas
└── dashboard_layout.json          ← Visual specs
```

---

## 🔄 Data Flow

```
[Mac/Colab]
    ↓
Run notebook → Updates DuckDB → Exports CSVs to export_powerbi/
    ↓
[Manual Transfer]
USB drive / Network / WeChat
    ↓
[Windows] C:\EUREX\export_powerbi\
    ↓
Load in Power BI → Build Dashboard → Save as .pbix
```

**Update Workflow:**
1. Mac: Run notebook when data changes
2. Transfer: Copy CSVs to Windows (overwrite old files)
3. Windows: Open Power BI → Refresh → Save new version

---

## 💡 Tips & Tricks

### For Beginners
1. Start with CSV files (easiest)
2. Use templates from `dashboard_layout.json`
3. Copy DAX measures exactly as written
4. Test with small dataset first

### For Advanced Users
1. Use DuckDB ODBC for live connection
2. Create custom DAX measures
3. Add time intelligence (if date data available)
4. Publish to Power BI Service with gateway

### Performance Optimization
- Use Import mode (not DirectQuery)
- Aggregate at segment level when possible
- Limit visuals to 3-5 per page
- Use slicers instead of filters

---

## 📚 Learning Resources

### Power BI Basics
- Official docs: https://docs.microsoft.com/power-bi/
- DAX guide: https://dax.guide
- Community: https://community.powerbi.com

### EUREX Liquidity Models
- See: `ADVANCED_MODELS.md` in project root
- Kyle's Lambda paper: Kyle (1985)
- Amihud measure: Amihud (2002)
- Spread decomposition: Huang & Stoll (1997)

---

## 🛠 Customization

### Add New Metrics
1. Create measure in `dax_measures.txt`
2. Test in Power BI
3. Add to dashboard
4. Update documentation

### Modify Colors
Edit in `dashboard_layout.json`:
```json
"theme_colors": {
  "primary": "#YOUR_COLOR",
  "success": "#YOUR_COLOR",
  ...
}
```

### Change Layout
- Drag visuals in Power BI
- Resize as needed
- Save new template

---

## 🐛 Common Issues

### "Can't find CSV files"
→ Check `connection_guide.md` Section: CSV Import

### "DAX formula error"
→ Verify table and column names match exactly

### "Visuals not updating"
→ Check relationships in Model view

### "Performance is slow"
→ Reduce number of rows, use aggregations

---

## 📦 Deliverables

After setup, you should have:
- ✅ `eurex_liquidity.pbix` (Power BI file)
- ✅ 4 dashboard pages
- ✅ ~30 DAX measures
- ✅ ~15 visualizations
- ✅ Working data refresh

---

## 🔒 Data Security

### Sensitive Information
- Lambda values may reveal trading strategies
- Keep .pbix files secure
- Limit dashboard access to authorized users

### Sharing Options
1. Export to PDF (static)
2. Publish to Power BI Service (controlled access)
3. Export to PowerPoint (presentation)

---

## 📝 Maintenance

### Daily
- [ ] Check Google Drive sync
- [ ] Verify data freshness

### Weekly  
- [ ] Run notebook to update data
- [ ] Refresh Power BI dashboard
- [ ] Review metrics for anomalies

### Monthly
- [ ] Backup .pbix file
- [ ] Update documentation
- [ ] Review and optimize performance

---

## 🎯 Next Steps

1. **Today**: Load data, create basic visuals
2. **This Week**: Add all measures, complete 4 pages
3. **Next Week**: Publish to Power BI Service, set up refresh
4. **Ongoing**: Monitor, maintain, enhance

---

## 📞 Support

**Got questions?**
1. Check relevant .md file first
2. Search Power BI community
3. Review error messages carefully
4. Test with sample data

**File-specific help:**
- Setup issues → `QUICKSTART.md`
- Connection problems → `connection_guide.md`
- Formula errors → `dax_measures.txt`
- Layout questions → `dashboard_layout.json`

---

## ✨ Final Notes

This package gives you everything needed to build a professional liquidity analysis dashboard. While I can't create the actual `.pbix` file (Power BI proprietary format), these files provide:

- ✅ Complete DAX formulas
- ✅ Detailed visual specifications
- ✅ Step-by-step setup guide
- ✅ Troubleshooting help
- ✅ Best practices

**Estimated build time:** 30-60 minutes for experienced users, 2-3 hours for beginners.

**Good luck! 🚀**
