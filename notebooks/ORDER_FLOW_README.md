# Order Flow Data Preparation - README

## 📖 概述

本notebook (`4_prepare_order_flow_for_powerbi.ipynb`) 从真实的EUREX市场数据中提取并转换Order Flow数据，用于Power BI可视化。

## 🎯 功能

1. **从DuckDB读取真实市场数据**
   - 连接到 `warehouse/eurex.duckdb`
   - 使用 `metrics_1s` 表（1秒聚合的orderbook数据）

2. **生成多时间粒度Order Flow数据**
   - 1s (1秒窗口)
   - 3s (3秒窗口)
   - 1m (1分钟窗口)
   - 5m (5分钟窗口)

3. **计算关键指标**
   - 每个价格层级的买单量 (bid_volume)
   - 每个价格层级的卖单量 (ask_volume)
   - 净成交量 (net_volume = bid - ask)
   - 交易次数 (trade_count)

4. **导出Power BI格式CSV**
   - 单个合并文件: `order_flow_data.csv`
   - 分时间窗口文件: `order_flow_1s.csv`, `order_flow_3s.csv`, 等

---

## 🚀 使用方法

### 前提条件

1. ✅ 已运行 `2_duckdb_warehouse_setup.ipynb` (DuckDB仓库已建立)
2. ✅ Python环境包含: `duckdb`, `pandas`, `numpy`, `matplotlib`
3. ✅ 有可用的市场数据（至少一个segment）

### 运行步骤

#### 方法1: Jupyter Notebook (推荐)

```bash
# 启动Jupyter
cd /path/to/eurex-liquidity-demo/notebooks
jupyter notebook

# 打开 4_prepare_order_flow_for_powerbi.ipynb
# 依次运行所有cell (Cell → Run All)
```

#### 方法2: VS Code

```bash
# 在VS Code中打开
code /path/to/eurex-liquidity-demo

# 打开 notebooks/4_prepare_order_flow_for_powerbi.ipynb
# 选择Python kernel
# Run All Cells
```

#### 方法3: 命令行 (headless)

```bash
jupyter nbconvert --to notebook --execute \
  4_prepare_order_flow_for_powerbi.ipynb \
  --output 4_prepare_order_flow_for_powerbi_executed.ipynb
```

---

## 📊 输出文件

### 主要输出 (export_powerbi/)

```
export_powerbi/
├── order_flow_data.csv         ← 所有时间窗口合并 [Power BI使用]
├── order_flow_1s.csv            ← 1秒窗口
├── order_flow_3s.csv            ← 3秒窗口
├── order_flow_1m.csv            ← 1分钟窗口
├── order_flow_5m.csv            ← 5分钟窗口
├── order_flow_preview.png       ← 预览图
└── order_flow_summary.txt       ← 摘要报告
```

### CSV文件结构

```csv
time_bin,price_level,bid_volume,ask_volume,net_volume,trade_count,avg_bid_price,avg_ask_price,time_window,segment_id
1606809700,270.5,150,120,30,5,270.45,270.55,1s,589
1606809700,271.0,200,180,20,8,270.95,271.05,1s,589
...
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `time_bin` | int | UNIX时间戳（秒），已按时间窗口聚合 |
| `price_level` | float | 价格层级（四舍五入到0.5） |
| `bid_volume` | int | 该时间窗口内该价格的累计买单量 |
| `ask_volume` | int | 该时间窗口内该价格的累计卖单量 |
| `net_volume` | int | 净成交量 (bid - ask) |
| `trade_count` | int | 更新次数 |
| `avg_bid_price` | float | 平均买价 |
| `avg_ask_price` | float | 平均卖价 |
| `time_window` | string | 时间窗口标识 (1s/3s/1m/5m) |
| `segment_id` | int | 数据segment ID |

---

## ⚙️ 配置说明

### 可调整的参数

在notebook的 "Generate Order Flow Data" cell中：

```python
# 配置参数
TIME_WINDOWS = {
    '1s': 1,      # 1秒窗口
    '3s': 3,      # 3秒窗口
    '1m': 60,     # 1分钟窗口
    '5m': 300     # 5分钟窗口
}

TARGET_SEGMENT = 589  # 选择segment (589最活跃)

# 价格tick大小
price_tick = 0.5      # 价格层级粒度

# 最大行数限制
max_rows = 20000      # 防止数据过大
```

### 选择不同的Segment

查看可用segments:
```python
segments = con.execute("SELECT DISTINCT segment_id FROM metrics_1s").df()
print(segments)
```

修改 `TARGET_SEGMENT`:
```python
TARGET_SEGMENT = 688  # 例如切换到segment 688
```

### 调整价格精度

```python
# 更粗粒度（更快，数据更少）
price_tick = 1.0  # 1元精度

# 更细粒度（更慢，数据更多）
price_tick = 0.1  # 0.1元精度
```

---

## 📈 数据量预估

基于Segment 589（最活跃）:

| 时间窗口 | 预估行数 | 文件大小 | 处理时间 |
|---------|---------|---------|---------|
| 1s | ~15,000 | ~1 MB | 5-10秒 |
| 3s | ~8,000 | ~0.5 MB | 3-5秒 |
| 1m | ~5,000 | ~0.3 MB | 2-3秒 |
| 5m | ~2,000 | ~0.15 MB | 1-2秒 |
| **合并** | **~30,000** | **~2 MB** | **10-20秒** |

*实际数据量取决于segment的活跃度和价格波动范围*

---

## 🔍 验证数据质量

### 检查点1: 数据完整性

```python
# 检查是否有缺失值
print(df.isnull().sum())

# 检查时间连续性
print(df['time_bin'].nunique(), "unique time bins")

# 检查价格范围
print(f"Price range: {df['price_level'].min():.2f} - {df['price_level'].max():.2f}")
```

### 检查点2: 成交量合理性

```python
# 总成交量应该为正
assert df['bid_volume'].sum() > 0
assert df['ask_volume'].sum() > 0

# 价格应该在合理范围
assert df['price_level'].min() > 0
assert df['price_level'].max() < 10000  # 根据实际市场调整
```

### 检查点3: 可视化检查

Notebook会自动生成预览图 (`order_flow_preview.png`)，检查：
- ✅ 热图有明显的颜色变化
- ✅ Bid和Ask有不同的模式
- ✅ Net Volume显示买卖压力差异

---

## 🐛 故障排除

### 问题1: "Database not found"

**原因:** DuckDB仓库未建立

**解决:**
```bash
# 先运行仓库设置notebook
jupyter notebook 2_duckdb_warehouse_setup.ipynb
```

### 问题2: "No data for segment"

**原因:** 选择的segment没有数据

**解决:**
```python
# 查看可用segments
con.execute("SELECT segment_id, COUNT(*) as rows FROM metrics_1s GROUP BY segment_id").df()

# 选择有数据的segment
TARGET_SEGMENT = 589  # 或其他有数据的segment
```

### 问题3: "Memory Error"

**原因:** 数据量太大

**解决:**
```python
# 减少max_rows
max_rows = 5000  # 降低到5000

# 或只处理一个时间窗口
TIME_WINDOWS = {'1m': 60}  # 只做1分钟
```

### 问题4: CSV文件乱码

**原因:** 编码问题

**解决:**
```python
# 指定UTF-8编码
df.to_csv(filepath, index=False, encoding='utf-8-sig')
```

---

## 🔗 下一步

### 1. 传输到Windows

```bash
# 方法A: USB
复制 export_powerbi/ 文件夹到USB
在Windows上粘贴到 C:\EUREX\export_powerbi\

# 方法B: 网络共享
# Mac: 共享文件夹
# Windows: 访问 \\MacBookPro\...

# 方法C: 云盘
上传到OneDrive/Dropbox
在Windows下载
```

### 2. 在Power BI中使用

参考文档:
- `dashboards/powerbi/ORDER_FLOW_QUICK_REF.md` (快速开始)
- `dashboards/powerbi/PYTHON_VISUAL_GUIDE.md` (详细指南)

基本步骤:
1. Power BI → Get Data → CSV → `order_flow_data.csv`
2. Insert → Python Visual
3. 拖拽字段: `price_level`, `bid_volume`, `ask_volume`, `time_window`
4. 粘贴代码: `python_order_flow_plotly.py`
5. Run

---

## 📚 技术细节

### SQL查询逻辑

```sql
-- 1. 时间分桶
CAST(ts_s / time_window_seconds AS INTEGER) * time_window_seconds

-- 2. 价格分层
ROUND(midprice / price_tick) * price_tick

-- 3. 聚合
GROUP BY time_bin, price_level

-- 4. 计算指标
SUM(bid_volume), SUM(ask_volume), SUM(bid_volume) - SUM(ask_volume)
```

### 数据流

```
DuckDB: metrics_1s
    ↓
[时间分桶 + 价格分层]
    ↓
[按time_bin + price_level聚合]
    ↓
[计算bid/ask/net volumes]
    ↓
CSV: order_flow_data.csv
    ↓
Power BI Python Visual
    ↓
Order Flow Chart
```

---

## ✅ 成功标志

运行完成后，你应该看到:

1. ✅ Console输出:
   ```
   ✅ Connected to database
   📊 Processing 1s (1s windows)...
      ✅ Generated 15,234 rows
   ...
   ✅ ORDER FLOW DATA READY FOR POWER BI
   ```

2. ✅ 文件生成:
   ```
   export_powerbi/
   ├── order_flow_data.csv  ✅
   ├── order_flow_1s.csv    ✅
   ├── order_flow_3s.csv    ✅
   ├── order_flow_1m.csv    ✅
   ├── order_flow_5m.csv    ✅
   └── order_flow_preview.png ✅
   ```

3. ✅ 预览图显示清晰的热图模式

4. ✅ summary.txt 包含完整统计信息

---

## 🎓 学习资源

- **Order Flow分析**: https://www.investopedia.com/terms/o/order-flow.asp
- **DuckDB文档**: https://duckdb.org/docs/
- **Pandas聚合**: https://pandas.pydata.org/docs/user_guide/groupby.html
- **Power BI Python Visuals**: https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-python-visuals

---

**需要帮助?** 
- 检查 `export_powerbi/order_flow_summary.txt`
- 查看 notebook中的可视化输出
- 参考 `dashboards/powerbi/` 中的Power BI指南

**Notebook版本**: 1.0  
**最后更新**: 2024-11-17  
**作者**: Ivan
