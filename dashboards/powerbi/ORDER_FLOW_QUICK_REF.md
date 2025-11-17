# Order Flow Chart - Quick Reference

## 🚀 5分钟快速实现

### 前提条件
- ✅ Power BI Desktop安装完成
- ✅ Python环境已配置（Anaconda推荐）
- ✅ 已安装: `pip install plotly matplotlib pandas`

---

## 📊 方案选择

| 特性 | Plotly | Matplotlib |
|------|---------|-----------|
| 交互性 | ✅ 完全交互 | ❌ 静态 |
| 性能 | 中等 | 快速 |
| 美观度 | 现代化 | 传统 |
| 数值标签 | 需自定义 | ✅ 内置 |
| 推荐场景 | 探索分析 | 报告展示 |

**推荐:** 先用Plotly测试，如果数据量大(>5000行)改用Matplotlib

---

## ⚡ 3步实现

### Step 1: 准备数据（Mac）

**方法A: 使用Jupyter Notebook（推荐）**
```bash
# 打开notebook
jupyter notebook notebooks/4_prepare_order_flow_for_powerbi.ipynb

# 或在VS Code中打开并运行所有cell
```

**方法B: 使用Python脚本**
```bash
cd notebooks
python prepare_order_flow_data.py
```

生成文件: `export_powerbi/order_flow_data.csv` 及分时间窗口的CSV

### Step 2: 导入Power BI（Windows）

```
Get Data → Text/CSV → order_flow_data.csv
```

### Step 3: 创建Python Visual

1. 点击 Python visual (🐍)
2. 拖拽字段:
   - price_level
   - bid_volume
   - ask_volume
   - time_window
3. 粘贴代码（选择一个）:
   - `python_order_flow_plotly.py` (交互式)
   - `python_order_flow_heatmap.py` (热图)
4. 点击 Run ▶️

---

## 📝 必需的数据字段

```csv
time_bin,price_level,bid_volume,ask_volume,net_volume,time_window
0,100.5,150,120,30,1s
1,100.5,180,140,40,1s
0,101.0,200,180,20,1s
...
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `time_bin` | int | 时间桶ID |
| `price_level` | float | 价格层级 |
| `bid_volume` | int | 买单量 |
| `ask_volume` | int | 卖单量 |
| `net_volume` | int | bid - ask |
| `time_window` | string | 1s/3s/1m/5m |

---

## 🎨 Plotly代码（极简版）

```python
import plotly.graph_objects as go

df = dataset.copy()

fig = go.Figure()

# 买单（绿）
fig.add_trace(go.Bar(
    y=df['price_level'],
    x=-df['bid_volume'],
    orientation='h',
    name='Bid',
    marker_color='green'
))

# 卖单（红）
fig.add_trace(go.Bar(
    y=df['price_level'],
    x=df['ask_volume'],
    orientation='h',
    name='Ask',
    marker_color='red'
))

fig.update_layout(
    title='Order Flow',
    barmode='overlay'
)

fig.show()
```

---

## 🔥 Matplotlib代码（极简版）

```python
import matplotlib.pyplot as plt

df = dataset.copy()

# 透视表
pivot = df.pivot_table(
    values='net_volume',
    index='price_level',
    columns='time_bin'
)

# 热图
plt.figure(figsize=(12, 8))
plt.imshow(pivot, cmap='RdYlGn', aspect='auto')
plt.colorbar(label='Net Volume')
plt.title('Order Flow Heatmap')
plt.ylabel('Price Level')
plt.xlabel('Time')
plt.show()
```

---

## 🔧 常见问题

### Q1: "Python script error"
**A:** File → Options → Python scripting → 设置Python路径

### Q2: "Module 'plotly' not found"
**A:** 
```bash
conda activate powerbi
pip install plotly
```

### Q3: 图表空白
**A:** 检查Values中的字段是否正确拖入

### Q4: 性能慢
**A:** 
- 减少数据: 在Power Query中筛选最近100个价格层级
- 或使用Matplotlib代替Plotly

---

## 📐 推荐布局

```
┌─────────────────────────────────┐
│ Time: [1s] [3s] [1m] [5m]      │ ← Slicer
└─────────────────────────────────┘

┌─────────────────────────────────┐
│                                 │
│    Order Flow Python Visual     │
│                                 │
│         (Plotly/Matplotlib)     │
│                                 │
└─────────────────────────────────┘

┌───────────┬───────────┬─────────┐
│ Total Bid │ Total Ask │Net Flow │ ← KPI Cards
│  12,450   │  11,230   │ +1,220  │
└───────────┴───────────┴─────────┘
```

---

## 🎯 下一步优化

1. **添加时间动画**
   ```python
   fig = px.bar(df, animation_frame='time_bin', ...)
   ```

2. **添加当前价格线**
   ```python
   plt.axhline(y=current_price, color='blue', linestyle='--')
   ```

3. **Volume Profile**
   ```python
   total_vol = df.groupby('price_level')['bid_volume'].sum()
   plt.barh(total_vol.index, total_vol.values)
   ```

4. **Delta分析**
   ```python
   df['delta'] = df['bid_volume'] - df['ask_volume']
   df['cumulative_delta'] = df.groupby('price_level')['delta'].cumsum()
   ```

---

## 📚 完整文档

- 详细指南: `PYTHON_VISUAL_GUIDE.md`
- Plotly完整代码: `python_order_flow_plotly.py`
- Matplotlib完整代码: `python_order_flow_heatmap.py`
- 数据准备脚本: `../notebooks/prepare_order_flow_data.py`

---

## ✨ 预期效果

### Plotly输出
- 双向柱状图（类似你的截图）
- 可缩放、平移
- Hover显示详细数据
- 颜色渐变表示成交量

### Matplotlib输出
- 热图矩阵
- 格子内显示数值
- 红绿分层
- 3个子图（Bid/Ask/Net）

---

**所需时间:** 
- 初次设置: 20-30分钟
- 后续使用: 2-3分钟

**成功标志:**
- ✅ Power BI中显示Order Flow图表
- ✅ 可以切换1s/3s/1m/5m
- ✅ 颜色正确（绿色=买，红色=卖）
- ✅ 图表随筛选器更新
