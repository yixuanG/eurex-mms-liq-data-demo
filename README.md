# Eurex Market Microstructure Liquidity Analysis

**Demonstrating quantitative analysis skills through high-frequency market data engineering and liquidity visualization.**

This repository showcases end-to-end expertise in market microstructure analysis: from raw tick data processing to analytical warehouse design and interactive visualization. Built to demonstrate the basic proficiency for the role of derivatives micro liquidity analysis for market design reference.

To save time, please view the final interactive dashboard in `/dashboards/GUO_Yixuan_eurex-mms-liq-viz-demo.pbix`. In case of unavailable Python visuals on the dashboard, please view the static export `/dashboards/GUO_Yixuan_eurex-mms-liq-viz-demo.pdf`.

> This modest result serves merely as a showcase of my initial understanding and competency in this role at Eurex. While recognizing the current results fall short of professional standards, I understand deepening experience and support from the Eurex colleagues will be crucial for me to developing code and visuals with tangible business value.

## Job-related knowledge and skills used for this demonstration

* **Financial Markets Microstructure**
  * An open-source course by Egor Starkov of University of Copenhagen)

    * Files (syllabus, slides, homeworks, exams): [[Spring 2025]](https://starkov.site/teaching/FMM_2025.7z). Older versions: [[Spring 2020]](https://starkov.site/teaching/FMM_2020.zip).
    * LaTeX sources: [[github]](https://github.com/electronicgore/finmarkets)
    * Lecture recordings (unedited): [[2020]](https://www.youtube.com/playlist?list=PL4pUs4P_j1Wa2_P1lw44kFWWjKDTGUY7S) (youtube playlist)

    **Concepts learned and applied:**

    * Three Dimensions of Liquidity (tightness, depth, resiliency)
    * Market Maker Economics (spread decomposition) *(due to lack of transaction price, realized spread wasn't able to be calculated)*
    * Order Book Dynamics (reconstruction algorithm)
    * Price Impact Models (Kyle's lambda)
    * Market Quality Metrics (academic standard measures)

- **Data Engineering**: High-frequency data pipeline, batch processing at scale, schema inference
- **Analytical Warehouse**: DuckDB design with time-series optimization and multi-granularity aggregation
- **System Design**: Memory-efficient processing , storage optimization, parallelization
- **Business Analytics**: Interactive Power BI dashboards for liquidity monitoring

---

## 1. Data Source

**Provider**: Eurofidai - Eurex dataset sample data
**Data Source:** https://www.eurofidai.org/high-frequency-database
**Date**: December 1, 2020
**Original Source:** **Eurex T7 Trading System (Release 8.1)**
**Data Type****:** High-frequency market data with nanosecond precision

```none
Sample_Eurex_20201201_10MktSegID/
├── DataSample.xlsx              # Product reference metadata
├── PS_20201201.csv              # Product snapshot (global)
└── {MarketSegmentID}/           # Per-product directories
    ├── DI_{ID}_20201201.csv     # Depth Incremental
    ├── DS_{ID}_20201201.csv     # Depth Snapshot
    ├── IS_{ID}_20201201.csv     # Instrument Snapshot
    ├── ISC_{ID}_20201201.csv    # Instrument State Change
    ├── MISC_{ID}_20201201.csv   # Mass Instrument State Change
    ├── PSC_{ID}_20201201.csv    # Product State Change
    ├── CIU_{ID}_20201201.csv    # Complex Instrument Update (optional)
    ├── II_{ID}_20201201.csv     # Instrument Incremental (optional)
    ├── QR_{ID}_20201201.csv     # Quote Request (optional)
    └── CR_{ID}_20201201.csv     # Cross Request (optional)
```

### Market Segments in Dataset

| Segment ID | Mkt Seg | File Size (DI) | File Size (DS) | Activity  | Remark                                    |
| :--------- | ------- | -------------- | -------------- | --------- | ----------------------------------------- |
| 48         | ADSG    | 1.22 MB        | 0.97 MB        | Low       |                                           |
| 589        | FDAX    | 221.08 MB      | 2830.40 MB     | High      |                                           |
| 688        | FGBL    | 366.51 MB      | 2064.69 MB     | High      |                                           |
| 821        | FVS     | 54.77 MB       | 183.36 MB      | Medium    |                                           |
| 1176       | ODAX    | 9610.09 MB     | 4584.69 MB     | Very high | Not used for demo due to time consumption |
| 1373       | OGBL    | 921.57 MB      | 1106.05 MB     | High      | Not used for demo due to time consumption |
| 1374       | OEXD    | 0.38 MB        | 21.02 MB       | Low       | Not used for due to missing columns       |

---

## 2. Raw Data Interpretation

**Depth Incremental (DI) Message Format**:
Each message represents an order book update with nanosecond precision:

- `MsgSeqNum`: Sequence number for ordering
- `TransactTime`: Nanosecond timestamp
- `MDUpdateAction`: Add (0), Change (1), or Delete (2)
- `MDEntryType`: Bid or Ask side
- `SecurityID`: Product identifier
- `MDPriceLevel`: Depth level *(Note: The market depths of the sample data reaches up to only level 5)*
- `MDEntryPx`: Price
- `MDEntrySize`: Volume

**Key Insight**: Raw data is **incremental** - each message only contains the change, not full snapshots. Requires stateful order book reconstruction to derive analytical metrics.

**Data Quality Considerations**:

- Handle out-of-sequence messages
- Auto-detect maximum depth per segment (sample data: L0-L5)
- Parse variable schemas (column order differs by segment)

---

## 3. About This Repository

### Quick Start

**Step 1**: Environment Setup

```bash
# Open in Google Colab
notebooks/0_env_db_prep.ipynb
```

- Mounts Google Drive
- Extracts raw data to Colab local SSD
- Creates symlinks for navigation

**Step 2**: Process Market Segments

```bash
# Batch process all segments
notebooks/1_batch_process_all_segments.ipynb
```

- Parses incremental messages
- Reconstructs order book snapshots (L0-L5)
- Aggregates to 1-second granularity
- Outputs: Parquet files (~1-2 GB)

**Step 3**: Build Analytical Warehouse

```bash
# Import to DuckDB
notebooks/2_duckdb_warehouse_setup.ipynb
```

- Loads processed data into DuckDB
- Creates time-series optimized tables
- Generates 5s/1m rollup views

**Step 4**: Visualize in Power BI

- Open `dashboards/powerbi/eurex_liquidity.pbix`
- Connect to DuckDB warehouse
- Interactive dashboards ready to use

### Key Scripts

- `scripts/parse_and_l5.py`: Parse DI messages and reconstruct order book
- `scripts/aggregate_l5.py`: Calculate liquidity metrics at 1s intervals
- `scripts/process_all_segments.py`: Batch processing orchestration
- `scripts/setup_duckdb_warehouse.py`: Warehouse schema setup

---

## 4. Data Preprocessing Methods

### (1) Schema Inference & Parsing (`src/eurex_liquidity/parser.py`)

**Challenge**: Each segment has different column ordering and schemas.
**Solution**: Automatic schema detection by analyzing column names and patterns.

- Identifies key fields: timestamps, price levels, sides, actions
- Handles missing columns gracefully
- Validates data types and ranges

### (2) Order Book Reconstruction (`src/eurex_liquidity/orderbook_multi.py`)

**Challenge**: Incremental updates → Need to maintain stateful order book.
**Technical Approach**:

1. **Initialize** empty order book state (bid/ask dictionaries by price level)
2. **Process** each message sequentially:
   - **Add (0)**: Insert new price level
   - **Change (1)**: Update existing level
   - **Delete (2)**: Remove price level
3. **Snapshot**: Capture full L0-L5 state at each timestamp
4. **Validate**: Check for crossed markets and data anomalies

**Performance Optimization**:

- Process in batches (100K messages)
- Use vectorized pandas operations
- Memory-efficient: Only store active price levels

### (3) Liquidity Metrics Calculation (`scripts/aggregate_l5.py`)

**Metrics Computed** (at 1-second intervals):

**1. Tightness**:

- Bid-ask spread: `spread = best_ask - best_bid`
- Relative spread: `rel_spread = spread / midprice`
- Effective spread (for trades)

**2. Depth**:

- Volume at each level (L0-L5)
- Total bid/ask volume
- Depth imbalance: `imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)`

**3. Price Formation**:

- Midprice: `(best_bid + best_ask) / 2`
- Microprice: Volume-weighted midprice
- Depth-weighted price

**4. Market Activity**:

- Message rate (updates/second)
- Price volatility (std dev of midprice)
- Trade intensity

### (4) Data Pipeline Architecture

```
Raw CSV (14GB) 
  → Parse & Validate
  → Reconstruct Order Book (L0 to L5)
  → Calculate Metrics (1s granularity)
  → Export Parquet (1-2GB)
  → Load to DuckDB (500MB optimized)
```

**Storage Optimization**: 14 GB → 500 MB (96% reduction through aggregation)

---

## 5️⃣ DuckDB Analytical Warehouse

### Database Schema Design

**Table: `liquidity_metrics`**

```sql
CREATE TABLE liquidity_metrics (
    timestamp TIMESTAMP,
    segment_id INTEGER,
    security_id BIGINT,
    -- Price metrics
    best_bid DOUBLE,
    best_ask DOUBLE,
    mid_price DOUBLE,
    spread DOUBLE,
    rel_spread DOUBLE,
    -- Volume metrics (L1-L5)
    bid_vol_l1 DOUBLE,
    ask_vol_l1 DOUBLE,
    bid_vol_l5 DOUBLE,
    ask_vol_l5 DOUBLE,
    -- Imbalance metrics
    imbalance_l1 DOUBLE,
    imbalance_l5 DOUBLE,
    -- Activity metrics
    msg_count INTEGER,
    price_volatility DOUBLE,
    -- Partition key
    PRIMARY KEY (timestamp, segment_id, security_id)
);
```

### Key Design Decisions

**1. Time-Series Optimization**:

- Primary key on `(timestamp, segment_id, security_id)`
- Columnar storage for analytical queries
- Automatic compression (~90% for repetitive data)

**2. Multi-Granularity Views**:

```sql
-- 5-second rollup
CREATE VIEW liquidity_5s AS
SELECT 
    time_bucket('5 seconds', timestamp) as ts_5s,
    segment_id,
    security_id,
    AVG(mid_price) as avg_mid,
    AVG(spread) as avg_spread,
    SUM(msg_count) as total_msgs
FROM liquidity_metrics
GROUP BY ts_5s, segment_id, security_id;

-- 1-minute rollup (for dashboards)
CREATE VIEW liquidity_1m AS ...
```

**3. Indexing Strategy**:

- Clustered index on timestamp (range queries)
- Secondary index on `segment_id` (filtering)
- Bloom filters for `security_id` (point lookups)

**4. Query Performance**:

- Typical query: < 100ms for 1 day of data
- Aggregations: Leverage pre-computed rollups
- Parallel execution: DuckDB auto-parallelizes

### Warehouse Potential Benefits

(Without ODBC licence, I wasn't able to connect the DuckDB warehouse to PowerBI.)

- **Fast Analytics**: Columnar format optimized for OLAP
- **Small Footprint**: 500 MB vs 14 GB raw (96% compression)
- **Easy Integration**: Standard SQL, ODBC/JDBC drivers
- **No Infrastructure**: Embedded database, no server needed

---

## 6. Power BI Visualization & Interpretation

### Dashboard Overview

**File**: `dashboards/powerbi/eurex_liquidity.pbix`

### Key Visualizations

**1. Liquidity Heatmap**

- **Purpose**: Identify illiquid periods and price levels across market segments
- **Metrics**: Bid-ask spread over time, color-coded by severity
- **Market Design Value**: Detect systematic liquidity droughts that may require intervention (e.g., market maker incentive adjustments, tick size calibration)

**2. Order Book Depth Profile**

- **Purpose**: Visualize depth distribution across price levels
- **Metrics**: Volume distribution across L1-L5, depth concentration ratios
- **Market Design Value**: Assess whether displayed liquidity is sufficient; steep drop-offs may indicate need for improved market maker obligations or pro-rata matching adjustments

**3. Order Flow Imbalance Monitoring**

- **Purpose**: Track order book imbalance patterns across segments
- **Metrics**: Order imbalance by side, net volume delta, imbalance persistence
- **Market Design Value**: Identify structural imbalances that may signal need for circuit breakers or trading halt rule refinements

**4. Spread Dynamics**

- **Purpose**: Monitor market tightness as indicator of market quality
- **Metrics**: Absolute & relative spread time series, spread volatility
- **Market Design Value**: Evaluate if current tick sizes are appropriate; persistent wide spreads may indicate need for market maker incentive program review

**5. Market Activity Dashboard**

- **Purpose**: Monitor overall market health and operational efficiency
- **Metrics**: Message rate, quote updates per second, order-to-trade ratios
- **Market Design Value**: Detect unusual activity patterns (e.g., quote stuffing, excessive cancellations) that may require surveillance or rule enforcement

### Sample Dashboard Visualizations

**Order Book Imbalance Analysis**

![Order Book Imbalance](visuals/Order%20Book%20Imbalance%20(attempt).png)

*Tracks buy/sell pressure across price levels. Persistent imbalances can signal need for circuit breaker calibration or market maker incentive adjustments.*

**Intraday Spread Evolution**

![Intraday Spread Evolution](visuals/Intraday%20Spread%20Evolution%20(attempt).png)

*Monitors spread dynamics throughout the trading day. Identifies periods of low market quality that may require intervention.*

**Bid-Ask Volume Heatmap**

![Bid-Ask Volume Heatmap](visuals/Bid-Ask%20Volume%20Heatmap%20%EF%BC%88attempt%EF%BC%89.png)

*Visualizes liquidity distribution across time and price levels. Helps assess depth adequacy and detect liquidity gaps.*

**Cross-Segment Comparison**

![Cross-Segment Comparison](visuals/Cross-Segment%20Comparison%20(attempt).png)

*Compares liquidity quality across different market segments. Enables identification of underperforming products requiring attention.*

### Technical Implementation

**Data Connection**:

- ~~Power BI → DuckDB via ODBC)~~  Failed due to no license
- ~~DirectQuery mode for real-time data~~
- ~~Import mode for historical analysis~~

**Interactivity Features**:

- **Slicers**: Filter by segment, security, time range
- **Drill-through**: Click spread → see order book detail
- **Cross-filtering**: Select time period → update all visuals

### Market Design Applications

**Liquidity Monitoring & Assessment**:

- Track liquidity quality metrics across segments to identify products requiring attention
- Compare liquidity profiles across similar instruments to detect outliers
- Monitor temporal patterns (intraday, day-of-week) to inform market hours or auction design

**Market Structure Evaluation**:

- Assess impact of tick size on spread tightness and depth display
- Evaluate pro-rata vs FIFO matching effectiveness across product types
- Identify whether market maker obligations are sufficient for various liquidity regimes

**Evidence-Based Policy Support**:

- Provide quantitative evidence for rule changes (e.g., circuit breaker thresholds, tick size adjustments)
- Monitor market quality before/after structural changes to measure impact
- Support discussions with market participants using objective liquidity data

**Surveillance & Market Quality**:

- Detect anomalous trading patterns that may indicate manipulation or operational issues
- Monitor message-to-trade ratios to assess if excessive messaging fees are needed
- Identify periods of market stress requiring heightened surveillance
