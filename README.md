# Distributed Web Log Analysis with Anomaly Detection

A PySpark-based system for analyzing large-scale web server logs and detecting traffic anomalies in real-time.

**Team:** Weidong Wang, Yanan Zhang, Yuxin Sun, Zhehuan Chen

---

## Final Project: Spark Performance Analysis

**Focus:** Partitioning & Caching Strategies for Large-Scale Log Processing

### Goals
✅ **Goal 1:** Process 1GB datasets with attack patterns
✅ **Goal 3:** Compare partitioning strategies (default vs IP-hash)
✅ **Goal 4:** Compare caching strategies (none, cache, persist)
✅ **Goal 5:** Scalability analysis (100MB-1GB)
✅ **Goal 6:** Performance comparison charts

### Quick Start

```bash
# Complete pipeline (generate + evaluate + visualize)
python run_all.py

# Or run individually:
python gen_large_logs.py      # Generate 100MB-1GB datasets
python run_evaluation.py      # Run performance tests
python generate_charts.py     # Generate visualizations
```

### Key Results
- **Scalability:** Linear scaling from 100MB (5s) to 1GB (14s)
- **Partitioning:** IP-hash 3% faster than default on 1GB dataset
- **Caching:** cache() and persist() both ~29% faster than no caching on 1GB dataset

### Files
- `run_all.py` - Master pipeline script
- `gen_large_logs.py` - Large dataset generator (100MB-1GB)
- `run_evaluation.py` - Performance evaluation
- `generate_charts.py` - Visualization generator

### Output
- `datasets/` - 4 log files (100MB, 250MB, 500MB, 1GB)
- `results/` - Performance metrics (JSON)
- `charts/` - Comparison charts (PNG)

---

## Milestone Project: Anomaly Detection

### Goals
✅ **Goal 1:** Log parser with PySpark DataFrames
✅ **Goal 2:** Summary statistics (Top URLs/IPs, status distribution)
✅ **Goal 3:** Anomaly detection (high-volume IPs, error-rate surge)
✅ **Goal 4:** Visualization charts

## Quick Start

### Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Java 8 or 11

### Installation

```bash
# Install dependencies
pip install pyspark matplotlib seaborn pandas numpy
```

Requirements: Python 3.8+, Apache Spark 3.x, Java 8+

### Usage

```bash
# Milestone demos
python milestone_demo.py      # Complete demo
python quick_test.py         # Quick test
python log_generator.py      # Generate sample logs
```

### Project Structure

```
WebLogAnalysis/
├── run_all.py              # Final: Master pipeline
├── gen_large_logs.py       # Final: Large dataset generator
├── run_evaluation.py       # Final: Performance evaluation
├── generate_charts.py      # Final: Visualization
├── web_log_analyzer.py     # Milestone: Analysis engine
├── log_generator.py        # Milestone: Log generator
├── visualizations.py       # Milestone: Charts
├── milestone_demo.py       # Milestone: Demo
├── quick_test.py          # Milestone: Tests
├── datasets/              # Generated datasets
├── results/               # Performance metrics (JSON)
└── charts/                # Visualizations (PNG)
```

### Analysis Capabilities
- **Top-N Analytics:** URLs, IPs by request count
- **Status Distribution:** HTTP status code analysis
- **Anomaly Detection:** High-volume IPs, error-rate surge
- **Performance Metrics:** Parse time, aggregation time, total time

---

## Technical Details

### Technology Stack
- Apache Spark 3.x for distributed processing
- PySpark for Python API
- Matplotlib/Seaborn for visualization
- Pandas/NumPy for data manipulation

### Performance Strategies

**Partitioning (Goal 3):**
- **Default:** Round-robin distribution
- **IP-hash:** `repartition(8, 'ip')` - Optimizes IP groupBy queries (no shuffle)

**Caching (Goal 4):**
- **None:** No caching (baseline)
- **cache():** In-memory caching
- **persist():** MEMORY_AND_DISK (best performance)

**Metrics (Goal 5):**
- **Parse Time:** Data loading + preprocessing
- **Aggregation Time:** Analytics queries (Top URLs/IPs, hourly stats)
- **Anomaly Time:** Anomaly detection
- **Total Time:** Sum of all phases

### Configuration

```python
# Anomaly detection thresholds
ip_threshold=100          # Min requests to flag
error_rate_threshold=0.3  # Min error rate (30%)

# Spark configuration (run_evaluation.py)
spark.driver.memory = "4g"
spark.executor.memory = "4g"
spark.sql.shuffle.partitions = "8"
```

---

## Team Contributions

- **Yanan Zhang:** Performance evaluation framework, partitioning/caching
- **Weidong Wang:** Dataset generator, attack pattern simulation
- **Yuxin Sun:** Visualization module, chart generation
- **Zhehuan Chen:** Pipeline integration, documentation

