# Distributed Web Log Analysis with Anomaly Detection

A PySpark-based system for analyzing large-scale web server logs and detecting traffic anomalies in real-time.

## Milestone Goals

-Goal 1: Implement a log parser using PySpark DataFrames that processes synthetic/real Web Server logs and extracts necessary fields (e.g., timestamp, IP, method, URL, status, response_time, user_agent). - COMPLETED
-Goal 2: Develop basic analytics functions to compute basic summary statistics.- COMPLETED
Top-20 URLs by request count
Top-20 IPs by request count
HTTP status code distribution
-Goal 3: Create a simple detector to detect abnormal traffic. - COMPLETED
High-volume IPs 
Sudden spike 
Error-rate surge 
-Goal 4: Generate simple evaluation charts. - PARTIALLY COMPLETED
Bar chart: parsing runtime vs file size (10MB, 50MB, 100MB)
Pie chart: HTTP status code distribution
Table: top-10 anomalous IPs


## Quick Start

### Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Java 8 or 11

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/WebLogAnalysis.git
cd WebLogAnalysis

# Install dependencies
pip install pyspark matplotlib seaborn pandas
```

### Usage

```bash
# Generate sample logs
python log_generator.py

# Run complete demo
python milestone_demo.py

# Run quick test
python quick_test.py
```

## Project Structure

```
WebLogAnalysis/
├── web_log_analyzer.py      # Main analysis engine
├── log_generator.py         # Synthetic log generator
├── visualizations.py        # Chart generation
├── milestone_demo.py        # Complete demo
├── quick_test.py           # Quick functionality test
└── sample_logs/            # Sample datasets
    ├── web_10mb.log
    └── web_mixed.log
```

## Analysis Capabilities

### Basic Analytics
- Top-N URLs by request count
- Top-N IPs by request count
- HTTP status code distribution

### Anomaly Detection
- High-Volume IPs: Detects potential DDoS sources
- Error Rate Surge: Identifies attack patterns (100% error rate)
- Configurable Thresholds: Adjust sensitivity based on requirements

### Example Output
```
Top 10 Anomalous IPs:
  4.8.41.25        → 214 requests, 100% error rate
  214.3.198.102    → 208 requests, 100% error rate
  77.124.250.162   → 205 requests, 100% error rate
```

##  Visualization

Bar chart:  1. Top-20 URLs by request count
            2. Top-20 IPs by request count
            3. parsing runtime vs file size (10MB, 50MB, 100MB)
Pie chart:  4. HTTP status code distribution
Table:      5. Top-10 anomalous IPs

## Testing

```bash
# Generate test data
python log_generator.py

# Run tests
python quick_test.py
```

## 🛠️ Technology Stack

- **Apache Spark**: Distributed data processing
- **PySpark**: Python API for Spark
- **Matplotlib/Seaborn**: Data visualization
- **Pandas**: Data manipulation
- **Python 3.8+**: Core language

## 📖 API Example

```python
from web_log_analyzer import WebLogAnalyzer

# Initialize
analyzer = WebLogAnalyzer("MyAnalysis")

# Parse logs
df = analyzer.parse_apache_log('logs/*.log')

# Compute statistics
stats = analyzer.compute_basic_statistics()

# Detect anomalies
anomalies = analyzer.detect_anomalies(
    ip_threshold=100,
    error_rate_threshold=0.3
)

# Clean up
analyzer.stop()
```

## 🔧 Configuration

### Anomaly Detection Thresholds

```python
# In web_log_analyzer.py
anomalies = analyzer.detect_anomalies(
    ip_threshold=100,        # Min requests to flag high-volume
    error_rate_threshold=0.3 # Min error rate (30%) to flag suspicious
)
```

### Log Generator Parameters

```python
# In log_generator.py
generator.generate_mixed_traffic(
    output_file="log_file.log",
    num_requests=50000,
    error_rate=0.8,
    duration_hours=2,
    attack_duration_hours=0.333,
)
```

##  Sample Datasets

Included synthetic datasets:
- **web_5mb.log**: 50K normal traffic entries (4.2 MB)
- **web_mixed.log**: 30K mixed traffic with simulated DDoS (2.6 MB)

## Authors

- Weidong Wang
- Yanan Zhang
- Yuxin Sun
- Zhehuan Chen

---

**Note**: This project was developed as part of a graduate-level data science course focusing on distributed systems and large-scale data processing.
We also used the ChatGPT to help us to work on the project 