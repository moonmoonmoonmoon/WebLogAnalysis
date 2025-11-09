# Distributed Web Log Analysis with Anomaly Detection

A PySpark-based system for analyzing large-scale web server logs and detecting traffic anomalies in real-time.

## 🎯 Features

- **Distributed Log Parsing**: Process millions of log entries using Apache Spark
- **Real-time Analytics**: Compute traffic statistics (top URLs, IPs, status codes)
- **Anomaly Detection**: Identify suspicious traffic patterns including DDoS attacks
- **Visualization**: Generate professional charts and reports
- **Synthetic Data**: Built-in log generator for testing and development

## 🚀 Quick Start

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

# Or run quick test
python quick_test.py
```

## 📊 Performance

- **Processing Speed**: 17,000+ entries/second
- **Scalability**: Handles datasets from MB to GB scale
- **Efficiency**: Processes 80K logs in under 5 seconds

## 🏗️ Architecture

```
┌─────────────────────────────────────┐
│      Web Server Logs                │
│   (Apache/Nginx format)             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│    PySpark Log Parser               │
│  (Regex-based extraction)           │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│   Distributed Processing            │
│  • Analytics  • Aggregation         │
└──────────────┬──────────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌──────────────┐
│  Analytics  │  │   Anomaly    │
│  • Top URLs │  │   Detection  │
│  • Top IPs  │  │  • High Vol  │
│  • Status   │  │  • Errors    │
└─────────────┘  └──────────────┘
```

## 📁 Project Structure

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

## 🔍 Analysis Capabilities

### Basic Analytics
- Top-N URLs by request count
- Top-N IPs by request count
- HTTP status code distribution
- Traffic volume over time

### Anomaly Detection
- **High-Volume IPs**: Detects potential DDoS sources
- **Error Rate Surge**: Identifies attack patterns (100% error rate)
- **Configurable Thresholds**: Adjust sensitivity based on requirements

### Example Output
```
Top 10 Anomalous IPs:
  4.8.41.25        → 214 requests, 100% error rate
  214.3.198.102    → 208 requests, 100% error rate
  77.124.250.162   → 205 requests, 100% error rate
```

## 📈 Visualization

The system generates publication-quality charts:
- HTTP Status Distribution (Pie Chart)
- Top URLs/IPs (Bar Chart)
- Performance Analysis (Line/Bar Chart)
- Anomalous Traffic Tables

## 🧪 Testing

```bash
# Generate test data
python log_generator.py

# Run tests
python quick_test.py
```

**Test Results:**
- ✅ Log Parser: 2.3s for 50K entries
- ✅ Analytics: 1.5s
- ✅ Anomaly Detection: 0.8s

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

## 📊 Sample Datasets

Included synthetic datasets:
- **web_10mb.log**: 50K normal traffic entries (4.4 MB)
- **web_mixed.log**: 30K mixed traffic with simulated DDoS (2.6 MB)

## 🤝 Contributing

Contributions are welcome! Feel free to:
- Report bugs
- Suggest features
- Submit pull requests

## 📄 License

MIT License

## 👥 Authors

- Weidong Wang
- Yanan Zhang
- Yuxin Sun
- Zhehuan Chen

## 🔗 Links

- Course: Systems for Data Science
- Institution: UMass Amherst

---

**Note**: This project was developed as part of a graduate-level data science course focusing on distributed systems and large-scale data processing.
