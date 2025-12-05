"""
Quick Test Script - Demonstrates core functionality
"""

from web_log_analyzer import WebLogAnalyzer
import time
import argparse

parser = argparse.ArgumentParser(description='Quick test script')
parser.add_argument('--log_file', type=str, default='sample_logs/web_10mb.log', help='Path to log file')
args = parser.parse_args()

# Initialize
analyzer = WebLogAnalyzer("QuickTest")

try:
    # Test parsing
    print("\n[TEST 1] Parsing logs.")
    start = time.time()
    df = analyzer.parse_apache_log(args.log_file)
    parse_time = time.time() - start
    print(f" PASS - Parsed in {parse_time:.2f}s")
    
    # Test analytics
    print("\n[TEST 2] Computing analytics.")
    start = time.time()
    stats = analyzer.compute_basic_statistics()
    analytics_time = time.time() - start
    print(f" PASS - Analytics computed in {analytics_time:.2f}s")
    
    # Test anomaly detection
    print("\n[TEST 3] Detecting anomalies.")
    start = time.time()
    anomalies = analyzer.detect_anomalies(ip_threshold=50)
    anomaly_time = time.time() - start
    anomalies_found = anomalies['high_volume_ips'].count()
    print(f" PASS - Found {anomalies_found} anomalies in {anomaly_time:.2f}s")
    
    print("\n" + "-"*60)
    print(" All tests passed!")
    print(f"Total time: {parse_time + analytics_time + anomaly_time:.2f}s")
    print("-"*60)
    
except Exception as e:
    print(f" TEST FAILED: {e}")
    raise
finally:
    analyzer.stop()
