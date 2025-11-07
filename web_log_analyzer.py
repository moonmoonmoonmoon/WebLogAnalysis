"""
Distributed Web Log Analysis with Anomaly Detection
Systems for Data Science - Final Project
Team: Weidong Wang, Yanan Zhang, Yuxin Sun, Zhehuan Chen

This module implements a PySpark-based web log analyzer with anomaly detection capabilities.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, from_unixtime, hour, 
    minute, unix_timestamp, avg, stddev, 
    when, sum as spark_sum, regexp_extract, 
    to_timestamp, date_format
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os


class WebLogAnalyzer:
    """
    Main class for distributed web log analysis and anomaly detection.
    Uses PySpark for scalable processing of large log files.
    """
    
    def __init__(self, app_name="WebLogAnalyzer"):
        """
        Initialize Spark session with optimized configurations.
        
        Args:
            app_name (str): Name of the Spark application
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.df = None
        print(f"✓ Spark session initialized: {app_name}")
    
    def parse_apache_log(self, log_path):
        """
        Parse Apache/Nginx web server logs using PySpark DataFrames.
        Extracts: timestamp, IP, method, URL, status, response_time, user_agent
        
        Args:
            log_path (str): Path to log file(s) - supports wildcards
            
        Returns:
            DataFrame: Parsed log data
        """
        print(f"\n{'='*60}")
        print(f"GOAL 1: Parsing Web Server Logs")
        print(f"{'='*60}")
        
        # Read raw log files
        raw_logs = self.spark.read.text(log_path)
        print(f"✓ Loaded raw logs from: {log_path}")
        
        # Apache Common Log Format with extensions
        # Example: 192.168.1.1 - - [01/Nov/2025:10:30:45 +0000] "GET /api/users HTTP/1.1" 200 1234 0.123
        log_pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*\S*" (\d{3}) (\d+) ([\d.]+)'
        
        # Extract fields using regex
        parsed_df = raw_logs.select(
            regexp_extract('value', log_pattern, 1).alias('ip'),
            regexp_extract('value', log_pattern, 2).alias('timestamp_str'),
            regexp_extract('value', log_pattern, 3).alias('method'),
            regexp_extract('value', log_pattern, 4).alias('url'),
            regexp_extract('value', log_pattern, 5).cast(IntegerType()).alias('status'),
            regexp_extract('value', log_pattern, 6).cast(IntegerType()).alias('bytes'),
            regexp_extract('value', log_pattern, 7).cast('double').alias('response_time')
        ).filter(col('ip') != '')
        
        # Convert timestamp string to proper timestamp
        parsed_df = parsed_df.withColumn(
            'timestamp',
            to_timestamp(col('timestamp_str'), 'dd/MMM/yyyy:HH:mm:ss Z')
        )
        
        # Add derived time fields for analysis
        parsed_df = parsed_df.withColumn('hour', hour('timestamp'))
        parsed_df = parsed_df.withColumn('minute', minute('timestamp'))
        
        # Cache for multiple operations
        self.df = parsed_df.cache()
        
        row_count = self.df.count()
        print(f"✓ Successfully parsed {row_count:,} log entries")
        print(f"✓ Extracted fields: ip, timestamp, method, url, status, bytes, response_time")
        
        # Show sample records
        print("\nSample parsed records:")
        self.df.select('ip', 'timestamp', 'method', 'url', 'status', 'response_time').show(5, truncate=False)
        
        return self.df
    
    def compute_basic_statistics(self):
        """
        Compute basic summary statistics from parsed logs.
        Implements:
        - Top-20 URLs by request count
        - Top-20 IPs by request count
        - HTTP status code distribution
        
        Returns:
            dict: Dictionary containing all statistics
        """
        print(f"\n{'='*60}")
        print(f"GOAL 2: Computing Basic Analytics")
        print(f"{'='*60}")
        
        if self.df is None:
            raise ValueError("No data loaded. Run parse_apache_log() first.")
        
        stats = {}
        
        # Top-20 URLs by request count
        print("\n[1/3] Computing Top-20 URLs...")
        top_urls = self.df.groupBy('url') \
            .agg(count('*').alias('request_count')) \
            .orderBy(desc('request_count')) \
            .limit(20)
        
        stats['top_urls'] = top_urls
        print(f"✓ Top-20 URLs computed")
        print("\nTop 10 URLs:")
        top_urls.show(10, truncate=50)
        
        # Top-20 IPs by request count
        print("\n[2/3] Computing Top-20 IPs...")
        top_ips = self.df.groupBy('ip') \
            .agg(count('*').alias('request_count')) \
            .orderBy(desc('request_count')) \
            .limit(20)
        
        stats['top_ips'] = top_ips
        print(f"✓ Top-20 IPs computed")
        print("\nTop 10 IPs:")
        top_ips.show(10)
        
        # HTTP status code distribution
        print("\n[3/3] Computing HTTP Status Distribution...")
        status_dist = self.df.groupBy('status') \
            .agg(count('*').alias('count')) \
            .orderBy('status')
        
        stats['status_distribution'] = status_dist
        print(f"✓ HTTP Status Code Distribution computed")
        print("\nStatus Code Distribution:")
        status_dist.show()
        
        # Additional useful statistics
        total_requests = self.df.count()
        unique_ips = self.df.select('ip').distinct().count()
        unique_urls = self.df.select('url').distinct().count()
        avg_response_time = self.df.agg(avg('response_time')).collect()[0][0]
        
        print(f"\n{'='*60}")
        print(f"Summary Statistics:")
        print(f"{'='*60}")
        print(f"Total Requests:        {total_requests:,}")
        print(f"Unique IPs:            {unique_ips:,}")
        print(f"Unique URLs:           {unique_urls:,}")
        print(f"Avg Response Time:     {avg_response_time:.3f}s")
        print(f"{'='*60}")
        
        stats['summary'] = {
            'total_requests': total_requests,
            'unique_ips': unique_ips,
            'unique_urls': unique_urls,
            'avg_response_time': avg_response_time
        }
        
        return stats
    
    def detect_anomalies(self, ip_threshold=100, error_rate_threshold=0.3):
        """
        Simple anomaly detection based on traffic patterns.
        Detects:
        - High-volume IPs (potential DDoS)
        - High error rate IPs (potential attacks)
        
        Args:
            ip_threshold (int): Request count threshold for high-volume detection
            error_rate_threshold (float): Error rate threshold (0-1)
            
        Returns:
            dict: Dictionary containing anomaly detection results
        """
        print(f"\n{'='*60}")
        print(f"GOAL 3: Detecting Traffic Anomalies")
        print(f"{'='*60}")
        
        if self.df is None:
            raise ValueError("No data loaded. Run parse_apache_log() first.")
        
        anomalies = {}
        
        # High-volume IP detection
        print(f"\n[1/2] Detecting High-Volume IPs (threshold: {ip_threshold} requests)...")
        ip_stats = self.df.groupBy('ip') \
            .agg(
                count('*').alias('total_requests'),
                spark_sum(when(col('status') >= 400, 1).otherwise(0)).alias('error_count')
            ) \
            .withColumn('error_rate', col('error_count') / col('total_requests'))
        
        high_volume_ips = ip_stats.filter(col('total_requests') > ip_threshold) \
            .orderBy(desc('total_requests'))
        
        anomalies['high_volume_ips'] = high_volume_ips
        high_volume_count = high_volume_ips.count()
        print(f"✓ Found {high_volume_count} high-volume IPs")
        
        if high_volume_count > 0:
            print(f"\nTop High-Volume IPs:")
            high_volume_ips.show(10)
        
        # High error rate detection
        print(f"\n[2/2] Detecting High Error Rate IPs (threshold: {error_rate_threshold*100}%)...")
        high_error_ips = ip_stats.filter(
            (col('error_rate') > error_rate_threshold) & 
            (col('total_requests') > 10)  # Minimum requests to avoid false positives
        ).orderBy(desc('error_rate'))
        
        anomalies['high_error_ips'] = high_error_ips
        high_error_count = high_error_ips.count()
        print(f"✓ Found {high_error_count} high error rate IPs")
        
        if high_error_count > 0:
            print(f"\nTop High Error Rate IPs:")
            high_error_ips.show(10)
        
        print(f"\n{'='*60}")
        print(f"Anomaly Detection Summary:")
        print(f"{'='*60}")
        print(f"High-Volume IPs:       {high_volume_count}")
        print(f"High Error Rate IPs:   {high_error_count}")
        print(f"{'='*60}")
        
        return anomalies
    
    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()
            print("\n✓ Spark session stopped")


def main():
    """
    Main execution function demonstrating the complete workflow
    """
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║   Distributed Web Log Analysis System                        ║
    ║   Systems for Data Science - Final Project                   ║
    ║   Team: Wang, Zhang, Sun, Chen                               ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Initialize analyzer
    analyzer = WebLogAnalyzer()
    
    try:
        # Check for log files
        log_path = "sample_logs/*.log"
        
        # GOAL 1: Parse logs
        df = analyzer.parse_apache_log(log_path)
        
        # GOAL 2: Basic statistics
        stats = analyzer.compute_basic_statistics()
        
        # GOAL 3: Anomaly detection
        anomalies = analyzer.detect_anomalies(ip_threshold=100, error_rate_threshold=0.3)
        
        print("\n✓ All milestone goals executed successfully!")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
    finally:
        analyzer.stop()


if __name__ == "__main__":
    main()
