"""
Distributed Web Log Analysis with Anomaly Detection
1. Parsing logs
2. Summary statistics 
3. Anomaly detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, from_unixtime, hour, minute, unix_timestamp, avg, stddev, when, sum as spark_sum, regexp_extract, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os



class WebLogAnalyzer:    
    def __init__(self, app_name="WebLogAnalyzer"):
        self.spark = SparkSession.builder.appName(app_name).config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.df = None
        print(f"Spark session initialized: {app_name}")

    def parse_apache_log(self, log_path):
        """
        Parse Apache/Nginx web server log and extract: timestamp, IP, method, URL, status, user_agent
        """
        print(f"1. Parsing Web Server Logs")
        
        raw_logs = self.spark.read.text(log_path)
        print(f"Loaded raw logs from: {log_path}")
        
        # Log eg:
        # 192.168.1.1 - - [01/Nov/2025:10:30:45 +0000] "GET /api/users HTTP/1.1" 200 1234
        log_pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*\S*" (\d{3}) (\d+)'
        
        # Extract fields
        parsed_df = raw_logs.select(
            regexp_extract('value', log_pattern, 1).alias('ip'),
            regexp_extract('value', log_pattern, 2).alias('timestamp_str'),
            regexp_extract('value', log_pattern, 3).alias('method'),
            regexp_extract('value', log_pattern, 4).alias('url'),
            regexp_extract('value', log_pattern, 5).cast(IntegerType()).alias('status'),
            regexp_extract('value', log_pattern, 6).cast(IntegerType()).alias('bytes'),
        ).filter(col('ip') != '')
        
        # timestamp processing
        target_pattern_format = 'dd/MMM/yyyy:HH:mm:ss Z'
        parsed_df = parsed_df.withColumn('timestamp', to_timestamp(col('timestamp_str'), target_pattern_format))
        parsed_df = parsed_df.withColumn('hour', hour('timestamp'))
        parsed_df = parsed_df.withColumn('minute', minute('timestamp'))
        
        self.df = parsed_df.cache()
        row_count = self.df.count()

        print(f"Successfully parsed {row_count:,} log entries")
        print(f"Extracted fields: ip, timestamp, method, url, status, bytes")
        print("\nSample parsed records:")

        self.df.select('ip', 'timestamp', 'method', 'url', 'status').show(5, truncate=False)
        
        return self.df
    
    def compute_basic_statistics(self):
        """
        Compute basic summary statistics from parsed logs (self.df)
        1. Top-20 URLs by request count
        2. Top-20 IPs by request count
        3. HTTP status code distribution

        Returns:
            stats_dict: A dictionary with all statistics
        """
        print(f"2. Computing Basic Analytics\n")
        
        if self.df is None:
            raise ValueError("No data loaded. Run parse_apache_log() first.")
        
        stats_dict = {}
        
        # 1. Top-20 URLs by request count
        top_urls = self.df.groupBy('url').agg(count('*').alias('request_count')).orderBy(desc('request_count')).limit(20)
        stats_dict['top_urls'] = top_urls

        print(f"2.1 Top-20 URLs by request count")
        top_urls.show(20)
        
        # 2. Top-20 IPs by request count
        top_ips = self.df.groupBy('ip').agg(count('*').alias('request_count')).orderBy(desc('request_count')).limit(20)
        stats_dict['top_ips'] = top_ips

        print(f"2.2 Top-20 IPs by request count")
        top_ips.show(20)
        
        # 3. HTTP status code distribution
        status_dist = self.df.groupBy('status').agg(count('*').alias('count')).orderBy('status')
        stats_dict['status_distribution'] = status_dist

        print(f"2.3 HTTP Status Code Distribution")
        status_dist.show()
        
        return stats_dict
    
    def detect_anomalies(self, ip_threshold=100, error_rate_threshold=0.3):
        """
        Anomaly detection based on traffic patterns targetting for:
        1. High-volume IPs
        2. High error rate IPs
        
        Args:
            ip_threshold: request count threshold for high-volume detection
            error_rate_threshold: eeror rate threshold
            
        Returns:
            anomalies_dict: A dictionary with anomaly detection results
        """
        print(f"3. Detecting Traffic Anomalies")
        
        if self.df is None:
            raise ValueError("No data loaded. Run parse_apache_log() first.")
        
        anomalies_dict = {}
        
        # 3.1 High volume IP
        print(f"3.1 Detecting High-Volume IPs (threshold: {ip_threshold} requests).")
        ip_stats = self.df.groupBy('ip').agg(count('*').alias('total_requests'), spark_sum(when(col('status') >= 400, 1).otherwise(0)).alias('error_count')) \
            .withColumn('error_rate', col('error_count') / col('total_requests'))
        high_volume_ips = ip_stats.filter(col('total_requests') > ip_threshold).orderBy(desc('total_requests'))
        
        anomalies_dict['high_volume_ips'] = high_volume_ips
        high_volume_count = high_volume_ips.count()
        print(f"Found {high_volume_count} high-volume IPs")
        
        if high_volume_count > 0:
            print(f"\nTop High-Volume IPs:")
            high_volume_ips.show(10)
        
        # 3.2 High error rate
        print(f"3.2 Detecting High Error Rate IPs (threshold: {error_rate_threshold * 100}%)...")
        high_error_ips = ip_stats.filter((col('error_rate') > error_rate_threshold) & (col('total_requests') > 10)).orderBy(desc('error_rate'))
        
        anomalies_dict['high_error_ips'] = high_error_ips
        high_error_count = high_error_ips.count()
        print(f"Found {high_error_count} high error rate IPs")
        
        if high_error_count > 0:
            print(f"\nTop High Error Rate IPs:")
            high_error_ips.show(10)
        
        return anomalies_dict
    
    def stop(self):
        if self.spark:
            self.spark.stop()

def main():
    print("Distributed Web Log Analysis System running ...")
    
    analyzer = WebLogAnalyzer()
    
    try:
        log_path = "sample_logs/*.log"
        df = analyzer.parse_apache_log(log_path)
        stats = analyzer.compute_basic_statistics()
        anomalies = analyzer.detect_anomalies(ip_threshold=100, error_rate_threshold=0.3)
        
        print("All milestone goals executed successfully!")
        
    except Exception as e:
        print(e)
        raise

    finally:
        analyzer.stop()

if __name__ == "__main__":
    main()
