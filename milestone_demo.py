import time
import os
import sys


def print_header(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")


def main():
    print("""DISTRIBUTED WEB LOG ANALYSIS WITH ANOMALY DETECTION """)
    
    # Generate synthetic logs
    print_header("1. Generating Synthetic Web Server Logs")
    print("Creating sample datasets for testing and evaluation.")
    
    from log_generator import SyntheticLogGenerator
    
    os.makedirs('sample_logs', exist_ok=True)
    generator = SyntheticLogGenerator(seed=42)
    
    # Generate small dataset (10MB)
    print("\nGenerating normal traffic log (10MB).")
    start = time.time()
    generator.generate_normal_traffic(
        output_file='sample_logs/web_10mb.log',
        num_requests=50000,
        duration_hours=1,
    )
    gen_time_1 = time.time() - start
    
    # Generate mixed traffic with anomalies
    print("\nGenerating mixed traffic with simulated attacks.")
    start = time.time()
    generator.generate_mixed_traffic(
        output_file='sample_logs/web_mixed.log',
        num_requests=30000,
        duration_hours=2,
        error_rate=0.8,
        attack_duration_hours=0.333,
    )
    gen_time_2 = time.time() - start
    
    print(f"\nLog generation completed in {gen_time_1 + gen_time_2:.2f}s")
    
    # Parse and analyze logs
    print_header("2. Running PySpark Log Analysis")
    print("Initializing distributed analysis system.")
    
    from web_log_analyzer import WebLogAnalyzer
    
    analyzer = WebLogAnalyzer("Demo")
    
    try:
        start = time.time()
        df = analyzer.parse_apache_log('sample_logs/*.log')
        parse_time = time.time() - start
        print(f"\nParsing completed in {parse_time:.2f}s")
        
        # Basic analytics
        start = time.time()
        stats = analyzer.compute_basic_statistics()
        analytics_time = time.time() - start
        print(f"\nAnalytics completed in {analytics_time:.2f}s")
        
        # Anomaly detection
        start = time.time()
        anomalies = analyzer.detect_anomalies(ip_threshold=100, error_rate_threshold=0.3)
        anomaly_time = time.time() - start
        print(f"\nAnomaly detection completed in {anomaly_time:.2f}s")
        
        # Generate visualizations
        print_header("3: Generating Evaluation Charts")
        print("Creating visualizations for milestone report.")
        
        from visualizations import LogVisualizer
        
        visualizer = LogVisualizer(output_dir='charts')
        
        # Pie Chart: Status distribution
        print("\nCreating HTTP status distribution chart...")
        visualizer.plot_status_distribution(
            stats['status_distribution'],
            'status_distribution.png'
        )
        
        # Bar Chart: Top URLs
        print("\nCreating top URLs chart...")
        visualizer.plot_top_urls(
            stats['top_urls'],
            top_n=20,
            output_file='top_urls.png'
        )
        
        # Performance chart
        print("\nCreating parsing performance chart...")
        # Use actual parsing time
        file_size_mb = os.path.getsize('sample_logs/web_10mb.log') / (1024 * 1024)
        visualizer.plot_parsing_performance(
            file_sizes_mb=[file_size_mb],
            runtimes_sec=[parse_time],
            output_file='parsing_runtime.png'
        )
        
        # Anomalous IPs table
        if anomalies['high_volume_ips'].count() > 0:
            print("\nCreating anomalous IPs table...")
            visualizer.plot_anomalous_ips_table(
                anomalies['high_volume_ips'],
                top_n=10,
                output_file='anomalous_ips_table.png'
            )
        
        print_header("SUMMARY")
        
        print("Log Parser - COMPLETED")
        print(f"  • Implemented PySpark DataFrame parser")
        print(f"  • Extracts: timestamp, IP, method, URL, status")
        print(f"  • Processed {df.count():,} log entries")
        print(f"  • Runtime: {parse_time:.2f}s")
        
        print("Basic Analytics - COMPLETED")
        print(f"  • Top-20 URLs by request count")
        print(f"  • Top-20 IPs by request count")
        print(f"  • HTTP status code distribution")
        print(f"  • Runtime: {analytics_time:.2f}s")
        
        print("Anomaly Detector - COMPLETED")
        print(f"  • High-volume IP detection")
        print(f"  • High error rate detection")
        print(f"  • Found {anomalies['high_volume_ips'].count()} suspicious IPs")
        print(f"  • Runtime: {anomaly_time:.2f}s")
        
        print("Evaluation Charts - COMPLETED")
        print(f"  • Status code pie chart")
        print(f"  • Top URLs bar chart")
        print(f"  • Parsing performance chart")
        print(f"  • Charts saved to: charts/")
        
        print("\n" + "="*70)
        print(f"Total execution time: {parse_time + analytics_time + anomaly_time:.2f}s")
        print("="*70)
        
        print("\nOutput files:")
        print("Logs:  sample_logs/")
        print("Charts: charts/")
        
    except Exception as e:
        print(f"\nError during execution: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.stop()


if __name__ == "__main__":
    main()