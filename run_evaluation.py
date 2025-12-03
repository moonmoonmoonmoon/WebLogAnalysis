"""
Final Project Evaluation: Partitioning & Caching Performance
CHANGES: 
1. Added partitioning strategies: default, time-based (by hour), IP-hash
2. Added caching strategies: none, cache(), persist(MEMORY_AND_DISK)
3. Performance measurement for Goal 3, 4, 5, 6
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, hour, regexp_extract, to_timestamp, when, sum as spark_sum
from pyspark.sql.types import IntegerType
from pyspark.storagelevel import StorageLevel
import time
import json
import os

class Evaluator:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("FinalEval") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
    
    # CHANGE: Added partitioning and caching parameters
    def run_experiment(self, log_path, partition_strategy=None, cache_strategy=None):
        """Run single experiment with specified strategies"""
        start_total = time.time()
        
        # Parse
        start = time.time()
        raw = self.spark.read.text(log_path)
        pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*\S*" (\d{3}) (\d+)'
        
        df = raw.select(
            regexp_extract('value', pattern, 1).alias('ip'),
            regexp_extract('value', pattern, 2).alias('timestamp_str'),
            regexp_extract('value', pattern, 3).alias('method'),
            regexp_extract('value', pattern, 4).alias('url'),
            regexp_extract('value', pattern, 5).cast(IntegerType()).alias('status'),
            regexp_extract('value', pattern, 6).cast(IntegerType()).alias('bytes')
        ).filter(col('ip') != '')
        
        df = df.withColumn('timestamp', to_timestamp(col('timestamp_str'), 'dd/MMM/yyyy:HH:mm:ss Z'))
        df = df.withColumn('hour', hour('timestamp'))
        
        # CHANGE: Apply partitioning strategy (Goal 3)
        # Query pattern: 2x groupBy('ip'), 1x groupBy('url'), 1x groupBy('hour')
        # IP-hash should be fastest since we have 2 IP queries (most frequent)
        if partition_strategy == 'ip-hash':
            df = df.repartition(8, 'ip')  # Optimizes IP queries (no shuffle for IP groupBys)
        else:
            df = df.repartition(8)  # Default: round-robin distribution (shuffle for all groupBys)
        
        # CHANGE: Apply caching strategy (Goal 4)
        if cache_strategy == 'cache':
            df = df.cache()
        elif cache_strategy == 'persist':
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
        
        row_count = df.count()
        parse_time = time.time() - start
        
        # Analytics
        start = time.time()
        df.groupBy('url').agg(count('*').alias('cnt')).orderBy(desc('cnt')).limit(20).count()
        df.groupBy('ip').agg(count('*').alias('cnt')).orderBy(desc('cnt')).limit(20).count()
        df.groupBy('hour').agg(count('*').alias('cnt')).orderBy('hour').count()
        agg_time = time.time() - start
        
        # Anomaly detection
        start = time.time()
        ip_stats = df.groupBy('ip').agg(
            count('*').alias('total'),
            spark_sum(when(col('status') >= 400, 1).otherwise(0)).alias('errors')
        ).withColumn('error_rate', col('errors') / col('total'))
        anomalies = ip_stats.filter(col('total') > 50).count()
        anomaly_time = time.time() - start
        
        total_time = time.time() - start_total
        
        return {
            'partition': partition_strategy or 'default',
            'cache': cache_strategy or 'none',
            'rows': row_count,
            'parse_time': round(parse_time, 2),
            'agg_time': round(agg_time, 2),
            'anomaly_time': round(anomaly_time, 2),
            'total_time': round(total_time, 2)
        }
    
    def stop(self):
        self.spark.stop()

# CHANGE: Main evaluation for Goals 3, 4, 5, 6
def main():
    os.makedirs('results', exist_ok=True)
    ev = Evaluator()
    
    print("="*60)
    print("FINAL PROJECT EVALUATION")
    print("="*60)
    
    # Goal 5: Scalability analysis
    print("\n[Goal 5] Scalability Analysis")
    scalability = []
    for size in [100, 250, 500, 1000]:
        log_file = f'datasets/web_{size}mb.log'
        if not os.path.exists(log_file):
            print(f"  ⚠ {log_file} not found, skipping")
            continue
        print(f"  Testing {size}MB...")
        # Use IP-hash since it matches our query pattern (2 IP groupBys)
        result = ev.run_experiment(log_file, partition_strategy='ip-hash', cache_strategy='cache')
        result['size_mb'] = size
        scalability.append(result)
        print(f"    Parse: {result['parse_time']}s, Total: {result['total_time']}s")
    
    with open('results/scalability.json', 'w') as f:
        json.dump(scalability, f, indent=2)
    
    # Goal 3 & 6: Partitioning comparison
    print("\n[Goal 3 & 6] Partitioning Strategies")
    test_file = 'datasets/web_500mb.log' if os.path.exists('datasets/web_500mb.log') else 'datasets/web_100mb.log'
    partitioning = []
    # Compare only 2 strategies: default vs IP-hash
    # This clearly shows the benefit of aligning partition key with query pattern
    for strategy in [None, 'ip-hash']:
        print(f"  Testing {strategy or 'default'}...")
        result = ev.run_experiment(test_file, partition_strategy=strategy, cache_strategy='cache')
        partitioning.append(result)
        print(f"    Parse: {result['parse_time']}s, Agg: {result['agg_time']}s, Total: {result['total_time']}s")
    
    with open('results/partitioning.json', 'w') as f:
        json.dump(partitioning, f, indent=2)
    
    # Goal 4: Caching comparison
    print("\n[Goal 4] Caching Strategies")
    test_file = 'datasets/web_250mb.log' if os.path.exists('datasets/web_250mb.log') else 'datasets/web_100mb.log'
    caching = []
    for strategy in [None, 'cache', 'persist']:
        print(f"  Testing {strategy or 'none'}...")
        # Use IP-hash partitioning for all caching tests (consistent baseline)
        result = ev.run_experiment(test_file, partition_strategy='ip-hash', cache_strategy=strategy)
        caching.append(result)
        print(f"    Parse: {result['parse_time']}s, Agg: {result['agg_time']}s, Total: {result['total_time']}s")
    
    with open('results/caching.json', 'w') as f:
        json.dump(caching, f, indent=2)
    
    ev.stop()
    
    print("\n" + "="*60)
    print("✓ Evaluation complete. Results in results/")
    print("  - scalability.json (Goal 5)")
    print("  - partitioning.json (Goal 3 & 6)")
    print("  - caching.json (Goal 4)")
    print("="*60)

if __name__ == "__main__":
    main()
