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
import argparse

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
        
        if partition_strategy == 'ip-hash':
            df = df.repartition(8, 'ip')
        else:
            df = df.repartition(8)
        
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

# Main evaluation for Goals 3, 4, 5, 6
def main():
    parser = argparse.ArgumentParser(description='Run evaluation')
    parser.add_argument('--sizes', type=int, nargs='+', default=[100, 250, 500, 1000], help='Sizes in MB')
    parser.add_argument('--partition_file', type=str, default="datasets/web_1000mb.log", help='Partition Strategy test file.')
    parser.add_argument('--caching_file', type=str, default="datasets/web_1000mb.log", help='Caching Strategy test file.')
    args = parser.parse_args()
    sizes = args.sizes

    os.makedirs('results', exist_ok=True)
    ev = Evaluator()
    
    # Scalability test
    print("\n[Goal 5] Scalability analysis...")
    scalability = []
    for size in sizes:
        log_file = f'datasets/web_{size}mb.log'
        if not os.path.exists(log_file):
            print(f"  Skipping {log_file} (not found)")
            continue
        print(f"  {size}MB: ", end='', flush=True)
        result = ev.run_experiment(log_file, partition_strategy='ip-hash', cache_strategy='cache')
        result['size_mb'] = size
        scalability.append(result)
        print(f"Parse: {result['parse_time']}s, Total: {result['total_time']}s")
    
    with open('results/scalability.json', 'w') as f:
        json.dump(scalability, f, indent=2)
    
    # Partitioning comparison
    print("\n[Goal 3 & 6] Partitioning Strategies...")
    test_file = args.partition_file
    partitioning = []
    for strategy in [None, 'ip-hash']:
        name = strategy or 'default'
        print(f"  {name}: ", end='', flush=True)
        result = ev.run_experiment(test_file, partition_strategy=strategy, cache_strategy='cache')
        partitioning.append(result)
        print(f"Parse: {result['parse_time']}s, Agg: {result['agg_time']}s, Total: {result['total_time']}s")
    
    with open('results/partitioning.json', 'w') as f:
        json.dump(partitioning, f, indent=2)
    
    # Caching comparison
    print("\n[Goal 4] Caching strategies...")
    test_file = args.caching_file
    caching = []
    for strategy in [None, 'cache', 'persist']:
        name = strategy or 'none'
        print(f"  {name}: ", end='', flush=True)
        result = ev.run_experiment(test_file, partition_strategy='ip-hash', cache_strategy=strategy)
        caching.append(result)
        print(f"Parse: {result['parse_time']}s, Agg: {result['agg_time']}s, Total: {result['total_time']}s")
    
    with open('results/caching.json', 'w') as f:
        json.dump(caching, f, indent=2)
    
    ev.stop()
    print("\nDone. Results saved to results/")

if __name__ == "__main__":
    main()
