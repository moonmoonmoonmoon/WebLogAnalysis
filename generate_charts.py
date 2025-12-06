"""
Visualization for Goals 5 & 6
CHANGES: 
1. Goal 5: Scalability line graph (runtime vs dataset size)
2. Goal 6: Partitioning comparison grouped bar chart
"""
import json
import matplotlib.pyplot as plt
import numpy as np
import os

def plot_scalability():
    """Goal 5: Scalability analysis"""
    with open('results/scalability.json', 'r') as f:
        data = json.load(f)
    
    sizes = [d['size_mb'] for d in data]
    parse = [d['parse_time'] for d in data]
    agg = [d['agg_time'] for d in data]
    total = [d['total_time'] for d in data]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Runtime vs size
    ax1.plot(sizes, parse, 'o-', label='Parse', linewidth=2, markersize=8)
    ax1.plot(sizes, agg, 's-', label='Aggregation', linewidth=2, markersize=8)
    ax1.plot(sizes, total, '^-', label='Total', linewidth=2, markersize=8)
    ax1.set_xlabel('Dataset Size (MB)', fontweight='bold')
    ax1.set_ylabel('Time (seconds)', fontweight='bold')
    ax1.set_title('Goal 5: Scalability Analysis', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Throughput
    throughput = [d['rows'] / d['parse_time'] / 1000 for d in data]
    ax2.plot(sizes, throughput, 'D-', linewidth=2, markersize=8, color='green')
    ax2.set_xlabel('Dataset Size (MB)', fontweight='bold')
    ax2.set_ylabel('Parse Rate (K records/sec)', fontweight='bold')
    ax2.set_title('Parse Throughput', fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('charts/goal5_scalability.png', dpi=300, bbox_inches='tight')
    print("Goal 5 chart: charts/goal5_scalability.png")

def plot_partitioning():
    """Goal 6: Partitioning comparison"""
    with open('results/partitioning.json', 'r') as f:
        data = json.load(f)
    
    strategies = [d['partition'] for d in data]
    parse = [d['parse_time'] for d in data]
    agg = [d['agg_time'] for d in data]
    total = [d['total_time'] for d in data]
    
    x = np.arange(len(strategies))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width, parse, width, label='Parse', color='#2E86AB')
    ax.bar(x, agg, width, label='Aggregation', color='#A23B72')
    ax.bar(x + width, total, width, label='Total', color='#F18F01')
    
    ax.set_xlabel('Partitioning Strategy', fontweight='bold')
    ax.set_ylabel('Time (seconds)', fontweight='bold')
    ax.set_title('Goal 6: Partitioning Strategy Comparison', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(strategies)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for i, (p, a, t) in enumerate(zip(parse, agg, total)):
        ax.text(i - width, p, f'{p:.1f}', ha='center', va='bottom', fontsize=9)
        ax.text(i, a, f'{a:.1f}', ha='center', va='bottom', fontsize=9)
        ax.text(i + width, t, f'{t:.1f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('charts/goal6_partitioning.png', dpi=300, bbox_inches='tight')
    print("Goal 6 chart: charts/goal6_partitioning.png")

def plot_caching():
    """Goal 4: Caching comparison"""
    with open('results/caching.json', 'r') as f:
        data = json.load(f)
    
    strategies = [d['cache'] for d in data]
    parse = [d['parse_time'] for d in data]
    agg = [d['agg_time'] for d in data]
    total = [d['total_time'] for d in data]
    
    x = np.arange(len(strategies))
    width = 0.25
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width, parse, width, label='Parse', color='#2E86AB')
    ax.bar(x, agg, width, label='Aggregation', color='#A23B72')
    ax.bar(x + width, total, width, label='Total', color='#F18F01')
    
    ax.set_xlabel('Caching Strategy', fontweight='bold')
    ax.set_ylabel('Time (seconds)', fontweight='bold')
    ax.set_title('Goal 4: Caching Strategy Comparison', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(strategies)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('charts/goal4_caching.png', dpi=300, bbox_inches='tight')
    print("Goal 4 chart: charts/goal4_caching.png")

if __name__ == "__main__":
    os.makedirs('charts', exist_ok=True)
    
    print("Generating charts...")
    plot_scalability()
    plot_partitioning()
    plot_caching()
    
    print("\nAll charts generated in charts/")
