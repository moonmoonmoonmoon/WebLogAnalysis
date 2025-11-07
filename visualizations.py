"""
Visualization Module for Web Log Analysis
Generates evaluation charts for milestone and final report
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os
from datetime import datetime


class LogVisualizer:
    """
    Creates professional visualizations for web log analysis results.
    Generates charts required for milestone and final project evaluation.
    """
    
    def __init__(self, output_dir='charts'):
        """
        Initialize visualizer with output directory
        
        Args:
            output_dir (str): Directory to save generated charts
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set professional style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (10, 6)
        plt.rcParams['font.size'] = 10
        
        print(f"Visualizer initialized, charts will be saved to: {output_dir}/")
    
    def plot_status_distribution(self, status_df, output_file='status_distribution.png'):
        """
        Create pie chart showing HTTP status code distribution
        
        Args:
            status_df: PySpark DataFrame with columns [status, count]
            output_file (str): Output filename
        """
        print(f"\nGenerating status code distribution chart.")
        
        # Convert to pandas for plotting
        pdf = status_df.toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Create pie chart
        colors = sns.color_palette('Set3', n_colors=len(pdf))
        wedges, texts, autotexts = ax.pie(
            pdf['count'], 
            labels=[f'{int(s)}' for s in pdf['status']],
            autopct='%1.1f%%',
            colors=colors,
            startangle=90
        )
        
        # Enhance text
        for text in texts:
            text.set_fontsize(12)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(10)
            autotext.set_weight('bold')
        
        ax.set_title('HTTP Status Code Distribution', fontsize=14, weight='bold', pad=20)
        
        # Add legend
        legend_labels = [f'HTTP {int(row["status"])}: {row["count"]:,} requests' 
                        for _, row in pdf.iterrows()]
        ax.legend(legend_labels, loc='center left', bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.tight_layout()
        
        # Save
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Chart saved: {output_path}")
    
    def plot_top_urls(self, urls_df, top_n=20, output_file='top_urls.png'):
        """
        Create horizontal bar chart showing top URLs by request count
        
        Args:
            urls_df: PySpark DataFrame with columns [url, request_count]
            top_n (int): Number of top URLs to show
            output_file (str): Output filename
        """
        print(f"\nGenerating top-{top_n} URLs chart...")
        
        # Convert to pandas
        pdf = urls_df.limit(top_n).toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create horizontal bar chart
        bars = ax.barh(range(len(pdf)), pdf['request_count'], color=sns.color_palette('viridis', len(pdf)))
        
        # Set y-axis labels
        ax.set_yticks(range(len(pdf)))
        ax.set_yticklabels(pdf['url'], fontsize=9)
        ax.invert_yaxis()  # Highest at top
        
        # Labels and title
        ax.set_xlabel('Request Count', fontsize=12, weight='bold')
        ax.set_title(f'Top {top_n} URLs by Request Count', fontsize=14, weight='bold', pad=20)
        
        # Add value labels on bars
        for i, (bar, count) in enumerate(zip(bars, pdf['request_count'])):
            width = bar.get_width()
            ax.text(width + max(pdf['request_count'])*0.01, bar.get_y() + bar.get_height()/2,
                   f'{int(count):,}', ha='left', va='center', fontsize=9)
        
        # Grid
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        # Save
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Chart saved: {output_path}")
    
    def plot_top_ips(self, ips_df, top_n=20, output_file='top_ips.png'):
        """
        Create horizontal bar chart showing top IPs by request count
        
        Args:
            ips_df: PySpark DataFrame with columns [ip, request_count]
            top_n (int): Number of top IPs to show
            output_file (str): Output filename
        """
        print(f"\nGenerating top-{top_n} IPs chart.")
        
        # Convert to pandas
        pdf = ips_df.limit(top_n).toPandas()
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Color code: highlight potential anomalies (>1000 requests)
        colors = ['#d62728' if c > 1000 else '#2ca02c' for c in pdf['request_count']]
        
        # Create horizontal bar chart
        bars = ax.barh(range(len(pdf)), pdf['request_count'], color=colors)
        
        # Set y-axis labels
        ax.set_yticks(range(len(pdf)))
        ax.set_yticklabels(pdf['ip'], fontsize=9, family='monospace')
        ax.invert_yaxis()
        
        # Labels and title
        ax.set_xlabel('Request Count', fontsize=12, weight='bold')
        ax.set_title(f'Top {top_n} IP Addresses by Request Count', fontsize=14, weight='bold', pad=20)
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars, pdf['request_count'])):
            width = bar.get_width()
            ax.text(width + max(pdf['request_count'])*0.01, bar.get_y() + bar.get_height()/2,
                   f'{int(count):,}', ha='left', va='center', fontsize=9)
        
        # Legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#2ca02c', label='Normal Traffic'),
            Patch(facecolor='#d62728', label='High Volume (>1000 req)')
        ]
        ax.legend(handles=legend_elements, loc='lower right')
        
        # Grid
        ax.grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        # Save
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Chart saved: {output_path}")
    
    def plot_anomalous_ips_table(self, anomaly_df, top_n=10, output_file='anomalous_ips_table.png'):
        """
        Create formatted table showing anomalous IPs with statistics
        
        Args:
            anomaly_df: PySpark DataFrame with [ip, total_requests, error_count, error_rate]
            top_n (int): Number of IPs to display
            output_file (str): Output filename
        """
        print(f"\nGenerating anomalous IPs table.")
        
        # Convert to pandas
        pdf = anomaly_df.limit(top_n).toPandas()
        
        if len(pdf) == 0:
            print("  No anomalous IPs found - skipping table generation")
            return
        
        # Format the data
        pdf['error_rate'] = pdf['error_rate'].apply(lambda x: f'{x*100:.1f}%')
        pdf['total_requests'] = pdf['total_requests'].apply(lambda x: f'{x:,}')
        pdf['error_count'] = pdf['error_count'].apply(lambda x: f'{x:,}')
        
        # Rename columns for display
        pdf = pdf.rename(columns={
            'ip': 'IP Address',
            'total_requests': 'Total Requests',
            'error_count': 'Error Count',
            'error_rate': 'Error Rate'
        })
        
        # Create figure and table
        fig, ax = plt.subplots(figsize=(12, len(pdf)*0.5 + 2))
        ax.axis('tight')
        ax.axis('off')
        
        # Create table
        table = ax.table(
            cellText=pdf.values,
            colLabels=pdf.columns,
            cellLoc='left',
            loc='center',
            colWidths=[0.3, 0.25, 0.25, 0.2]
        )
        
        # Style the table
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Header styling
        for i in range(len(pdf.columns)):
            cell = table[(0, i)]
            cell.set_facecolor('#4CAF50')
            cell.set_text_props(weight='bold', color='white')
        
        # Alternate row colors
        for i in range(1, len(pdf) + 1):
            for j in range(len(pdf.columns)):
                cell = table[(i, j)]
                if i % 2 == 0:
                    cell.set_facecolor('#f0f0f0')
        
        plt.title(f'Top {top_n} Anomalous IP Addresses', 
                 fontsize=14, weight='bold', pad=20)
        
        # Save
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Table saved: {output_path}")
    
    def plot_parsing_performance(self, file_sizes_mb, runtimes_sec, output_file='parsing_runtime.png'):
        """
        Create bar chart showing parsing runtime vs file size
        
        Args:
            file_sizes_mb (list): File sizes in MB
            runtimes_sec (list): Runtime in seconds
            output_file (str): Output filename
        """
        print(f"\nGenerating parsing performance chart...")
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create bar chart
        bars = ax.bar(range(len(file_sizes_mb)), runtimes_sec, 
                      color=sns.color_palette('coolwarm', len(file_sizes_mb)))
        
        # Labels
        ax.set_xlabel('File Size (MB)', fontsize=12, weight='bold')
        ax.set_ylabel('Parsing Runtime (seconds)', fontsize=12, weight='bold')
        ax.set_title('Log Parsing Performance vs File Size', fontsize=14, weight='bold', pad=20)
        ax.set_xticks(range(len(file_sizes_mb)))
        ax.set_xticklabels([f'{int(s)} MB' for s in file_sizes_mb])
        
        # Add value labels on bars
        for bar, runtime in zip(bars, runtimes_sec):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, height + max(runtimes_sec)*0.02,
                   f'{runtime:.2f}s', ha='center', va='bottom', fontsize=10, weight='bold')
        
        # Grid
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        # Save
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Chart saved: {output_path}")


def main():
    """Demo visualization generation"""
    print("""
        Log Analysis Visualization Module                          
        Generating sample charts for milestone
    """)
    
    visualizer = LogVisualizer()
    
    # Demo: Create sample performance chart
    print("\nGenerating sample performance chart.")
    file_sizes = [10, 50, 100]
    runtimes = [2.3, 8.5, 15.2]
    visualizer.plot_parsing_performance(file_sizes, runtimes)
    
    print("\n Visualization module ready for use!")


if __name__ == "__main__":
    main()
