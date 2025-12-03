"""
Visualization Module for Web Log Analysis

Bar chart:  1. Top-20 URLs by request count
            2. Top-20 IPs by request count
            3. parsing runtime vs file size (10MB, 50MB, 100MB)
Pie chart:  4. HTTP status code distribution
Table:      5. Top-10 anomalous IPs

"""

import os
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import seaborn as sns
import pandas as pd


class LogVisualizer:
    # Generate charts

    def __init__(self, output_dir: str = "charts"):
        """
        Initialize visualizer with output directory
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        sns.set_style("whitegrid")
        plt.rcParams["figure.figsize"] = (10, 6)
        plt.rcParams["font.size"] = 10


    def _to_pdf(self, obj) -> pd.DataFrame:
        """
        Return pandas DataFrame.
        """
        if hasattr(obj, "toPandas"):
            return obj.toPandas()
        if isinstance(obj, pd.DataFrame):
            return obj
        raise TypeError("Expected PySpark or pandas DataFrame.")

    def _head_like(self, obj, n):
        """
        Spark limit(n) / pandas head(n).
        """
        if hasattr(obj, "limit"):
            return obj.limit(n)
        if isinstance(obj, pd.DataFrame):
            return obj.head(n)
        return obj

    def plot_top_urls(self, urls_df, top_n: int = 20, output_file: str = "top_urls.png"):
        # Top-20 URLs by request count

        urls_df = self._head_like(urls_df, top_n)
        pdf = self._to_pdf(urls_df).copy()

        pdf["request_count"] = pd.to_numeric(pdf["request_count"], errors="coerce").fillna(0)
        pdf = pdf.sort_values("request_count", ascending=False).reset_index(drop=True)

        fig, ax = plt.subplots()
        bars = ax.barh(pdf["url"], pdf["request_count"])

        ax.set_xlabel("Request Count")
        ax.set_title(f"Top {top_n} URLs")
        for bar, count in zip(bars, pdf["request_count"]):
            ax.text(
                bar.get_width() + 1,
                bar.get_y() + bar.get_height() / 2,
                f"{int(count)}",
                va='center'
            )

        plt.tight_layout()
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300)
        plt.close()
        print(f"Chart saved: {output_path}")


    def plot_top_ips(self, ips_df, top_n: int = 20, output_file: str = "top_ips.png"):

        # Top-20 IPs by request count

        ips_df = self._head_like(ips_df, top_n)
        pdf = self._to_pdf(ips_df).copy()

        pdf["request_count"] = pd.to_numeric(pdf["request_count"], errors="coerce").fillna(0)
        pdf = pdf.sort_values("request_count", ascending=False).reset_index(drop=True)

        fig, ax = plt.subplots()
        bars = ax.barh(pdf["ip"], pdf["request_count"])

        ax.set_xlabel("Request Count")
        ax.set_title(f"Top {top_n} IPs")

        for bar, count in zip(bars, pdf["request_count"]):
            ax.text(
                bar.get_width() + 1,
                bar.get_y() + bar.get_height()/2,
                f"{int(count)}",
                va="center"
            )

        plt.tight_layout()
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300)
        plt.close()
        print(f"Chart saved: {output_path}")


    def plot_parsing_performance(self, file_sizes_mb, runtimes_sec, output_file: str = "parsing_runtime.png"):

        # Parsing runtime vs file size (10MB, 50MB, 100MB)

        fig, ax = plt.subplots()
        

        bars = ax.bar(file_sizes_mb, runtimes_sec)

        ax.set_xlabel("File Size (MB)")
        ax.set_ylabel("Parsing Time (s)")
        ax.set_title("Parsing Time vs File Size")
        for bar, runtime in zip(bars, runtimes_sec):
            ax.text(
                bar.get_x() + bar.get_width()/2,
                bar.get_height(),
                f"{runtime:.2f}s",
                ha='center',
                va='bottom'
            )

        plt.tight_layout()
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300)
        plt.close()
        print(f"Chart saved: {output_path}")


    def plot_status_distribution(self, status_df, output_file: str = "status_distribution.png"):
        
        # HTTP status code distribution

        pdf = self._to_pdf(status_df).copy()
        pdf["count"] = pd.to_numeric(pdf["count"], errors="coerce").fillna(0)
        pdf = pdf.sort_values("count", ascending=False).reset_index(drop=True)
        total = pdf["count"].sum()
        labels = []
        for _, row in pdf.iterrows():
            pct = (row["count"] / total * 100) if total > 0 else 0
            labels.append(f'HTTP {int(row["status"])} — {int(row["count"]):,} ({pct:.1f}%)')

        fig, ax = plt.subplots(figsize=(6, 6))
        wedges, _ = ax.pie(pdf["count"], startangle=90, labels=[None]*len(pdf))  
        ax.axis("equal")  

        ax.legend(wedges, labels, title="Status (count, %)", loc="center left", bbox_to_anchor=(1, 0.5))
        ax.set_title("HTTP Status Codes")

        plt.tight_layout()
        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Chart saved: {output_path}")



    def plot_anomalous_ips_table(self, anomaly_df, top_n: int = 10, output_file: str = "anomalous_ips_table.png"):
        
        # Table: top-10 anomalous IPs

        anomaly_df = self._head_like(anomaly_df, top_n)
        pdf = self._to_pdf(anomaly_df).copy()

        if len(pdf) == 0:
            print("No anomalous IPs found")
            return

        pdf["total_requests"] = pd.to_numeric(pdf["total_requests"], errors="coerce").fillna(0).astype(int)
        pdf["error_count"] = pd.to_numeric(pdf["error_count"], errors="coerce").fillna(0).astype(int)
        pdf["error_rate"] = pd.to_numeric(pdf["error_rate"], errors="coerce").fillna(0.0)

        pdf_fmt = pdf.copy()
        pdf_fmt["error_rate"] = pdf_fmt["error_rate"].apply(lambda x: f"{x*100:.1f}%")
        pdf_fmt["total_requests"] = pdf_fmt["total_requests"].apply(lambda x: f"{x:,}")
        pdf_fmt["error_count"] = pdf_fmt["error_count"].apply(lambda x: f"{x:,}")

        pdf_fmt = pdf_fmt.rename(
            columns={
                "ip": "IP Address",
                "total_requests": "Total Requests",
                "error_count": "Error Count",
                "error_rate": "Error Rate",
            }
        )

        fig, ax = plt.subplots(figsize=(12, len(pdf_fmt) * 0.5 + 2))
        ax.axis("tight")
        ax.axis("off")

        table = ax.table(
            cellText=pdf_fmt.values,
            colLabels=pdf_fmt.columns,
            cellLoc="left",
            loc="center",
            colWidths=[0.3, 0.3, 0.3, 0.3],
        )

        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)

        plt.title(f"Top {top_n} Anomalous IP Addresses", fontsize=14, weight="bold", pad=20)

        output_path = os.path.join(self.output_dir, output_file)
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
        print(f"Table saved: {output_path}")

def main():
    visualizer = LogVisualizer()
    import pandas as pd
    print("charts generated in /charts folder")


if __name__ == "__main__":
    main()
