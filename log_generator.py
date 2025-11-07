"""
Synthetic Web Log Generator
Generates realistic web server logs for testing and evaluation
"""

import random
from datetime import datetime, timedelta
import os


class SyntheticLogGenerator:
    """
    Generates synthetic Apache/Nginx-style web server logs with configurable parameters.
    Supports normal traffic and simulated anomalies (DDoS, error surges, etc.)
    """
    
    def __init__(self, seed=42):
        random.seed(seed)
        
        # Common URLs with realistic distribution
        self.urls = [
            ('/api/users', 0.15),
            ('/api/products', 0.12),
            ('/api/orders', 0.10),
            ('/api/search', 0.08),
            ('/api/login', 0.08),
            ('/api/logout', 0.05),
            ('/static/css/main.css', 0.07),
            ('/static/js/app.js', 0.07),
            ('/images/logo.png', 0.06),
            ('/favicon.ico', 0.05),
            ('/api/cart', 0.04),
            ('/api/checkout', 0.03),
            ('/api/admin', 0.02),
            ('/health', 0.04),
            ('/metrics', 0.02),
            ('/docs', 0.02),
        ]
        
        # HTTP methods distribution
        self.methods = [
            ('GET', 0.65),
            ('POST', 0.25),
            ('PUT', 0.05),
            ('DELETE', 0.03),
            ('HEAD', 0.02),
        ]
        
        # Status codes for normal traffic
        self.normal_status_codes = [
            (200, 0.85),
            (201, 0.05),
            (204, 0.03),
            (301, 0.02),
            (302, 0.02),
            (304, 0.01),
            (400, 0.01),
            (404, 0.005),
            (500, 0.005),
        ]
        
        # Status codes for attack/error scenarios
        self.error_status_codes = [
            (400, 0.20),
            (401, 0.15),
            (403, 0.15),
            (404, 0.20),
            (429, 0.10),
            (500, 0.10),
            (502, 0.05),
            (503, 0.05),
        ]
    
    def _weighted_choice(self, choices):
        """Select item based on weighted probability"""
        total = sum(w for c, w in choices)
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices:
            if upto + w >= r:
                return c
            upto += w
        return choices[-1][0]
    
    def _generate_ip(self, ip_pool=None):
        """Generate random IP address or select from pool"""
        if ip_pool:
            return random.choice(ip_pool)
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
    
    def _generate_timestamp(self, base_time):
        """Generate timestamp in Apache log format"""
        return base_time.strftime('%d/%b/%Y:%H:%M:%S +0000')
    
    def _generate_response_time(self, is_slow=False):
        """Generate realistic response time"""
        if is_slow:
            return round(random.uniform(2.0, 10.0), 3)
        else:
            return round(random.lognormvariate(mu=-2, sigma=1), 3)
    
    def _generate_bytes(self, url):
        """Generate realistic byte size based on URL"""
        if '/api/' in url:
            return random.randint(100, 5000)
        elif '/static/' in url or '/images/' in url:
            return random.randint(5000, 100000)
        else:
            return random.randint(500, 3000)
    
    def generate_log_entry(self, timestamp, ip=None, method=None, url=None, 
                          status=None, is_error=False, is_slow=False):
        """
        Generate a single log entry
        
        Args:
            timestamp (datetime): Timestamp for the log entry
            ip (str): IP address (generated if None)
            method (str): HTTP method (random if None)
            url (str): URL path (random if None)
            status (int): HTTP status code (random if None)
            is_error (bool): Use error status codes
            is_slow (bool): Generate slow response time
        
        Returns:
            str: Formatted log entry
        """
        ip = ip or self._generate_ip()
        method = method or self._weighted_choice(self.methods)
        url = url or self._weighted_choice(self.urls)
        
        if status is None:
            if is_error:
                status = self._weighted_choice(self.error_status_codes)
            else:
                status = self._weighted_choice(self.normal_status_codes)
        
        timestamp_str = self._generate_timestamp(timestamp)
        bytes_sent = self._generate_bytes(url)
        response_time = self._generate_response_time(is_slow)
        
        # Apache Combined Log Format with response time
        log_line = f'{ip} - - [{timestamp_str}] "{method} {url} HTTP/1.1" {status} {bytes_sent} {response_time}'
        
        return log_line
    
    def generate_normal_traffic(self, output_file, num_requests=10000, 
                               start_time=None, duration_hours=1):
        """
        Generate normal web traffic logs
        
        Args:
            output_file (str): Output file path
            num_requests (int): Number of requests to generate
            start_time (datetime): Start timestamp
            duration_hours (int): Time span in hours
        """
        print(f"\nGenerating normal traffic: {num_requests:,} requests over {duration_hours}h")
        
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=duration_hours)
        
        time_increment = (duration_hours * 3600) / num_requests
        
        with open(output_file, 'w') as f:
            for i in range(num_requests):
                current_time = start_time + timedelta(seconds=i * time_increment)
                log_entry = self.generate_log_entry(current_time)
                f.write(log_entry + '\n')
                
                if (i + 1) % 1000 == 0:
                    print(f"Generated {i+1:,}/{num_requests:,} entries.", end='\r')
        
        print(f"\nNormal traffic saved to: {output_file}")
    
    def generate_ddos_attack(self, output_file, attacker_ips, num_requests=5000,
                            start_time=None, duration_minutes=10):
        """
        Generate DDoS attack pattern logs
        
        Args:
            output_file (str): Output file path
            attacker_ips (list): List of attacker IP addresses
            num_requests (int): Number of attack requests
            start_time (datetime): Attack start time
            duration_minutes (int): Attack duration in minutes
        """
        print(f"\nGenerating DDoS attack: {num_requests:,} requests from {len(attacker_ips)} IPs")
        
        if start_time is None:
            start_time = datetime.now() - timedelta(minutes=duration_minutes)
        
        time_increment = (duration_minutes * 60) / num_requests
        
        with open(output_file, 'w') as f:
            for i in range(num_requests):
                current_time = start_time + timedelta(seconds=i * time_increment)
                attacker_ip = random.choice(attacker_ips)
                
                # DDoS typically hits a few endpoints repeatedly
                url = random.choice(['/api/login', '/api/search', '/api/products', '/'])
                
                log_entry = self.generate_log_entry(
                    current_time, 
                    ip=attacker_ip,
                    url=url,
                    is_error=random.random() > 0.3,  # 70% error rate
                    is_slow=True
                )
                f.write(log_entry + '\n')
        
        print(f"DDoS attack logs saved to: {output_file}")
    
    def generate_mixed_traffic(self, output_file, total_requests=15000,
                              attack_ratio=0.3, start_time=None):
        """
        Generate mixed traffic with both normal and attack patterns
        
        Args:
            output_file (str): Output file path
            total_requests (int): Total number of requests
            attack_ratio (float): Ratio of attack traffic (0-1)
            start_time (datetime): Start timestamp
        """
        print(f"\nGenerating mixed traffic: {total_requests:,} total requests")
        print(f"Normal traffic: {int(total_requests * (1-attack_ratio)):,} requests")
        print(f"Attack traffic: {int(total_requests * attack_ratio):,} requests")
        
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=2)
        
        # Generate attacker IPs
        num_attackers = random.randint(5, 15)
        attacker_ips = [self._generate_ip() for _ in range(num_attackers)]
        
        normal_count = int(total_requests * (1 - attack_ratio))
        attack_count = total_requests - normal_count
        
        time_increment = 7200 / total_requests  # 2 hours in seconds
        
        # Create attack windows
        attack_start = random.uniform(1800, 5400)  # Start between 30min and 1.5h
        attack_duration = 600  # 10 minutes
        
        with open(output_file, 'w') as f:
            for i in range(total_requests):
                current_seconds = i * time_increment
                current_time = start_time + timedelta(seconds=current_seconds)
                
                # Determine if this request is part of attack
                is_attack = (attack_start <= current_seconds <= attack_start + attack_duration 
                           and random.random() < 0.7)
                
                if is_attack:
                    ip = random.choice(attacker_ips)
                    url = random.choice(['/api/login', '/api/search', '/'])
                    log_entry = self.generate_log_entry(
                        current_time, ip=ip, url=url, 
                        is_error=True, is_slow=True
                    )
                else:
                    log_entry = self.generate_log_entry(current_time)
                
                f.write(log_entry + '\n')
                
                if (i + 1) % 1000 == 0:
                    print(f"Generated {i+1:,}/{total_requests:,} entries.", end='\r')
        
        print(f"\nMixed traffic saved to: {output_file}")
        print(f"Attack window: {attack_start:.0f}s - {attack_start + attack_duration:.0f}s")
        print(f"Attacker IPs: {', '.join(attacker_ips[:5])}...")


def main():
    """Generate sample datasets for milestone demonstration"""
    print("""Synthetic Web Log Generator. Creating sample datasets for evaluation """)
    
    # Create output directory
    os.makedirs('sample_logs', exist_ok=True)
    
    generator = SyntheticLogGenerator(seed=42)
    
    # Generate 10MB dataset for milestone
    print("\n[Dataset 1] Small dataset (10MB) for milestone testing")
    generator.generate_normal_traffic(
        'sample_logs/web_10mb.log',
        num_requests=50000,
        duration_hours=1
    )
    
    # Generate mixed traffic with attack patterns
    print("\n[Dataset 2] Mixed traffic with DDoS simulation")
    generator.generate_mixed_traffic(
        'sample_logs/web_mixed.log',
        total_requests=30000,
        attack_ratio=0.3
    )
    
    print("\n" + "-"*60)
    print("All sample datasets generated successfully!")
    print("-"*60)
    print("\nGenerated files:")
    for f in os.listdir('sample_logs'):
        size = os.path.getsize(f'sample_logs/{f}') / (1024 * 1024)
        print(f"- sample_logs/{f} ({size:.2f} MB)")


if __name__ == "__main__":
    main()
