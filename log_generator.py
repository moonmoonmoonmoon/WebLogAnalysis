import random
from datetime import datetime, timedelta
import os


class SyntheticLogGenerator:
    """
    Generates synthetic Apache/Nginx-style web server logs with configurable parameters.
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
    
    def _generate_bytes(self, url):
        """Generate realistic byte size based on URL"""
        if '/api/' in url:
            return random.randint(100, 5000)
        elif '/static/' in url or '/images/' in url:
            return random.randint(5000, 100000)
        else:
            return random.randint(500, 3000)
    
    def _generate_log_entry(
        self,
        timestamp,
        ip=None,
        method=None,
        url=None, 
        status=None,
        is_error=False,
        is_slow=False
    ):
        """
        Generate a single log entry
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

        # Apache Combined Log Format with response time
        log_line = f'{ip} - - [{timestamp_str}] "{method} {url} HTTP/1.1" {status} {bytes_sent}'
        
        return log_line
    
    def generate_normal_traffic(
        self,
        output_file,
        num_requests=10000, 
        start_time=None,
        duration_hours=1
    ):
        """
        Generate normal web traffic logs
        """
        print(f"Generating normal traffic ({num_requests} requests over {duration_hours}h)")
        
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=duration_hours)
        
        time_increment = (duration_hours * 3600) / num_requests
        
        with open(output_file, 'w') as f:
            for i in range(num_requests):
                current_time = start_time + timedelta(seconds=i * time_increment)
                log_entry = self._generate_log_entry(current_time)
                f.write(log_entry + '\n')
                
                if (i + 1) % 1000 == 0:
                    print(f"Generated {i + 1} / {num_requests} entries.", end='\r')
        
        print(f"\nNormal traffic saved to: {output_file}")
    
    def generate_mixed_traffic(
        self,
        output_file,
        num_requests=15000,
        error_rate=0.8,
        start_time=None,
        duration_hours=2,
        attack_start_time=None,
        attack_duration_hours=0.333,
    ):
        """
        Generate mixed traffic with both normal and attack patterns
        """
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=duration_hours)
        
        # Generate attacker IPs
        num_attackers = random.randint(5, 15)
        attacker_ips = [self._generate_ip() for _ in range(num_attackers)]
        duration = duration_hours * 3600
        attack_duration = attack_duration_hours * 3600
        time_increment = duration / num_requests

        if attack_start_time is None:
            # attack_start_time = start_time + random.uniform(0, duration - attack_duration)
            attack_start_time = start_time + timedelta(
                seconds=random.uniform(0, duration - attack_duration))
        else:
            assert attack_start_time >= start_time and attack_start_time + attack_duration <= duration
        
        with open(output_file, 'w') as f:
            for i in range(num_requests):
                current_seconds = i * time_increment
                # current_time = start_time + timedelta(seconds=current_seconds)
                # is_attack = (attack_start_time <= current_seconds <= attack_start_time + attack_duration and random.random() <= error_rate)
                
                current_time = start_time + timedelta(seconds=current_seconds)

                is_attack = (
                    attack_start_time <= current_time <= attack_start_time + timedelta(seconds=attack_duration)
                        and random.random() <= error_rate
                )
                if is_attack:
                    ip = random.choice(attacker_ips)
                    url = random.choice(['/api/login', '/api/search', '/'])
                    log_entry = self._generate_log_entry(current_time, ip=ip, url=url, is_error=True, is_slow=True)
                else:
                    log_entry = self._generate_log_entry(current_time)
                
                f.write(log_entry + '\n')
                
                if (i + 1) % 1000 == 0:
                    print(f"Generated {i + 1} / {num_requests} entries.", end='\r')
        
        print(f"\nMixed traffic saved to: {output_file}")
        print(f"Attack window: {attack_start_time} - {attack_start_time + timedelta(seconds=attack_duration)}")
        print(f"Attacker IPs: {', '.join(attacker_ips[:5])}...")