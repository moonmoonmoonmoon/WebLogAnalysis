"""
Large-scale log generator for final project
CHANGES: Added support for 100MB-1GB datasets with multiple attack periods
"""
import random
from datetime import datetime, timedelta
import os
import argparse

class LogGenerator:
    def __init__(self, seed=42):
        random.seed(seed)
        self.urls = [('/api/users', 0.15), ('/api/products', 0.12), ('/api/orders', 0.10),
                     ('/api/search', 0.08), ('/api/login', 0.08), ('/static/css/main.css', 0.07)]
        self.methods = [('GET', 0.65), ('POST', 0.25), ('PUT', 0.05)]
        self.normal_codes = [(200, 0.85), (201, 0.05), (404, 0.05), (500, 0.05)]
        self.error_codes = [(400, 0.25), (403, 0.25), (404, 0.25), (500, 0.25)]
    
    def _weighted_choice(self, choices):
        total = sum(w for c, w in choices)
        r = random.uniform(0, total)
        upto = 0
        for c, w in choices:
            if upto + w >= r:
                return c
            upto += w
        return choices[-1][0]
    
    def _gen_ip(self, pool=None):
        return random.choice(pool) if pool else f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,255)}"
    
    def _gen_log(self, timestamp, ip=None, is_attack=False):
        ip = ip or self._gen_ip()
        method = self._weighted_choice(self.methods)
        url = self._weighted_choice(self.urls)
        status = self._weighted_choice(self.error_codes if is_attack else self.normal_codes)
        bytes_sent = random.randint(100, 5000)
        ts = timestamp.strftime('%d/%b/%Y:%H:%M:%S +0000')
        return f'{ip} - - [{ts}] "{method} {url} HTTP/1.1" {status} {bytes_sent}'
    
    # CHANGE: New method for large datasets
    def generate_dataset(self, output_file, target_mb=100, duration_hrs=24, num_attacks=2):
        """Generate dataset with target size and attack patterns"""
        print(f"Generating {target_mb}MB dataset...")

        num_requests = int((target_mb * 1024 * 1024) / 85)  # ~85 bytes per log
        start_time = datetime.now() - timedelta(hours=duration_hrs)
        time_inc = (duration_hrs * 3600) / num_requests
        
        # Create attack windows
        attack_duration = 0.5 * 3600  # 30 min attacks
        attack_windows = []
        for i in range(num_attacks):
            offset = (i + 1) * (duration_hrs * 3600) / (num_attacks + 1)
            att_start = start_time + timedelta(seconds=offset)
            att_end = att_start + timedelta(seconds=attack_duration)
            attack_windows.append((att_start, att_end))
        
        attacker_ips = [self._gen_ip() for _ in range(10)]
        
        with open(output_file, 'w') as f:
            for i in range(num_requests):
                current_time = start_time + timedelta(seconds=i * time_inc)
                
                # Check if in attack window
                is_attack = any(start <= current_time <= end for start, end in attack_windows)
                is_attack = is_attack and random.random() < 0.8
                
                if is_attack:
                    log = self._gen_log(current_time, ip=random.choice(attacker_ips), is_attack=True)
                else:
                    log = self._gen_log(current_time)
                
                f.write(log + '\n')
                
                if (i + 1) % 10000 == 0:
                    print(f"  {(i+1)/num_requests*100:.1f}%", end='\r')
        
        actual_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"\n  ✓ Generated {output_file}: {actual_mb:.1f}MB ({num_requests:,} records)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate large logs')
    parser.add_argument('--sizes', type=int, nargs='+', default=[100, 250, 500, 1000], help='Sizes in MB')
    args = parser.parse_args()
    sizes = args.sizes

    os.makedirs('datasets', exist_ok=True)
    gen = LogGenerator()
    
    # Generate datasets for Goal 1 and Goal 5
    for size in sizes:
        gen.generate_dataset(
            f'datasets/web_{size}mb.log',
            target_mb=size,
            duration_hrs=24 * (size // 100),
            num_attacks=2 + (size // 250)
        )
    
    print("\n✓ All datasets generated in datasets/")
