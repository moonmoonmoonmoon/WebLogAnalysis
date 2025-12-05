# run_all.py - master pipeline script
# Usage: python run_all.py [full|eval|viz]

import sys
import subprocess
import argparse

def run(script):
    print(f"\n>>> Running {script}...")
    result = subprocess.run([sys.executable, script])
    if result.returncode != 0:
        print(f"ERROR: {script} failed")
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description='Run all scripts')
    parser.add_argument('--mode', type=str, default='full', help='Mode: full, eval, viz')
    args = parser.parse_args()
    mode = args.mode
    
    scripts = []
    if mode == 'full':
        scripts = ['gen_large_logs.py', 'run_evaluation.py', 'generate_charts.py']
    elif mode == 'eval':
        scripts = ['run_evaluation.py', 'generate_charts.py']
    elif mode == 'viz':
        scripts = ['generate_charts.py']
    else:
        print(f"Unknown mode: {mode}. Use: full, eval, or viz")
        return 1
    
    for script in scripts:
        if not run(script):
            return 1
    
    print("\nDone! Check datasets/, results/, and charts/")
    return 0

if __name__ == "__main__":
    sys.exit(main())