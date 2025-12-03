"""
Master script - Run complete pipeline
Usage: python run_all.py [mode]
  mode = full  : Generate + Evaluate + Visualize (default)
  mode = eval  : Evaluate + Visualize (skip generation)
  mode = viz   : Visualize only
"""
import sys
import subprocess
import os

def run(script):
    """Run a Python script"""
    print(f"\n{'='*60}\nRunning: {script}\n{'='*60}")
    result = subprocess.run([sys.executable, script])
    return result.returncode == 0

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'full'
    
    print("="*60)
    print("FINAL PROJECT PIPELINE")
    print("="*60)
    
    if mode == 'full':
        print("\nMode: Full pipeline (generate + evaluate + visualize)")
        if not run('gen_large_logs.py'):
            print("✗ Generation failed")
            return 1
        if not run('run_evaluation.py'):
            print("✗ Evaluation failed")
            return 1
        if not run('generate_charts.py'):
            print("✗ Visualization failed")
            return 1
    
    elif mode == 'eval':
        print("\nMode: Evaluation + Visualization")
        if not run('run_evaluation.py'):
            print("✗ Evaluation failed")
            return 1
        if not run('generate_charts.py'):
            print("✗ Visualization failed")
            return 1
    
    elif mode == 'viz':
        print("\nMode: Visualization only")
        if not run('generate_charts.py'):
            print("✗ Visualization failed")
            return 1
    
    else:
        print(f"Unknown mode: {mode}")
        print("Use: full, eval, or viz")
        return 1
    
    print("\n" + "="*60)
    print("✓ PIPELINE COMPLETE")
    print("="*60)
    print("\nCheck results:")
    print("  datasets/  - Log files")
    print("  results/   - JSON data")
    print("  charts/    - PNG charts")
    return 0

if __name__ == "__main__":
    sys.exit(main())