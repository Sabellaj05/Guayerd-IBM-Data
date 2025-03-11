#!/usr/bin/env python3
"""
Main orchestration script for running the full ETL pipeline.
This script runs all the necessary scripts in the correct order to process data and load it into the database.

Order of execution:
1. merge_data.py
2. clean_donantes.py
3. clean_proveedores.py
4. db-ingestion.py
"""

import os
import sys
import importlib.util
import time
from pathlib import Path

def import_and_execute_script(script_path):
    """Import a Python script as a module and execute its main function if available.
    
    Args:
        script_path: Path to the script to import and execute
        
    Returns:
        The imported module
    """
    script_name = os.path.basename(script_path).replace('.py', '')
    spec = importlib.util.spec_from_file_location(script_name, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[script_name] = module
    spec.loader.exec_module(module)
    
    # If the module has a main function, execute it
    if hasattr(module, 'main'):
        module.main()
    
    return module

def run_script(script_path):
    """Run a Python script and print status information.
    
    Args:
        script_path: Path to the script to run
    """
    script_name = os.path.basename(script_path)
    print(f"\n{'=' * 80}")
    print(f"Running {script_name}...")
    print(f"{'=' * 80}")
    
    start_time = time.time()
    
    # Try to import and run the script
    try:
        # Save the original sys.argv and restore it after execution
        original_argv = sys.argv.copy()
        sys.argv = [script_path]
        
        # Import and execute the script
        import_and_execute_script(script_path)
        
        # Restore the original sys.argv
        sys.argv = original_argv
        
        execution_time = time.time() - start_time
        print(f"\n✅ {script_name} completed successfully in {execution_time:.2f} seconds")
    except Exception as e:
        print(f"\n❌ Error running {script_name}: {str(e)}")
        sys.exit(1)

def main():
    """Main function to run all scripts in the correct order."""
    # Get the current directory (src)
    src_dir = Path(__file__).parent.absolute()
    pipeline_dir = src_dir / "pipeline"
    
    # Script execution order - merge_data.py first, then cleaning scripts, then db-ingestion.py
    scripts = [
        pipeline_dir / "merge_data.py",
        pipeline_dir / "clean_donantes.py",
        pipeline_dir / "clean_proveedores.py",
        src_dir / "db-ingestion.py"
    ]
    
    print("\n🚀 Starting ETL pipeline")
    start_time = time.time()
    
    # Run each script in order
    for script in scripts:
        if not script.exists():
            print(f"\n❌ Script not found: {script}")
            sys.exit(1)
        run_script(str(script))
    
    total_time = time.time() - start_time
    print(f"\n✨ ETL pipeline completed successfully in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
