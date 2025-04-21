#!/usr/bin/env python3
"""
Main orchestration script for running the full ETL pipeline.
This script runs all the necessary scripts in the correct order to process data and load it into the database.

Order of execution:
1. merge_data.py - Combina los datos crudos de distintos periodos
2. clean_donantes.py - Limpia y procesa los datos de donantes
3. clean_proveedores.py - Limpia y procesa los datos de los proveedores
4. db-ingestion.py - Carga los datos limpios acorde al schema de la db previamente creada (_DATABASE/ddl.sql)

Usage:
  python main.py [--prod]

Options:
  --prod: Use production environment (otherwise uses local environment)
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

def parse_arguments():
    """Parse command line arguments for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Run the ETL pipeline")
    parser.add_argument("--prod", action="store_true", help="Use production environment (otherwise uses local environment)")
    return parser.parse_args()

def setup_environment(use_prod):
    """Set up the environment by checking appropriate .env files exist.
    
    Args:
        use_prod: Boolean indicating whether to use production environment
    
    Returns:
        None
    """
    repo_root = Path(__file__).resolve().parent.parent
    src_dir = repo_root / "src"
    
    # Define .env file paths
    env_example = src_dir / ".env.example"  # Local environment template
    env_file = src_dir / ".env"            # Production environment file
    
    # Just check that the appropriate file exists
    if use_prod:
        if not env_file.exists():
            print(f"❌ Production environment file {env_file} not found")
            sys.exit(1)

        print(f"✅ Using production environment (.env)")

    else:
        # For local mode, we just need .env.example to exist
        # The db-ingestion.py script will handle loading it
        if not env_example.exists():
            print(f"❌ Local environment file {env_example} not found")
            sys.exit(1)
        print(f"✅ Using local environment (.env.example)")
        
        # We need to set an environment variable to tell scripts to use .env.example (local)
        os.environ["USE_ENV_EXAMPLE"] = "1"

def run_script(script_path):
    """Run a Python script as a subprocess and handle its output.
    
    This function executes another Python script as a separate process
    and captures its output and errors. This approach allows each script
    to run independently with its own environment.
    
    Args:
        script_path: Path to the Python script to run
    
    Returns:
        The return code from the script execution (0 for success)
    """
    script_name = os.path.basename(script_path)
    print(f"\n{'=' * 80}")
    print(f"Running {script_name}...")
    print(f"{'=' * 80}")
    
    start_time = time.time()
    
    # Build the command - run the script with the same Python interpreter
    cmd = [sys.executable, script_path]
    
    # Run the script as a subprocess
    try:
        # subprocess.run executes the command and waits for it to complete
        process = subprocess.run(
            cmd,
            check=True,
            text=True,
            capture_output=True,
            env=os.environ.copy()  # Pass current environment variables to subprocess
        )
        
        # Print output from the script
        if process.stdout:
            print(process.stdout)
        
        execution_time = time.time() - start_time
        print(f"\n✅ {script_name} completed successfully in {execution_time:.2f} seconds")
        return 0
    
    except subprocess.CalledProcessError as e:
        # This exception is raised when the command returns a non-zero exit code
        print(f"\n❌ Error running {script_name} (exit code {e.returncode}):")
        if e.stdout:
            print(e.stdout)
        if e.stderr:
            print(f"Error output:\n{e.stderr}")
        return e.returncode

def main():
    """Main function to run all scripts in the correct order."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up environment based on arguments
    use_prod = args.prod
    setup_environment(use_prod)
    
    # Get the current directory (src)
    src_dir = Path(__file__).parent.absolute()
    pipeline_dir = src_dir / "pipeline"
    
    # Prepare script paths
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
        script_path = str(script)
        if not script.exists():
            print(f"\n❌ Script not found: {script_path}")
            sys.exit(1)
        
        result = run_script(script_path)
        
        # Stop the pipeline if any script fails
        if result != 0:
            print(f"\n❌ ETL pipeline failed at {os.path.basename(script_path)}")
            sys.exit(result)
    
    total_time = time.time() - start_time
    print(f"\n✨ ETL pipeline completed successfully in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
