#!/usr/bin/env python3
"""
Build script to prepare Lambda deployment package with dependencies.
"""

import os
import subprocess
import shutil
import tempfile
from pathlib import Path

def build_lambda_package():
    """Build Lambda package with dependencies installed."""
    
    # Paths
    lambda_dir = Path("./lambda")
    build_dir = Path("./lambda_build")
    
    # Clean and create build directory
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir()
    
    # Copy source files
    for file in lambda_dir.glob("*.py"):
        shutil.copy2(file, build_dir)
    
    # Copy requirements.txt if it exists
    requirements_file = lambda_dir / "requirements.txt"
    if requirements_file.exists():
        shutil.copy2(requirements_file, build_dir)
        
        # Install dependencies
        subprocess.run([
            "pip", "install", 
            "-r", str(build_dir / "requirements.txt"),
            "-t", str(build_dir),
            "--no-deps"  # Don't install dependencies of dependencies
        ], check=True)
        
        # Remove requirements.txt from build directory
        (build_dir / "requirements.txt").unlink()
    
    print(f"Lambda package built in {build_dir}")
    return str(build_dir)

if __name__ == "__main__":
    build_lambda_package()
