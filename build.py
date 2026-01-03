#!/usr/bin/env python3
"""Build script for Fractal Connector EXE."""
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    """Build the executable."""
    project_root = Path(__file__).parent
    os.chdir(project_root)

    print("=" * 50)
    print("Fractal Connector Build Script")
    print("=" * 50)

    # Check Python version
    if sys.version_info < (3, 10):
        print("Error: Python 3.10 or higher is required")
        sys.exit(1)

    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.system()}")

    # Install dependencies
    print("\n[1/3] Installing dependencies...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"])

    # Clean previous builds
    print("\n[2/3] Cleaning previous builds...")
    for folder in ['build', 'dist']:
        if (project_root / folder).exists():
            shutil.rmtree(project_root / folder)

    # Build with PyInstaller
    print("\n[3/3] Building executable...")
    result = subprocess.run([
        sys.executable, "-m", "PyInstaller",
        "--clean",
        "--noconfirm",
        "fractal-connector.spec"
    ])

    if result.returncode != 0:
        print("\nBuild failed!")
        sys.exit(1)

    # Show result
    exe_name = "FractalConnector.exe" if platform.system() == "Windows" else "FractalConnector"
    exe_path = project_root / "dist" / exe_name

    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print("\n" + "=" * 50)
        print("Build successful!")
        print(f"Executable: {exe_path}")
        print(f"Size: {size_mb:.1f} MB")
        print("=" * 50)
    else:
        print("\nBuild completed but executable not found")


if __name__ == "__main__":
    main()
