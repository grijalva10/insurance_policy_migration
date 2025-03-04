#!/usr/bin/env python3
"""
Initialize a Git repository for the insurance policy migration project
"""

import os
import subprocess
import sys

def run_command(command):
    """Run a shell command and return the output"""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error message: {e.stderr}")
        return None

def init_git_repo():
    """Initialize a Git repository for the project"""
    print("Initializing Git repository for insurance policy migration project...")
    
    # Check if Git is installed
    if not run_command("git --version"):
        print("Git is not installed. Please install Git and try again.")
        return False
    
    # Check if .git directory already exists
    if os.path.exists(".git"):
        print("Git repository already exists.")
        return True
    
    # Initialize Git repository
    if not run_command("git init"):
        print("Failed to initialize Git repository.")
        return False
    
    # Create .gitignore file
    gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
ENV/
env/
env.bak/
venv.bak/

# Environment variables
.env

# Logs
*.log

# Cache
.cache/

# Sensitive data
data/input/*.csv
data/reports/*.csv
"""
    
    with open(".gitignore", "w") as f:
        f.write(gitignore_content)
    
    print("Created .gitignore file.")
    
    # Add all files to Git
    if not run_command("git add ."):
        print("Failed to add files to Git.")
        return False
    
    # Create initial commit
    if not run_command('git commit -m "Initial commit"'):
        print("Failed to create initial commit.")
        return False
    
    print("Git repository initialized successfully.")
    print("\nTo add a remote repository, run:")
    print("  git remote add origin https://github.com/grijalva10/insurance_policy_migration.git")
    print("  git push -u origin master")
    
    return True

if __name__ == "__main__":
    init_git_repo() 