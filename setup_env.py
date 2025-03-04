#!/usr/bin/env python3
"""
Set up environment variables for the insurance policy migration project
"""

import os
import sys
import argparse
import platform

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Set up environment variables for the insurance policy migration project")
    parser.add_argument("--github-token", help="GitHub API token")
    parser.add_argument("--ams-token", help="AMS API token")
    return parser.parse_args()

def create_env_file(github_token=None, ams_token=None):
    """Create a .env file with the provided tokens"""
    env_content = "# Environment variables for insurance policy migration\n\n"
    
    if github_token:
        env_content += f"GITHUB_TOKEN={github_token}\n"
    
    if ams_token:
        env_content += f"AMS_API_TOKEN={ams_token}\n"
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print("Created .env file with environment variables.")
    print("To load these variables, run:")
    
    if platform.system() == "Windows":
        print("  $env:GITHUB_TOKEN = (Get-Content .env | Select-String GITHUB_TOKEN).ToString().Split('=')[1]")
        print("  $env:AMS_API_TOKEN = (Get-Content .env | Select-String AMS_API_TOKEN).ToString().Split('=')[1]")
    else:
        print("  export $(grep -v '^#' .env | xargs)")
    
    return True

def main():
    """Main function"""
    args = parse_arguments()
    
    github_token = args.github_token
    ams_token = args.ams_token
    
    if not github_token and not ams_token:
        print("No tokens provided. Please provide at least one token.")
        return False
    
    return create_env_file(github_token, ams_token)

if __name__ == "__main__":
    main() 