"""
Script to push the project to GitHub.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add the src directory to the Python path
src_dir = Path(__file__).parent.parent / 'src'
sys.path.append(str(src_dir))

from insurance_migration.github_sync import GitHubSync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('github_sync')

def validate_token(token: str) -> bool:
    """Validate GitHub token format."""
    if not token:
        return False
    if not token.startswith('ghp_'):
        return False
    if len(token) < 30:  # Tokens are usually longer
        return False
    return True

def main():
    """Push the project to GitHub."""
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent / '.env'
    if not env_path.exists():
        logger.error(".env file not found. Please create one with GITHUB_TOKEN and GITHUB_USERNAME.")
        sys.exit(1)
    
    load_dotenv(env_path)
    
    # Check GitHub configuration
    github_token = os.environ.get('GITHUB_TOKEN', '')
    github_username = os.environ.get('GITHUB_USERNAME', '')
    
    if not github_username:
        logger.error("GitHub username not found in .env file. Please add GITHUB_USERNAME=your_username")
        sys.exit(1)
    
    if not github_token:
        logger.error("GitHub token not found in .env file. Please add GITHUB_TOKEN=your_token")
        sys.exit(1)
    
    if not validate_token(github_token):
        logger.error("Invalid GitHub token format. Token should start with 'ghp_' and be at least 30 characters long")
        sys.exit(1)
    
    logger.info(f"Using GitHub configuration:")
    logger.info(f"  Username: {github_username}")
    logger.info(f"  Token: {github_token[:4]}...{github_token[-4:]}")
    logger.info(f"  Repository: {os.environ.get('GITHUB_REPO', 'insurance_policy_migration')}")
    
    # Initialize GitHub sync
    github_sync = GitHubSync.from_env()
    if not github_sync:
        logger.error("Failed to initialize GitHub sync")
        sys.exit(1)
    
    # Get project root directory
    project_root = Path(__file__).parent.parent
    
    # Push to GitHub
    logger.info("Pushing project to GitHub...")
    success = github_sync.push_to_github(project_root)
    
    if success:
        logger.info("Successfully pushed project to GitHub")
    else:
        logger.error("Failed to push project to GitHub")
        sys.exit(1)

if __name__ == '__main__':
    main() 