"""
GitHub synchronization functionality for insurance policy migration.
Handles pushing updates to GitHub repository.
"""

import os
import base64
import logging
import requests
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

# Configure logger
logger = logging.getLogger('processing')

class GitHubSync:
    def __init__(self, username: str, token: str, repo_name: str):
        """Initialize GitHub sync with credentials."""
        self.username = username
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.repo_name = repo_name
        self.api_url = "https://api.github.com"
    
    def ensure_repository(self) -> bool:
        """Ensure repository exists, create if needed."""
        resp = requests.get(
            f"{self.api_url}/repos/{self.username}/{self.repo_name}",
            headers=self.headers
        )
        
        if resp.status_code == 404:
            resp = requests.post(
                f"{self.api_url}/user/repos",
                headers=self.headers,
                json={"name": self.repo_name, "private": False}
            )
            if resp.status_code not in {200, 201}:
                logger.error(f"Failed to create repository: {resp.status_code}")
                return False
            logger.info(f"Created repository: {self.repo_name}")
        elif resp.status_code != 200:
            logger.error(f"Failed to check repository: {resp.status_code}")
            return False
        
        return True
    
    def push_file(self, local_path: Path, remote_path: str) -> bool:
        """Push a single file to GitHub."""
        if not local_path.exists():
            logger.debug(f"Skipping {local_path} (not found)")
            return True
        
        try:
            with local_path.open('rb') as f:
                content = base64.b64encode(f.read()).decode('utf-8')
            
            # Check if file exists to determine if we need to update or create
            resp = requests.get(
                f"{self.api_url}/repos/{self.username}/{self.repo_name}/contents/{remote_path}",
                headers=self.headers
            )
            
            payload = {
                "message": f"Update {remote_path}",
                "content": content,
                "branch": "main"
            }
            
            # If file exists, we need to include the SHA
            if resp.status_code == 200:
                payload["sha"] = resp.json()["sha"]
            
            resp = requests.put(
                f"{self.api_url}/repos/{self.username}/{self.repo_name}/contents/{remote_path}",
                headers=self.headers,
                json=payload
            )
            
            if resp.status_code not in {200, 201}:
                logger.error(f"Failed to push {remote_path}: {resp.status_code}")
                return False
            
            logger.debug(f"Pushed {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error pushing {remote_path}: {e}")
            return False
    
    def push_to_github(self, project_root: Path, dry_run: bool = False) -> bool:
        """
        Push the entire project to GitHub, preserving critical mapping files.
        
        Args:
            project_root: Root directory of the project
            dry_run: Whether this is a dry run (will still push to GitHub)
        
        Returns:
            True if successful, False otherwise
        """
        if not self.ensure_repository():
            return False
        
        # Create .gitignore if it doesn't exist
        gitignore_path = project_root / '.gitignore'
        if not gitignore_path.exists():
            logger.info("Creating .gitignore file")
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

# Data directories
data/cache/*
!data/cache/.gitkeep

# Keep mapping files
!data/mappings/broker_mapping.json
!data/mappings/carrier_mapping.json
!data/mappings/policy_type_mapping.json
!data/mappings/unmatched_values.json

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db
"""
            with gitignore_path.open('w') as f:
                f.write(gitignore_content.strip())
        
        # Ensure .gitkeep files exist in empty directories
        for dir_path in ['data/cache', 'data/input', 'data/reports']:
            keep_file = project_root / dir_path / '.gitkeep'
            keep_file.parent.mkdir(parents=True, exist_ok=True)
            if not keep_file.exists():
                with keep_file.open('w') as f:
                    f.write('')
        
        # Get list of files to push
        files_to_push = self._get_files_to_push(project_root)
        
        # Create timestamp for commit message
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Push each file
        success = True
        total_files = len(files_to_push)
        logger.info(f"Pushing {total_files} files to GitHub")
        
        for i, (local_path, remote_path) in enumerate(files_to_push.items(), 1):
            logger.info(f"Pushing file {i}/{total_files}: {remote_path}")
            if not self.push_file(local_path, remote_path):
                success = False
        
        if success:
            logger.info(f"Successfully pushed project to GitHub: https://github.com/{self.username}/{self.repo_name}")
            
            # Create a commit message file to document the push
            commit_message = f"Preserve critical mapping files and update project - {timestamp}"
            commit_file = project_root / 'data' / 'reports' / 'last_github_commit.json'
            commit_file.parent.mkdir(parents=True, exist_ok=True)
            
            with commit_file.open('w') as f:
                json.dump({
                    'timestamp': timestamp,
                    'message': commit_message,
                    'files_pushed': len(files_to_push),
                    'repository': f"https://github.com/{self.username}/{self.repo_name}"
                }, f, indent=4)
        
        return success
    
    def _get_files_to_push(self, project_root: Path) -> Dict[Path, str]:
        """
        Get all files to push to GitHub, respecting .gitignore.
        
        Args:
            project_root: Root directory of the project
        
        Returns:
            Dictionary mapping local paths to remote paths
        """
        files_to_push = {}
        ignored_patterns = self._parse_gitignore(project_root)
        
        # Critical mapping files that must be included
        critical_files = [
            'data/mappings/broker_mapping.json',
            'data/mappings/carrier_mapping.json',
            'data/mappings/policy_type_mapping.json',
            'data/mappings/unmatched_values.json'
        ]
        
        # Add all files in the project
        for path in project_root.glob('**/*'):
            # Skip directories
            if path.is_dir():
                continue
            
            # Get relative path
            rel_path = path.relative_to(project_root)
            str_path = str(rel_path).replace('\\', '/')
            
            # Skip ignored files unless they're critical
            if str_path in critical_files or not self._is_ignored(str_path, ignored_patterns):
                files_to_push[path] = str_path
        
        return files_to_push
    
    def _parse_gitignore(self, project_root: Path) -> List[str]:
        """
        Parse .gitignore file.
        
        Args:
            project_root: Root directory of the project
        
        Returns:
            List of ignore patterns
        """
        gitignore_path = project_root / '.gitignore'
        if not gitignore_path.exists():
            return []
        
        patterns = []
        with gitignore_path.open('r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Handle negated patterns (patterns starting with !)
                    if line.startswith('!'):
                        # Skip negated patterns as we'll handle critical files separately
                        continue
                    patterns.append(line)
        
        return patterns
    
    def _is_ignored(self, path: str, patterns: List[str]) -> bool:
        """
        Check if a path is ignored by .gitignore patterns.
        
        Args:
            path: Path to check
            patterns: List of ignore patterns
        
        Returns:
            True if the path is ignored, False otherwise
        """
        import fnmatch
        
        for pattern in patterns:
            if fnmatch.fnmatch(path, pattern):
                return True
            
            # Handle directory patterns (ending with /)
            if pattern.endswith('/') and path.startswith(pattern[:-1]):
                return True
        
        return False
    
    @classmethod
    def from_env(cls) -> Optional['GitHubSync']:
        """Create GitHubSync instance from environment variables."""
        username = os.environ.get("GITHUB_USERNAME")
        token = os.environ.get("GITHUB_TOKEN")
        repo_name = os.environ.get("GITHUB_REPO", "insurance_policy_migration")
        
        if not username or not token:
            logger.error("Missing GitHub configuration in environment")
            return None
        
        return cls(username, token, repo_name) 