# Insurance Policy Migration Script

This script processes insurance policies from CSV files, checks their existence in an AMS system, and prepares them for upload via an API.

## Features

- Loads policies from CSV files in the `data/input/` directory
- Validates and normalizes policy data
- Fetches existing policy numbers from AMS to identify new vs. existing policies
- Splits valid policies into new and existing ones
- Generates detailed reports in the `data/reports/` directory
- Automatically uploads log files and reports to GitHub repository for easy sharing
- Secure handling of API tokens via environment variables
- Creates private GitHub repositories by default for data security

## Requirements

- Python 3.6+
- Required packages: pandas, requests, python-dateutil
- Git (for repository initialization)

## Installation

1. Clone this repository
   ```
   git clone https://github.com/grijalva10/insurance_policy_migration.git
   cd insurance_policy_migration
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```
   python setup_env.py --github-token YOUR_GITHUB_TOKEN --ams-token YOUR_AMS_TOKEN
   ```

   Then load the environment variables:
   - Windows (PowerShell):
     ```
     $env:GITHUB_TOKEN = (Get-Content .env | Select-String GITHUB_TOKEN).ToString().Split('=')[1]
     $env:AMS_API_TOKEN = (Get-Content .env | Select-String AMS_API_TOKEN).ToString().Split('=')[1]
     ```
   - Linux/Mac:
     ```
     export $(grep -v '^#' .env | xargs)
     ```

## Usage

Basic usage:
```
python policy_migration.py
```

### Command Line Options

- `--dry-run`: Run without making API calls
- `--no-cache`: Don't use cached data
- `--upload-log`: Upload log file to GitHub repository
- `--upload-script`: Upload script file to GitHub repository
- `--github-token TOKEN`: GitHub API token for repository upload
- `--ams-token TOKEN`: AMS API token for AMS API calls
- `--skip-ams-fetch`: Skip fetching policies from AMS
- `--include-all-files`: Include all files in the repository when uploading

### Environment Variables

- `GITHUB_TOKEN`: GitHub API token for repository upload (alternative to `--github-token`)
- `AMS_API_TOKEN`: AMS API token for AMS API calls (alternative to `--ams-token`)

## Output Files

The script generates the following output files in the `data/reports/` directory:

- `valid_policies.csv`: All valid policies
- `invalid_policies.csv`: Policies with validation issues
- `new_policies.csv`: Valid policies not found in AMS
- `existing_policies.csv`: Valid policies already in AMS

## Log File

The script generates a detailed log file (`policy_upload_log.txt`) that can be automatically uploaded to GitHub repository for easy sharing.

## GitHub Repository

The script can create or update a GitHub repository named `insurance_policy_migration` with all the necessary files. This provides a centralized location for all policy migration data and makes it easy to share with team members.

The repository is created as private by default to ensure the security of sensitive insurance policy data. Only users with explicit access to the repository will be able to view or download the files.

## Git Repository Setup

To initialize a local Git repository for this project:

```
python init_git_repo.py
```

This will create a Git repository with appropriate `.gitignore` settings and make an initial commit.

## Example

```
python policy_migration.py --upload-log --upload-script --include-all-files
```

This will process all policies, split them into new and existing, and upload all files to the GitHub repository.

## Security

- API tokens are stored as environment variables rather than hardcoded in the script
- The `.env` file containing sensitive tokens is excluded from Git via `.gitignore`
- Use the `setup_env.py` script to securely manage your environment variables 