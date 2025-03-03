# Insurance Policy Migration

This script migrates insurance policy data from CSV files to an AMS (Agency Management System) via a Frappe API.

## Project Structure

```
insurance_policy_migration/
├── data/
│   ├── input/      # Place CSV files here for processing
│   ├── cache/      # Temporary cache files
│   └── reports/    # Generated report files
├── policy_migration.py    # Main script
├── setup_env.py          # Environment setup script
├── init_git_repo.py      # Git repository initialization
├── requirements.txt      # Python dependencies
├── policy_upload_log.txt # Processing log
└── .gitignore           # Git ignore rules
```

## Prerequisites

- Python 3.7+
- Git
- AMS API access token
- GitHub token (for repository management)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/grijalva10/insurance_policy_migration.git
   cd insurance_policy_migration
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # OR
   .\venv\Scripts\activate   # Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   python setup_env.py
   ```

## Usage

1. Place your CSV files in the `data/input/` directory.

2. Run the script:
   ```bash
   python policy_migration.py [options]
   ```

   Options:
   - `--dry-run`: Run without making API calls
   - `--no-cache`: Don't use cached data
   - `--ams-token TOKEN`: Specify AMS API token
   - `--github-token TOKEN`: Specify GitHub token
   - `--skip-ams-fetch`: Skip fetching policies from AMS

3. Check the results:
   - Valid policies: `data/reports/valid_policies.csv`
   - Invalid policies: `data/reports/invalid_policies.csv`
   - New policies: `data/reports/new_policies.csv`
   - Existing policies: `data/reports/existing_policies.csv`
   - Processing log: `policy_upload_log.txt`

## Policy Type Mapping

The script maps policy types to standardized values:

- Bond
- Builders Risk
- Commercial Auto
- Commercial Property
- Excess
- General Liability
- Inland Marine
- Pollution Liability
- Professional Liability
- Workers Compensation
- Endorsement
- General Liability + Excess
- General Liability + Inland Marine
- General Liability + Builders Risk
- Other

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 