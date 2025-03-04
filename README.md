# Insurance Policy Migration

This project migrates insurance policy data from CSV files to an AMS (Agency Management System) via a Frappe API.

## Project Structure

```
insurance_policy_migration/
├── src/
│   └── insurance_migration/
│       ├── __init__.py
│       ├── __main__.py           # Entry point
│       ├── ams_client.py         # AMS API client
│       ├── data_loader.py        # CSV loading and transformation
│       ├── policy_processor.py   # Policy processing logic
│       ├── mapping_manager.py    # Mapping file handling
│       ├── logger.py             # Logging configuration
│       └── github_sync.py        # GitHub integration
├── data/
│   ├── input/                    # Place CSV files here for processing
│   ├── cache/                    # Temporary cache files
│   ├── reports/                  # Generated report files
│   └── mappings/                 # Mapping files
├── logs/                         # Log files
├── setup.py                      # Package configuration
├── run_migration.py              # Simple entry point script
├── recover_mappings.py           # Script to recover mapping data
├── requirements.txt              # Python dependencies
└── .gitignore                    # Git ignore rules
```

## Prerequisites

- Python 3.8+
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

4. Install the package in development mode:
   ```bash
   pip install -e .
   ```

5. Set up environment variables:
   ```bash
   # Linux/Mac
   export AMS_API_URL="https://ams.jmggo.com/api/method"
   export AMS_API_TOKEN="your-token-here"
   
   # Windows
   set AMS_API_URL=https://ams.jmggo.com/api/method
   set AMS_API_TOKEN=your-token-here
   ```

## Usage

1. Place your CSV files in the `data/input/` directory.

2. Run the script:
   ```bash
   python run_migration.py [options]
   ```

   Or use the package entry point:
   ```bash
   python -m src.insurance_migration [options]
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
   - Processing log: `logs/policy_upload_log.txt`

## Mapping Files

The project uses several mapping files to standardize data:

1. **Carrier Mappings** (`data/mappings/carrier_mapping.json`):
   - Maps carrier names to standardized values

2. **Policy Type Mappings** (`data/mappings/policy_type_mapping.json`):
   - Maps policy types to standardized values

3. **Broker Mappings** (`data/mappings/broker_mapping.json`):
   - Maps broker names to email addresses

4. **Exclusion Mappings** (`data/mappings/exclusion_mapping.json`):
   - Lists values that should be excluded from processing

5. **Unmatched Values** (`data/mappings/unmatched_values.json`):
   - Tracks values that couldn't be mapped for future improvement

If mapping files are missing or corrupted, you can recover them using:
```bash
python recover_mappings.py
```

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