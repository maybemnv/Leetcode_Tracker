# LeetCode Progress Tracker

## Overview
An automated system for tracking LeetCode progress through GraphQL API integration with Google Sheets, providing comprehensive analytics and progress monitoring capabilities.

## Core Features
- Automated problem data collection via LeetCode GraphQL API
- Google Sheets integration for data storage and visualization
- Advanced analytics and progress tracking
- Automated daily synchronization via GitHub Actions
- Configurable backup and restore functionality
- Environment-based configuration management

## System Architecture
```
LeetCode GraphQL API → Data Processor → Google Sheets API
       |                    |               |
  Problem Fetching    Data Analysis    Sheet Updates
```

## Project Structure
```
leetcode-tracker/
├── src/
│   ├── leetcode_client.py
│   ├── sheets_client.py
│   ├── data_processor.py
│   └── sync_manager.py
├── config/
│   ├── settings.py
│   └── topic_mapping.json
├── .github/workflows/
│   └── sync.yml
├── main.py
└── README.md
```

## Requirements
- Python 3.9 or higher
- Active LeetCode account
- Google Cloud Platform account with Sheets API enabled
- Google Service Account credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/leetcode-tracker.git
cd leetcode-tracker
```

2. Set up virtual environment:
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

3. Configure Google Sheets API:
   - Access Google Cloud Console
   - Create new project and enable Sheets API
   - Generate service account credentials
   - Store credentials as `credentials.json` in `config/`
   - Grant sheet access to service account

4. Configure environment variables:
```
LEETCODE_USERNAME=<username>
SHEET_ID=<google_sheet_id>
NOTIFICATION_EMAIL=<email>
```

## Usage

### Basic Operations
```bash
# Execute full sync
python main.py sync

# Run incremental sync
python main.py sync --incremental

# Verify system status
python main.py status
```

### Advanced Operations
```bash
# Create data backup
python main.py backup

# Restore from backup
python main.py restore <backup_file>

# Data cleanup
python main.py cleanup --days <retention_days>
```

## API Considerations

### LeetCode API
- Request limit: 50 requests per minute
- Implementation guidelines:
  - Utilize GraphQL queries
  - Implement local caching
  - Use exponential backoff for retries

### Google Sheets API
- Limitations:
  - 300 requests per minute per user
  - 40,000 requests per day per project
- Best practices:
  - Implement batch updates
  - Cache data locally
  - Minimize read operations
  - Use range updates

## Data Structure

### Sheet Organization
1. Problems Sheet
   - Raw problem data
   - Submission metadata
   - Solution details

2. Analytics Sheet
   - Topic-wise metrics
   - Performance statistics
   - Progress indicators

3. Progress Sheet
   - Temporal tracking
   - Streak calculations
   - Completion rates

## Testing
```bash
# Execute test suite
pytest

# Run with coverage analysis
pytest --cov=src

# Code formatting
black src/

# Static analysis
flake8 src/
```

## Contributing
1. Fork repository
2. Create feature branch
3. Implement changes
4. Add appropriate tests
5. Submit pull request

## License
MIT License - See LICENSE file for details

## Support
For technical issues or questions, create an issue in the repository.

---
Documentation Version: 1.0.0
Last Updated: 2025-09-06