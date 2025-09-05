# LeetCode Progress Tracker 🚀

An automated system to track your LeetCode progress by fetching solved problems via the GraphQL API and syncing data to Google Sheets with comprehensive analytics and topic-wise progress tracking.

## ✨ Features

- **🔍 Automated Data Fetching**: Fetches solved problems from LeetCode using GraphQL API
- **📊 Google Sheets Integration**: Automatically syncs data to organized spreadsheets
- **📈 Advanced Analytics**: Topic-wise progress, difficulty breakdown, solving patterns
- **🔄 Incremental Sync**: Only syncs new problems for efficiency
- **📅 Progress Tracking**: Daily, weekly, and monthly progress with streak calculation
- **🤖 GitHub Actions**: Automated daily synchronization (free tier)
- **💾 Backup & Restore**: Data backup and restoration capabilities
- **🔧 Flexible Configuration**: Environment-based configuration management

## 🏗️ Architecture

```
LeetCode GraphQL API → Data Processor → Google Sheets API
       ↓                      ↓              ↓
  Fetch Problems    →  Analytics &    →  Update Sheets
  & Metadata       →  Progress Calc   →  (Problems, Analytics, Progress)
```

## 📁 Project Structure

```
leetcode-tracker/
├── src/                          # Core source code
│   ├── leetcode_client.py       # LeetCode GraphQL API client
│   ├── sheets_client.py         # Google Sheets operations
│   ├── data_processor.py        # Data transformation & analytics
│   └── sync_manager.py          # Main orchestration logic
├── config/                       # Configuration files
│   ├── setting.py               # Configuration management
│   └── topic_mapping.json       # LeetCode topic categorization
├── .github/workflows/            # GitHub Actions automation
│   └── sync.yml                 # Daily sync workflow
├── main.py                      # Command-line interface
├── requirements.txt              # Python dependencies
└── README.md                    # This file
```

## 🚀 Quick Start

### 1. Prerequisites

- Python 3.9+
- LeetCode account
- Google Cloud Project with Sheets API enabled
- Google Service Account credentials

### 2. Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/leetcode-tracker.git
cd leetcode-tracker

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Setup Configuration

```bash
# Create environment template
python main.py setup

# Copy and edit the environment file
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# LeetCode Configuration
LEETCODE_USERNAME=your_username_here
LEETCODE_SESSION_ID=your_session_id_here  # Optional
LEETCODE_CSRF_TOKEN=your_csrf_token_here  # Optional

# Google Sheets Configuration
GOOGLE_SHEETS_ID=your_spreadsheet_id_here
GOOGLE_CREDENTIALS_JSON={"type": "service_account", ...}  # Service account JSON

# Synchronization Configuration
SYNC_INTERVAL=daily
LOG_LEVEL=INFO
```

### 4. Google Sheets Setup

1. **Create a new Google Sheet** or use existing one
2. **Enable Google Sheets API** in Google Cloud Console
3. **Create Service Account** and download JSON credentials
4. **Share the sheet** with your service account email
5. **Copy the spreadsheet ID** from the URL

### 5. Test Configuration

```bash
# Test connections
python main.py test

# Show status
python main.py status
```

### 6. First Sync

```bash
# Perform full synchronization
python main.py sync

# Or incremental sync
python main.py sync --incremental
```

## 📖 Usage

### Command Line Interface

```bash
# Test connections
python main.py test

# Full synchronization
python main.py sync

# Incremental synchronization
python main.py sync --incremental

# Show current status
python main.py status

# Create backup
python main.py backup

# Restore from backup
python main.py restore backup_file.json

# Clean up old data
python main.py cleanup --days 365

# Create environment template
python main.py setup
```

### Google Sheets Structure

The system creates three sheets automatically:

1. **Problems Sheet**: Raw problem data with metadata
2. **Analytics Sheet**: Topic-wise summary and statistics
3. **Progress Sheet**: Daily progress and streak tracking

## 📊 Analytics Features

### Topic-wise Progress
- Problem count by topic
- Difficulty breakdown (Easy/Medium/Hard)
- Last solved date per topic
- Progress percentage calculations

### Progress Tracking
- Daily submission counts
- Weekly and monthly summaries
- Current and longest streaks
- Total problems solved

### Solving Patterns
- Most productive days
- Difficulty progression over time
- Language usage statistics
- Runtime and memory analysis

## 🧪 Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=src

# Format code
black src/

# Lint code
flake8 src/
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- LeetCode for providing the GraphQL API
- Google for Google Sheets API
- Open source community for the libraries used

---

**Happy Coding! 🎉**

*Track your LeetCode progress, analyze your patterns, and level up your problem-solving skills!* 