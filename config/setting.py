import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class ConfigManager:
    """Configuration management for LeetCode tracker"""
    
    def __init__(self, config_path: str = None, env_file: str = None):
        """
        Initialize configuration manager
        
        Args:
            config_path: Path to configuration file (optional)
            env_file: Path to .env file (optional)
        """
        self.config_path = config_path
        self.env_file = env_file or '.env'
        self.config = {}
        
        # Load configuration
        self._load_config()
    
    def _load_config(self):
        """Load configuration from multiple sources"""
        try:
            # Load environment variables
            self._load_env_vars()
            
            # Load configuration file if specified
            if self.config_path and os.path.exists(self.config_path):
                self._load_config_file()
            
            # Set default values
            self._set_defaults()
            
            # Validate configuration
            self._validate_config()
            
            logger.info("Configuration loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _load_env_vars(self):
        """Load environment variables from .env file"""
        try:
            # Try to load .env file
            if os.path.exists(self.env_file):
                load_dotenv(self.env_file)
                logger.info(f"Loaded environment variables from {self.env_file}")
            else:
                logger.warning(f"Environment file {self.env_file} not found, using system environment")
            
            # Load required environment variables
            self.config = {
                'leetcode': {
                    'username': os.getenv('LEETCODE_USERNAME'),
                    'session_id': os.getenv('LEETCODE_SESSION_ID'),
                    'csrf_token': os.getenv('LEETCODE_CSRF_TOKEN')
                },
                'google_sheets': {
                    'spreadsheet_id': os.getenv('GOOGLE_SHEETS_ID'),
                    'credentials_path': os.getenv('GOOGLE_CREDENTIALS_PATH'),
                    'credentials_json': os.getenv('GOOGLE_CREDENTIALS_JSON')
                },
                'sync': {
                    'interval': os.getenv('SYNC_INTERVAL', 'daily'),
                    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
                    'max_retries': int(os.getenv('MAX_RETRIES', '3')),
                    'timeout': int(os.getenv('TIMEOUT', '30'))
                },
                'backup': {
                    'enabled': os.getenv('BACKUP_ENABLED', 'true').lower() == 'true',
                    'path': os.getenv('BACKUP_PATH', './backups'),
                    'retention_days': int(os.getenv('BACKUP_RETENTION_DAYS', '30'))
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to load environment variables: {e}")
            raise
    
    def _load_config_file(self):
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            
            # Merge with environment-based config
            self._merge_config(file_config)
            logger.info(f"Loaded configuration from {self.config_path}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration file {self.config_path}: {e}")
            raise
    
    def _merge_config(self, file_config: Dict):
        """Merge file configuration with environment configuration"""
        for section, values in file_config.items():
            if section not in self.config:
                self.config[section] = {}
            
            if isinstance(values, dict):
                for key, value in values.items():
                    # Only override if environment variable is not set
                    env_key = f"{section.upper()}_{key.upper()}"
                    if not os.getenv(env_key):
                        self.config[section][key] = value
            else:
                self.config[section] = values
    
    def _set_defaults(self):
        """Set default configuration values"""
        defaults = {
            'sync': {
                'interval': 'daily',
                'log_level': 'INFO',
                'max_retries': 3,
                'timeout': 30
            },
            'backup': {
                'enabled': True,
                'path': './backups',
                'retention_days': 30
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': None
            }
        }
        
        for section, values in defaults.items():
            if section not in self.config:
                self.config[section] = {}
            
            for key, value in values.items():
                if key not in self.config[section]:
                    self.config[section][key] = value
    
    def _validate_config(self):
        """Validate configuration values"""
        errors = []
        
        # Validate LeetCode configuration
        leetcode_config = self.config.get('leetcode', {})
        if not leetcode_config.get('username'):
            errors.append("LEETCODE_USERNAME is required")
        
        # Validate Google Sheets configuration
        sheets_config = self.config.get('google_sheets', {})
        if not sheets_config.get('spreadsheet_id'):
            errors.append("GOOGLE_SHEETS_ID is required")
        
        if not sheets_config.get('credentials_path') and not sheets_config.get('credentials_json'):
            errors.append("Either GOOGLE_CREDENTIALS_PATH or GOOGLE_CREDENTIALS_JSON is required")
        
        # Validate sync configuration
        sync_config = self.config.get('sync', {})
        if sync_config.get('interval') not in ['hourly', 'daily', 'weekly']:
            errors.append("SYNC_INTERVAL must be one of: hourly, daily, weekly")
        
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"- {error}" for error in errors)
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key
        
        Args:
            key: Configuration key (e.g., 'leetcode.username')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_leetcode_config(self) -> Dict:
        """Get LeetCode configuration"""
        return self.config.get('leetcode', {})
    
    def get_sheets_config(self) -> Dict:
        """Get Google Sheets configuration"""
        return self.config.get('google_sheets', {})
    
    def get_sync_config(self) -> Dict:
        """Get synchronization configuration"""
        return self.config.get('sync', {})
    
    def get_backup_config(self) -> Dict:
        """Get backup configuration"""
        return self.config.get('backup', {})
    
    def get_logging_config(self) -> Dict:
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def update(self, key: str, value: Any):
        """
        Update configuration value
        
        Args:
            key: Configuration key (e.g., 'leetcode.username')
            value: New value
        """
        keys = key.split('.')
        config = self.config
        
        # Navigate to the parent of the target key
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # Set the value
        config[keys[-1]] = value
        logger.info(f"Updated configuration: {key} = {value}")
    
    def save_config(self, file_path: str = None):
        """
        Save configuration to file
        
        Args:
            file_path: Path to save configuration file
        """
        if not file_path:
            file_path = self.config_path or 'config.json'
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Configuration saved to {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration to {file_path}: {e}")
            raise
    
    def create_env_template(self, file_path: str = '.env.example'):
        """
        Create environment variables template file
        
        Args:
            file_path: Path to save template file
        """
        template = """# LeetCode Tracker Environment Variables

# LeetCode Configuration
LEETCODE_USERNAME=your_username_here
LEETCODE_SESSION_ID=your_session_id_here  # Optional
LEETCODE_CSRF_TOKEN=your_csrf_token_here  # Optional

# Google Sheets Configuration
GOOGLE_SHEETS_ID=your_spreadsheet_id_here
GOOGLE_CREDENTIALS_PATH=path/to/service_account.json  # Use this OR GOOGLE_CREDENTIALS_JSON
GOOGLE_CREDENTIALS_JSON={"type": "service_account", ...}  # Use this OR GOOGLE_CREDENTIALS_PATH

# Synchronization Configuration
SYNC_INTERVAL=daily  # hourly, daily, weekly
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
MAX_RETRIES=3
TIMEOUT=30

# Backup Configuration
BACKUP_ENABLED=true
BACKUP_PATH=./backups
BACKUP_RETENTION_DAYS=30

# Copy this file to .env and fill in your actual values
"""
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(template)
            
            logger.info(f"Environment template created at {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to create environment template: {e}")
            raise
    
    def get_required_env_vars(self) -> Dict[str, str]:
        """Get list of required environment variables"""
        return {
            'LEETCODE_USERNAME': 'Your LeetCode username',
            'GOOGLE_SHEETS_ID': 'Your Google Sheets spreadsheet ID',
            'GOOGLE_CREDENTIALS_PATH': 'Path to Google service account JSON file (OR use GOOGLE_CREDENTIALS_JSON)',
            'GOOGLE_CREDENTIALS_JSON': 'Google service account JSON content (OR use GOOGLE_CREDENTIALS_PATH)'
        }
    
    def check_missing_env_vars(self) -> list:
        """Check for missing required environment variables"""
        missing = []
        required = self.get_required_env_vars()
        
        for var, description in required.items():
            if not os.getenv(var):
                missing.append(f"{var}: {description}")
        
        return missing
    
    def print_config_summary(self):
        """Print a summary of the current configuration"""
        print("\n=== LeetCode Tracker Configuration Summary ===")
        
        # LeetCode config
        leetcode = self.get_leetcode_config()
        print(f"\nLeetCode:")
        print(f"  Username: {leetcode.get('username', 'NOT SET')}")
        print(f"  Session ID: {'SET' if leetcode.get('session_id') else 'NOT SET'}")
        print(f"  CSRF Token: {'SET' if leetcode.get('csrf_token') else 'NOT SET'}")
        
        # Google Sheets config
        sheets = self.get_sheets_config()
        print(f"\nGoogle Sheets:")
        print(f"  Spreadsheet ID: {sheets.get('spreadsheet_id', 'NOT SET')}")
        print(f"  Credentials: {'PATH' if sheets.get('credentials_path') else 'JSON' if sheets.get('credentials_json') else 'NOT SET'}")
        
        # Sync config
        sync = self.get_sync_config()
        print(f"\nSynchronization:")
        print(f"  Interval: {sync.get('interval', 'daily')}")
        print(f"  Log Level: {sync.get('log_level', 'INFO')}")
        print(f"  Max Retries: {sync.get('max_retries', 3)}")
        
        # Check for missing required vars
        missing = self.check_missing_env_vars()
        if missing:
            print(f"\n⚠️  MISSING REQUIRED ENVIRONMENT VARIABLES:")
            for var in missing:
                print(f"  - {var}")
        else:
            print(f"\n✅ All required environment variables are set")
        
        print("=" * 50)


def load_config(config_path: str = None, env_file: str = None) -> ConfigManager:
    """
    Convenience function to load configuration
    
    Args:
        config_path: Path to configuration file
        env_file: Path to .env file
        
    Returns:
        ConfigManager instance
    """
    return ConfigManager(config_path=config_path, env_file=env_file)


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config()
        config.print_config_summary()
    except Exception as e:
        print(f"Configuration test failed: {e}")
