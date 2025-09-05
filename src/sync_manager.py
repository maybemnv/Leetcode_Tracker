import logging
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json
import os

from .leetcode_client import LeetCodeClient
from .sheets_client import SheetsClient
from .data_processor import DataProcessor

logger = logging.getLogger(__name__)

class SyncManager:
    """Main orchestration class for LeetCode to Google Sheets synchronization"""
    
    def __init__(self, config: Dict):
        """
        Initialize sync manager
        
        Args:
            config: Configuration dictionary containing all necessary settings
        """
        self.config = config
        self.leetcode_client = None
        self.sheets_client = None
        self.data_processor = None
        
        # Initialize components
        self._initialize_components()
        
        # Sync state
        self.last_sync_time = None
        self.sync_stats = {
            'total_problems': 0,
            'new_problems': 0,
            'updated_problems': 0,
            'errors': 0,
            'last_sync': None
        }
    
    def _initialize_components(self):
        """Initialize all client components"""
        try:
            # Initialize LeetCode client
            leetcode_config = self.config.get('leetcode', {})
            username = leetcode_config.get('username')
            if not username:
                raise ValueError("LeetCode username is required in configuration")
                
            # Only pass session_id and csrf_token if both are provided
            client_kwargs = {'username': username}
            if leetcode_config.get('session_id') and leetcode_config.get('csrf_token'):
                client_kwargs.update({
                    'session_id': leetcode_config.get('session_id'),
                    'csrf_token': leetcode_config.get('csrf_token')
                })
                
            self.leetcode_client = LeetCodeClient(**client_kwargs)
            logger.info("LeetCode client initialized")
            
            # Initialize Google Sheets client
            sheets_config = self.config.get('google_sheets', {})
            if sheets_config.get('credentials_json'):
                self.sheets_client = SheetsClient(
                    spreadsheet_id=sheets_config.get('spreadsheet_id'),
                    credentials_json=sheets_config.get('credentials_json')
                )
            else:
                self.sheets_client = SheetsClient(
                    spreadsheet_id=sheets_config.get('spreadsheet_id'),
                    credentials_path=sheets_config.get('credentials_path')
                )
            logger.info("Google Sheets client initialized")
            
            # Initialize data processor
            topic_mapping = self.config.get('topic_mapping', {})
            self.data_processor = DataProcessor(topic_mapping=topic_mapping)
            logger.info("Data processor initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            raise
    
    def test_connections(self) -> Dict[str, bool]:
        """
        Test connections to both LeetCode and Google Sheets
        
        Returns:
            Dictionary with connection test results
        """
        results = {}
        
        try:
            # Test LeetCode connection
            leetcode_ok = self.leetcode_client.test_connection()
            results['leetcode'] = leetcode_ok
            logger.info(f"LeetCode connection test: {'PASSED' if leetcode_ok else 'FAILED'}")
        except Exception as e:
            results['leetcode'] = False
            logger.error(f"LeetCode connection test failed: {e}")
        
        try:
            # Test Google Sheets connection
            sheets_ok = self.sheets_client.test_connection()
            results['google_sheets'] = sheets_ok
            logger.info(f"Google Sheets connection test: {'PASSED' if sheets_ok else 'FAILED'}")
        except Exception as e:
            results['google_sheets'] = False
            logger.error(f"Google Sheets connection test failed: {e}")
        
        return results
    
    def sync_all_data(self, force_full_sync: bool = False) -> bool:
        """
        Perform complete data synchronization
        
        Args:
            force_full_sync: If True, ignore incremental sync and fetch all data
            
        Returns:
            bool: True if sync was successful
        """
        start_time = time.time()
        logger.info("Starting LeetCode data synchronization")
        
        try:
            # Test connections first
            connection_results = self.test_connections()
            if not all(connection_results.values()):
                failed_services = [k for k, v in connection_results.items() if not v]
                logger.error(f"Connection test failed for: {failed_services}")
                return False
            
            # Fetch data from LeetCode
            problems_data = self._fetch_leetcode_data(force_full_sync)
            if not problems_data:
                logger.warning("No problems data fetched from LeetCode")
                return False
            
            # Process and validate data
            validated_problems = self.data_processor.validate_problem_data(problems_data)
            if not validated_problems:
                logger.error("No valid problems data after validation")
                return False
            
            # Generate analytics
            analytics_data = self.data_processor.generate_analytics(validated_problems)
            
            # Update Google Sheets
            success = self._update_google_sheets(validated_problems, analytics_data)
            
            if success:
                # Update sync state
                self.last_sync_time = datetime.now()
                self.sync_stats.update({
                    'total_problems': len(validated_problems),
                    'new_problems': len(validated_problems),  # Simplified for now
                    'updated_problems': 0,
                    'errors': 0,
                    'last_sync': self.last_sync_time.strftime('%Y-%m-%d %H:%M:%S')
                })
                
                sync_duration = time.time() - start_time
                logger.info(f"Sync completed successfully in {sync_duration:.2f} seconds")
                logger.info(f"Synced {len(validated_problems)} problems")
                return True
            else:
                logger.error("Failed to update Google Sheets")
                return False
                
        except Exception as e:
            logger.error(f"Sync failed with error: {e}")
            self.sync_stats['errors'] += 1
            return False
    
    def _fetch_leetcode_data(self, force_full_sync: bool = False) -> List[Dict]:
        """
        Fetch data from LeetCode
        
        Args:
            force_full_sync: If True, fetch all data regardless of last sync
            
        Returns:
            List of problem dictionaries
        """
        try:
            if force_full_sync or not self.last_sync_time:
                logger.info("Performing full data fetch from LeetCode")
                problems = self.leetcode_client.get_all_solved_problems()
            else:
                logger.info("Performing incremental data fetch from LeetCode")
                # For now, always fetch all data
                # In the future, this could implement incremental fetching
                problems = self.leetcode_client.get_all_solved_problems()
            
            logger.info(f"Fetched {len(problems)} problems from LeetCode")
            return problems
            
        except Exception as e:
            logger.error(f"Failed to fetch LeetCode data: {e}")
            return []
    
    def _update_google_sheets(self, problems_data: List[Dict], analytics_data: Dict) -> bool:
        """
        Update Google Sheets with new data
        
        Args:
            problems_data: List of problem dictionaries
            analytics_data: Dictionary containing analytics data
            
        Returns:
            bool: True if update was successful
        """
        try:
            # Update Problems sheet
            problems_success = self.sheets_client.update_problems_sheet(problems_data)
            if not problems_success:
                logger.error("Failed to update Problems sheet")
                return False
            
            # Update Analytics sheet
            topic_analytics = analytics_data.get('topic_analytics', {})
            analytics_success = self.sheets_client.update_analytics_sheet(topic_analytics)
            if not analytics_success:
                logger.error("Failed to update Analytics sheet")
                return False
            
            # Update Progress sheet
            progress_data = analytics_data.get('progress_data', [])
            progress_success = self.sheets_client.update_progress_sheet(progress_data)
            if not progress_success:
                logger.error("Failed to update Progress sheet")
                return False
            
            logger.info("Successfully updated all Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Google Sheets: {e}")
            return False
    
    def incremental_sync(self) -> bool:
        """
        Perform incremental synchronization (only new/updated data)
        
        Returns:
            bool: True if sync was successful
        """
        logger.info("Starting incremental synchronization")
        
        try:
            # Get existing problems from sheets
            existing_problems = self.sheets_client.get_existing_problems()
            existing_titles = {p.get('title', '') for p in existing_problems}
            
            # Fetch new data from LeetCode
            all_problems = self._fetch_leetcode_data(force_full_sync=False)
            if not all_problems:
                return False
            
            # Identify new problems
            new_problems = []
            for problem in all_problems:
                if problem.get('title') not in existing_titles:
                    new_problems.append(problem)
            
            if not new_problems:
                logger.info("No new problems to sync")
                return True
            
            logger.info(f"Found {len(new_problems)} new problems to sync")
            
            # Merge with existing problems
            all_problems_for_sync = existing_problems + new_problems
            
            # Generate analytics for all problems
            analytics_data = self.data_processor.generate_analytics(all_problems_for_sync)
            
            # Update sheets with merged data
            success = self._update_google_sheets(all_problems_for_sync, analytics_data)
            
            if success:
                self.sync_stats.update({
                    'total_problems': len(all_problems_for_sync),
                    'new_problems': len(new_problems),
                    'updated_problems': 0,
                    'last_sync': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                logger.info(f"Incremental sync completed: {len(new_problems)} new problems added")
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"Incremental sync failed: {e}")
            return False
    
    def get_sync_status(self) -> Dict:
        """
        Get current synchronization status
        
        Returns:
            Dictionary with sync status information
        """
        status = {
            'last_sync_time': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'sync_stats': self.sync_stats.copy(),
            'connections': self.test_connections(),
            'config_loaded': bool(self.config),
            'components_initialized': all([
                self.leetcode_client is not None,
                self.sheets_client is not None,
                self.data_processor is not None
            ])
        }
        
        return status
    
    def backup_data(self, backup_path: str = None) -> bool:
        """
        Create a backup of current data
        
        Args:
            backup_path: Path to save backup file
            
        Returns:
            bool: True if backup was successful
        """
        try:
            if not backup_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"backup_leetcode_data_{timestamp}.json"
            
            # Get current data from sheets
            problems_data = self.sheets_client.get_existing_problems()
            
            # Create backup data
            backup_data = {
                'backup_timestamp': datetime.now().isoformat(),
                'sync_stats': self.sync_stats,
                'problems_count': len(problems_data),
                'problems_data': problems_data
            }
            
            # Save to file
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Backup created successfully at: {backup_path}")
            return True
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
    
    def restore_data(self, backup_path: str) -> bool:
        """
        Restore data from backup
        
        Args:
            backup_path: Path to backup file
            
        Returns:
            bool: True if restore was successful
        """
        try:
            # Load backup data
            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            
            problems_data = backup_data.get('problems_data', [])
            if not problems_data:
                logger.error("No problems data found in backup")
                return False
            
            # Generate analytics
            analytics_data = self.data_processor.generate_analytics(problems_data)
            
            # Update sheets
            success = self._update_google_sheets(problems_data, analytics_data)
            
            if success:
                logger.info(f"Data restored successfully from backup: {backup_path}")
                return True
            else:
                logger.error("Failed to restore data to sheets")
                return False
                
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> bool:
        """
        Clean up old data (optional feature)
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            bool: True if cleanup was successful
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            cutoff_str = cutoff_date.strftime('%Y-%m-%d')
            
            # Get current data
            problems_data = self.sheets_client.get_existing_problems()
            
            # Filter out old problems
            recent_problems = []
            for problem in problems_data:
                date_solved = problem.get('date_solved', '')
                if date_solved and date_solved >= cutoff_str:
                    recent_problems.append(problem)
            
            if len(recent_problems) < len(problems_data):
                logger.info(f"Cleaning up old data: keeping {len(recent_problems)} out of {len(problems_data)} problems")
                
                # Generate analytics for remaining problems
                analytics_data = self.data_processor.generate_analytics(recent_problems)
                
                # Update sheets with cleaned data
                success = self._update_google_sheets(recent_problems, analytics_data)
                
                if success:
                    logger.info("Data cleanup completed successfully")
                    return True
                else:
                    logger.error("Failed to update sheets during cleanup")
                    return False
            else:
                logger.info("No old data to clean up")
                return True
                
        except Exception as e:
            logger.error(f"Data cleanup failed: {e}")
            return False
