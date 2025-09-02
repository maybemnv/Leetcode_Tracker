import gspread
from google.oauth2.service_account import Credentials
from typing import List, Dict, Optional
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class SheetsClient:
    """Google Sheets client for LeetCode progress tracking"""
    
    def __init__(self, spreadsheet_id: str, credentials_path: str = None, credentials_json: str = None):
        """
        Initialize Google Sheets client
        
        Args:
            spreadsheet_id: ID of the target spreadsheet
            credentials_path: Path to service account JSON file
            credentials_json: Service account JSON as string (alternative to file path)
        """
        self.spreadsheet_id = spreadsheet_id
        self.client = None
        self.spreadsheet = None
        
        # Initialize authentication
        if credentials_json:
            self._authenticate_with_json(credentials_json)
        elif credentials_path:
            self._authenticate_with_file(credentials_path)
        else:
            raise ValueError("Either credentials_path or credentials_json must be provided")
        
        # Initialize spreadsheet
        self._initialize_spreadsheet()
    
    def _authenticate_with_file(self, credentials_path: str):
        """Authenticate using service account JSON file"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = Credentials.from_service_account_file(credentials_path, scopes=scope)
            self.client = gspread.authorize(credentials)
            logger.info("Authenticated with Google Sheets using file")
        except Exception as e:
            logger.error(f"Failed to authenticate with file: {e}")
            raise
    
    def _authenticate_with_json(self, credentials_json: str):
        """Authenticate using service account JSON string"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = Credentials.from_service_account_info(
                json.loads(credentials_json), scopes=scope
            )
            self.client = gspread.authorize(credentials)
            logger.info("Authenticated with Google Sheets using JSON")
        except Exception as e:
            logger.error(f"Failed to authenticate with JSON: {e}")
            raise
    
    def _initialize_spreadsheet(self):
        """Initialize spreadsheet and create sheets if they don't exist"""
        try:
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            logger.info(f"Connected to spreadsheet: {self.spreadsheet.title}")
            
            # Ensure required sheets exist
            self._ensure_sheets_exist()
        except Exception as e:
            logger.error(f"Failed to initialize spreadsheet: {e}")
            raise
    
    def _ensure_sheets_exist(self):
        """Create required sheets if they don't exist"""
        required_sheets = ["Problems", "Analytics", "Progress"]
        existing_sheets = [sheet.title for sheet in self.spreadsheet.worksheets()]
        
        for sheet_name in required_sheets:
            if sheet_name not in existing_sheets:
                self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                logger.info(f"Created sheet: {sheet_name}")
                
                # Initialize sheet headers
                if sheet_name == "Problems":
                    self._initialize_problems_sheet()
                elif sheet_name == "Analytics":
                    self._initialize_analytics_sheet()
                elif sheet_name == "Progress":
                    self._initialize_progress_sheet()
    
    def _initialize_problems_sheet(self):
        """Initialize Problems sheet with headers"""
        try:
            worksheet = self.spreadsheet.worksheet("Problems")
            headers = [
                "Problem Name", "Difficulty", "Topics", "Date Solved", 
                "Attempts", "Status", "Problem ID", "Last Updated"
            ]
            worksheet.update('A1:H1', [headers])
            
            # Format headers
            worksheet.format('A1:H1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}
            })
            logger.info("Initialized Problems sheet headers")
        except Exception as e:
            logger.error(f"Failed to initialize Problems sheet: {e}")
    
    def _initialize_analytics_sheet(self):
        """Initialize Analytics sheet with headers and formulas"""
        try:
            worksheet = self.spreadsheet.worksheet("Analytics")
            headers = [
                "Topic", "Total Problems", "Solved", "Percentage", 
                "Last Solved", "Easy", "Medium", "Hard"
            ]
            worksheet.update('A1:H1', [headers])
            
            # Format headers
            worksheet.format('A1:H1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}
            })
            logger.info("Initialized Analytics sheet headers")
        except Exception as e:
            logger.error(f"Failed to initialize Analytics sheet: {e}")
    
    def _initialize_progress_sheet(self):
        """Initialize Progress sheet with headers"""
        try:
            worksheet = self.spreadsheet.worksheet("Progress")
            headers = [
                "Date", "Daily Count", "Weekly Count", "Monthly Count", 
                "Streak", "Total Solved", "Last Updated"
            ]
            worksheet.update('A1:G1', [headers])
            
            # Format headers
            worksheet.format('A1:G1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.8, 'green': 0.8, 'blue': 0.8}
            })
            logger.info("Initialized Progress sheet headers")
        except Exception as e:
            logger.error(f"Failed to initialize Progress sheet: {e}")
    
    def update_problems_sheet(self, problems_data: List[Dict]) -> bool:
        """
        Update Problems sheet with new problem data
        
        Args:
            problems_data: List of problem dictionaries
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            worksheet = self.spreadsheet.worksheet("Problems")
            
            # Clear existing data (keep headers)
            worksheet.clear()
            self._initialize_problems_sheet()
            
            # Prepare data for batch update
            rows = []
            for problem in problems_data:
                row = [
                    problem.get('title', ''),
                    problem.get('difficulty', ''),
                    ', '.join(problem.get('topics', [])),
                    problem.get('date_solved', ''),
                    problem.get('attempts', 1),
                    problem.get('status', 'Solved'),
                    problem.get('problem_id', ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows.append(row)
            
            # Batch update
            if rows:
                worksheet.update(f'A2:H{len(rows)+1}', rows)
                logger.info(f"Updated Problems sheet with {len(rows)} problems")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Problems sheet: {e}")
            return False
    
    def update_analytics_sheet(self, analytics_data: Dict) -> bool:
        """
        Update Analytics sheet with topic-wise summary
        
        Args:
            analytics_data: Dictionary containing topic analytics
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            worksheet = self.spreadsheet.worksheet("Analytics")
            
            # Clear existing data (keep headers)
            worksheet.clear()
            self._initialize_analytics_sheet()
            
            # Prepare analytics data
            rows = []
            for topic, data in analytics_data.items():
                row = [
                    topic,
                    data.get('total', 0),
                    data.get('solved', 0),
                    f"={data.get('solved', 0)}/{data.get('total', 1)}",  # Formula for percentage
                    data.get('last_solved', ''),
                    data.get('easy', 0),
                    data.get('medium', 0),
                    data.get('hard', 0)
                ]
                rows.append(row)
            
            # Batch update
            if rows:
                worksheet.update(f'A2:H{len(rows)+1}', rows)
                
                # Add percentage formatting
                worksheet.format(f'D2:D{len(rows)+1}', {
                    'numberFormat': {'type': 'PERCENT'}
                })
                
                logger.info(f"Updated Analytics sheet with {len(rows)} topics")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Analytics sheet: {e}")
            return False
    
    def update_progress_sheet(self, progress_data: List[Dict]) -> bool:
        """
        Update Progress sheet with daily progress data
        
        Args:
            progress_data: List of daily progress dictionaries
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            worksheet = self.spreadsheet.worksheet("Progress")
            
            # Clear existing data (keep headers)
            worksheet.clear()
            self._initialize_progress_sheet()
            
            # Prepare progress data
            rows = []
            for progress in progress_data:
                row = [
                    progress.get('date', ''),
                    progress.get('daily_count', 0),
                    progress.get('weekly_count', 0),
                    progress.get('monthly_count', 0),
                    progress.get('streak', 0),
                    progress.get('total_solved', 0),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows.append(row)
            
            # Batch update
            if rows:
                worksheet.update(f'A2:G{len(rows)+1}', rows)
                logger.info(f"Updated Progress sheet with {len(rows)} progress entries")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update Progress sheet: {e}")
            return False
    
    def get_existing_problems(self) -> List[Dict]:
        """
        Get existing problems from the sheet
        
        Returns:
            List of problem dictionaries
        """
        try:
            worksheet = self.spreadsheet.worksheet("Problems")
            data = worksheet.get_all_records()
            
            problems = []
            for row in data:
                if row.get('Problem Name'):  # Skip empty rows
                    problem = {
                        'title': row.get('Problem Name', ''),
                        'difficulty': row.get('Difficulty', ''),
                        'topics': row.get('Topics', '').split(', ') if row.get('Topics') else [],
                        'date_solved': row.get('Date Solved', ''),
                        'attempts': int(row.get('Attempts', 1)),
                        'status': row.get('Status', ''),
                        'problem_id': row.get('Problem ID', ''),
                        'last_updated': row.get('Last Updated', '')
                    }
                    problems.append(problem)
            
            return problems
            
        except Exception as e:
            logger.error(f"Failed to get existing problems: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test the connection to Google Sheets
        
        Returns:
            bool: True if connection is successful
        """
        try:
            # Try to access the spreadsheet
            title = self.spreadsheet.title
            logger.info(f"Successfully connected to spreadsheet: {title}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
