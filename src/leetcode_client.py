import requests
import json
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class LeetCodeClient:
    """LeetCode GraphQL API client for fetching user data"""
    
    def __init__(self, username: str, session_id: str = None, csrf_token: str = None):
        """
        Initialize LeetCode client
        
        Args:
            username: LeetCode username
            session_id: Session ID for authentication (optional)
            csrf_token: CSRF token for authentication (optional)
        """
        self.username = username
        self.base_url = "https://leetcode.com/graphql"
        self.session = requests.Session()
        # Set headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://leetcode.com/',
        })
        
        # Set authentication if provided
        if session_id:
            self.session.cookies.set('LEETCODE_SESSION', session_id, domain='.leetcode.com')
        if csrf_token:
            self.session.cookies.set('csrftoken', csrf_token, domain='.leetcode.com')
    
    def _make_graphql_request(self, query: str, variables: Dict = None, retries: int = 3) -> Optional[Dict]:
        """
        Make GraphQL request with retry logic
        
        Args:
            query: GraphQL query string
            variables: Variables for the query
            retries: Number of retry attempts
            
        Returns:
            Response data or None if failed
        """
        for attempt in range(retries):
            try:
                payload = {
                    'query': query,
                    'variables': variables or {}
                }
                
                response = self.session.post(
                    self.base_url,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if 'errors' in data:
                            logger.error(f"GraphQL errors: {data['errors']}")
                            return None
                        return data.get('data')
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON decode error on attempt {attempt + 1}: {e}")
                        logger.warning(f"Response content (first 200 chars): {response.text[:200]}")
                        # Check if it's an HTML response (rate limit page)
                        if response.text.strip().startswith('<'):
                            logger.warning("Received HTML response instead of JSON - likely rate limited")
                            time.sleep(5)  # Wait longer for rate limit
                        continue
                else:
                    logger.warning(f"Request failed with status {response.status_code}, attempt {attempt + 1}")
                    logger.warning(f"Response content: {response.text[:200]}")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request exception on attempt {attempt + 1}: {e}")
            
            # Exponential backoff
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
        
        logger.error(f"Failed to make GraphQL request after {retries} attempts")
        return None
    
    def get_user_submissions(self, limit: int = 1000) -> List[Dict]:
        """
        Fetch user submission history
        
        Args:
            limit: Maximum number of submissions to fetch
            
        Returns:
            List of submission dictionaries
        """
        query = """
        query userSubmissions($username: String!, $limit: Int!) {
            userProfileUserCalendar(username: $username) {
                submissionCalendar
            }
            recentSubmissionList(username: $username, limit: $limit) {
                title
                titleSlug
                timestamp
                statusDisplay
                lang
                runtime
                memory
                submissionId
            }
            matchedUser(username: $username) {
                submitStats {
                    acSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                    totalSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                }
            }
        }
        """
        
        variables = {
            'username': self.username,
            'limit': limit
        }
        
        data = self._make_graphql_request(query, variables)
        if not data:
            return []
        
        submissions = []
        
        # Process recent submissions
        if 'recentSubmissionList' in data:
            for submission in data['recentSubmissionList']:
                if submission.get('statusDisplay') == 'Accepted':
                    submissions.append({
                        'title': submission.get('title', ''),
                        'title_slug': submission.get('titleSlug', ''),
                        'timestamp': submission.get('timestamp', ''),
                        'status': submission.get('statusDisplay', ''),
                        'language': submission.get('lang', ''),
                        'runtime': submission.get('runtime', ''),
                        'memory': submission.get('memory', ''),
                        'submission_id': submission.get('submissionId', ''),
                        'date_solved': datetime.fromtimestamp(
                            int(submission.get('timestamp', 0))
                        ).strftime('%Y-%m-%d')
                    })
        
        # Process submission calendar for daily counts
        if 'userProfileUserCalendar' in data and data['userProfileUserCalendar']:
            calendar_data = data['userProfileUserCalendar'].get('submissionCalendar', '{}')
            try:
                calendar = json.loads(calendar_data)
                # Add daily submission counts to submissions
                for timestamp, count in calendar.items():
                    if count > 0:
                        date = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
                        # Find submissions for this date
                        for submission in submissions:
                            if submission.get('date_solved') == date:
                                submission['daily_count'] = count
                                break
            except json.JSONDecodeError:
                logger.warning("Failed to parse submission calendar")
        
        logger.info(f"Fetched {len(submissions)} accepted submissions for user {self.username}")
        return submissions
    
    def get_problem_details(self, problem_slug: str) -> Optional[Dict]:
        """
        Get detailed problem information
        
        Args:
            problem_slug: Problem slug/identifier
            
        Returns:
            Problem details dictionary or None
        """
        query = """
    query problemDetails($titleSlug: String!) {
        question(titleSlug: $titleSlug) {
            questionId
            title
            titleSlug
            difficulty
            topicTags {
                name
                slug
            }
            companyTagStats
            stats
            content
            isPaidOnly
            categoryTitle
        }
    }
     """   
        variables = {
            'titleSlug': problem_slug
        }
        
        data = self._make_graphql_request(query, variables)
        if not data or 'question' not in data:
            return None
        
        question = data['question']
        
        # Extract topics
        topics = []
        if 'topicTags' in question:
            topics = [tag.get('name', '') for tag in question['topicTags']]
        
        # Extract company tags
        companies = []
        if 'companyTagStats' in question and question['companyTagStats']:
            try:
                company_data = json.loads(question['companyTagStats'])
                if isinstance(company_data, list):
                   companies = [company.get('tagName', '') for company in company_data]
                elif isinstance(company_data, dict) and 'stats' in company_data:
                  companies = [stat.get('tagName', '') for stat in company_data['stats']]
            except (json.JSONDecodeError, TypeError):
             logger.warning(f"Failed to parse company tags for {problem_slug}")
        
      # Parse stats from JSON string
        acceptance_rate = 0
        total_accepted = 0
        total_submissions = 0
    
        if 'stats' in question and question['stats']:
            try:
              stats_data = json.loads(question['stats'])
              acceptance_rate = stats_data.get('acRate', 0)
              total_accepted = stats_data.get('totalAccepted', 0)
              total_submissions = stats_data.get('totalSubmission', 0)
            except (json.JSONDecodeError, TypeError):
             logger.warning(f"Failed to parse stats for {problem_slug}")
    
        problem_details = {
        'problem_id': question.get('questionId', ''),
        'title': question.get('title', ''),
        'title_slug': question.get('titleSlug', ''),
        'difficulty': question.get('difficulty', ''),
        'topics': topics,
        'companies': companies,
        'is_paid_only': question.get('isPaidOnly', False),
        'category': question.get('categoryTitle', ''),
        'acceptance_rate': acceptance_rate,
        'total_accepted': total_accepted,
        'total_submissions': total_submissions
    }
    
        return problem_details
    
    def get_user_statistics(self) -> Optional[Dict]:
        """
        Get comprehensive user statistics
        
        Returns:
            User statistics dictionary or None
        """
        query = """
        query userStats($username: String!) {
            matchedUser(username: $username) {
                username
                profile {
                    realName
                    countryName
                    starRating
                    aboutMe
                    userAvatar
                    ranking
                    reputation
                }
                submitStats {
                    acSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                    totalSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                }
                badges {
                    id
                    displayName
                    icon
                    category
                }
                upcomingBadges {
                    name
                    icon
                    progress
                }
            }
            userContestRanking(username: $username) {
                attendedContestsCount
                rating
                globalRanking
                totalParticipants
                topPercentage
                badge {
                    name
                    icon
                }
            }
        }
        """
        
        variables = {
            'username': self.username
        }
        
        data = self._make_graphql_request(query, variables)
        if not data:
            return None
        
        matched_user = data.get('matchedUser', {})
        contest_ranking = data.get('userContestRanking', {})
        
        # Process submission stats
        submission_stats = {}
        if 'submitStats' in matched_user:
            stats = matched_user['submitStats']
            
            # Accepted submissions by difficulty
            for stat in stats.get('acSubmissionNum', []):
                difficulty = stat.get('difficulty', '').lower()
                submission_stats[f'{difficulty}_solved'] = stat.get('count', 0)
                submission_stats[f'{difficulty}_submissions'] = stat.get('submissions', 0)
            
            # Total submissions by difficulty
            for stat in stats.get('totalSubmissionNum', []):
                difficulty = stat.get('difficulty', '').lower()
                submission_stats[f'{difficulty}_total'] = stat.get('count', 0)
        
        # Calculate totals
        total_solved = sum([
            submission_stats.get('easy_solved', 0),
            submission_stats.get('medium_solved', 0),
            submission_stats.get('hard_solved', 0)
        ])
        
        total_submissions = sum([
            submission_stats.get('easy_submissions', 0),
            submission_stats.get('medium_submissions', 0),
            submission_stats.get('hard_submissions', 0)
        ])
        
        user_stats = {
            'username': matched_user.get('username', ''),
            'real_name': matched_user.get('profile', {}).get('realName', ''),
            'country': matched_user.get('profile', {}).get('countryName', ''),
            'star_rating': matched_user.get('profile', {}).get('starRating', 0),
            'ranking': matched_user.get('profile', {}).get('ranking', 0),
            'reputation': matched_user.get('profile', {}).get('reputation', 0),
            'total_solved': total_solved,
            'total_submissions': total_submissions,
            'acceptance_rate': (total_solved / total_submissions * 100) if total_submissions > 0 else 0,
            'easy_solved': submission_stats.get('easy_solved', 0),
            'medium_solved': submission_stats.get('medium_solved', 0),
            'hard_solved': submission_stats.get('hard_solved', 0),
            'contest_rating': contest_ranking.get('rating', 0),
            'contest_rank': contest_ranking.get('globalRanking', 0),
            'contests_attended': contest_ranking.get('attendedContestsCount', 0),
            'badges': [
                {
                    'name': badge.get('displayName', ''),
                    'category': badge.get('category', ''),
                    'icon': badge.get('icon', '')
                }
                for badge in matched_user.get('badges', [])
            ]
        }
        
        return user_stats
    
    def get_all_solved_problems(self) -> List[Dict]:
        """
        Get all solved problems with full details
        
        Returns:
            List of solved problems with metadata
        """
        logger.info(f"Fetching all solved problems for user {self.username}")
        
        # Get user submissions
        submissions = self.get_user_submissions(limit=2000)
        if not submissions:
            return []
        
        # Get problem details for each submission
        solved_problems = []
        
        # Use ThreadPoolExecutor for concurrent API calls
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Create futures for problem details
            future_to_slug = {
                executor.submit(self.get_problem_details, sub['title_slug']): sub
                for sub in submissions
            }
            
            # Process completed futures
            for future in future_to_slug:
                submission = future_to_slug[future]
                try:
                    problem_details = future.result(timeout=10)
                    if problem_details:
                        # Merge submission and problem data
                        solved_problem = {
                            'title': problem_details.get('title', submission.get('title', '')),
                            'problem_id': problem_details.get('problem_id', ''),
                            'title_slug': problem_details.get('title_slug', submission.get('title_slug', '')),
                            'difficulty': problem_details.get('difficulty', ''),
                            'topics': problem_details.get('topics', []),
                            'companies': problem_details.get('companies', []),
                            'date_solved': submission.get('date_solved', ''),
                            'language': submission.get('language', ''),
                            'runtime': submission.get('runtime', ''),
                            'memory': submission.get('memory', ''),
                            'submission_id': submission.get('submission_id', ''),
                            'is_paid_only': problem_details.get('is_paid_only', False),
                            'category': problem_details.get('category', ''),
                            'acceptance_rate': problem_details.get('acceptance_rate', 0),
                            'attempts': 1,  # Default, could be enhanced with more API calls
                            'status': 'Solved'
                        }
                        solved_problems.append(solved_problem)
                        
                        # Rate limiting
                        time.sleep(0.1)
                    else:
                        logger.warning(f"Failed to get details for problem: {submission.get('title', '')}")
                        
                except Exception as e:
                    logger.error(f"Error processing problem {submission.get('title', '')}: {e}")
        
        logger.info(f"Successfully fetched details for {len(solved_problems)} solved problems")
        return solved_problems
    
    def test_connection(self) -> bool:
        """
        Test the connection to LeetCode
        
        Returns:
            bool: True if connection is successful
        """
        try:
            # First try a simple request to check if the API is accessible
            logger.info(f"Testing LeetCode connection for user: {self.username}")
            
            # Try to get user statistics
            stats = self.get_user_statistics()
            if stats and isinstance(stats, dict) and stats.get('username'):
                logger.info(f"Successfully connected to LeetCode for user: {stats.get('username', '')}")
                return True
            
            # If that fails, try a simpler approach - just check if we can reach the API
            logger.warning("User statistics failed, trying basic API test...")
            return self._test_basic_connection()
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
