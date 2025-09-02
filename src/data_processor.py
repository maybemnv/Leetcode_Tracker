import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json

logger = logging.getLogger(__name__)

class DataProcessor:
    """Data processing and analytics for LeetCode problems"""
    
    def __init__(self, topic_mapping: Dict = None):
        """
        Initialize data processor
        
        Args:
            topic_mapping: Dictionary mapping LeetCode topics to custom categories
        """
        self.topic_mapping = topic_mapping or {}
        
        # Default topic categories if no mapping provided
        self.default_topics = [
            "Array", "String", "Hash Table", "Dynamic Programming", "Math",
            "Sorting", "Greedy", "Depth-First Search", "Binary Search", "Database",
            "Breadth-First Search", "Tree", "Matrix", "Two Pointers", "Bit Manipulation",
            "Stack", "Heap (Priority Queue)", "Graph", "Design", "Backtracking",
            "Sliding Window", "Union Find", "Linked List", "Recursion", "Monotonic Stack",
            "Binary Tree", "Trie", "Divide and Conquer", "Ordered Set", "Geometry",
            "Game Theory", "Segment Tree", "Topological Sort", "Number Theory",
            "Binary Indexed Tree", "Queue", "Brainteaser", "Memoization", "Minimax",
            "Reservoir Sampling", "Quickselect", "Eulerian Circuit", "Radix Sort",
            "Strongly Connected Component", "Shortest Path", "Data Stream", "Iterator",
            "Rolling Hash", "Monotonic Queue", "Randomized", "Enumeration",
            "Probability and Statistics", "Rejection Sampling", "Suffix Array",
            "Concurrency", "Minimum Spanning Tree", "Biconnected Component"
        ]
    
    def categorize_by_topic(self, problems: List[Dict]) -> Dict:
        """
        Group problems by topic with statistics
        
        Args:
            problems: List of problem dictionaries
            
        Returns:
            Dictionary with topic-wise statistics
        """
        topic_stats = defaultdict(lambda: {
            'total': 0,
            'solved': 0,
            'easy': 0,
            'medium': 0,
            'hard': 0,
            'last_solved': '',
            'problems': []
        })
        
        for problem in problems:
            topics = problem.get('topics', [])
            difficulty = problem.get('difficulty', '').lower()
            date_solved = problem.get('date_solved', '')
            
            # Map topics to custom categories if mapping exists
            mapped_topics = self._map_topics(topics)
            
            for topic in mapped_topics:
                topic_stats[topic]['total'] += 1
                topic_stats[topic]['solved'] += 1
                topic_stats[topic]['problems'].append(problem)
                
                # Count by difficulty
                if difficulty == 'easy':
                    topic_stats[topic]['easy'] += 1
                elif difficulty == 'medium':
                    topic_stats[topic]['medium'] += 1
                elif difficulty == 'hard':
                    topic_stats[topic]['hard'] += 1
                
                # Track last solved date
                if date_solved:
                    if not topic_stats[topic]['last_solved'] or date_solved > topic_stats[topic]['last_solved']:
                        topic_stats[topic]['last_solved'] = date_solved
        
        # Convert defaultdict to regular dict and remove problems list for sheets
        result = {}
        for topic, stats in topic_stats.items():
            result[topic] = {
                'total': stats['total'],
                'solved': stats['solved'],
                'easy': stats['easy'],
                'medium': stats['medium'],
                'hard': stats['hard'],
                'last_solved': stats['last_solved']
            }
        
        logger.info(f"Categorized {len(problems)} problems into {len(result)} topics")
        return result
    
    def _map_topics(self, topics: List[str]) -> List[str]:
        """
        Map LeetCode topics to custom categories
        
        Args:
            topics: List of original LeetCode topics
            
        Returns:
            List of mapped topic categories
        """
        if not self.topic_mapping:
            return topics
        
        mapped_topics = set()
        for topic in topics:
            # Check if topic has a direct mapping
            if topic in self.topic_mapping:
                mapped_topics.add(self.topic_mapping[topic])
            else:
                # Check for partial matches
                for key, value in self.topic_mapping.items():
                    if key.lower() in topic.lower() or topic.lower() in key.lower():
                        mapped_topics.add(value)
                        break
                else:
                    # Keep original topic if no mapping found
                    mapped_topics.add(topic)
        
        return list(mapped_topics)
    
    def calculate_progress_metrics(self, problems: List[Dict]) -> Dict:
        """
        Calculate progress metrics including streaks and patterns
        
        Args:
            problems: List of problem dictionaries
            
        Returns:
            Dictionary with progress metrics
        """
        if not problems:
            return {}
        
        # Sort problems by date solved
        sorted_problems = sorted(
            [p for p in problems if p.get('date_solved')],
            key=lambda x: x.get('date_solved', '')
        )
        
        # Calculate daily progress
        daily_progress = self._calculate_daily_progress(sorted_problems)
        
        # Calculate streaks
        current_streak, longest_streak = self._calculate_streaks(sorted_problems)
        
        # Calculate solving patterns
        patterns = self._analyze_solving_patterns(sorted_problems)
        
        # Calculate difficulty progression
        difficulty_progression = self._analyze_difficulty_progression(sorted_problems)
        
        metrics = {
            'total_problems': len(problems),
            'total_solved': len(sorted_problems),
            'current_streak': current_streak,
            'longest_streak': longest_streak,
            'daily_progress': daily_progress,
            'patterns': patterns,
            'difficulty_progression': difficulty_progression,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        logger.info(f"Calculated progress metrics: {current_streak} day streak, {len(sorted_problems)} total solved")
        return metrics
    
    def _calculate_daily_progress(self, problems: List[Dict]) -> List[Dict]:
        """
        Calculate daily progress data
        
        Args:
            problems: List of problems sorted by date
            
        Returns:
            List of daily progress entries
        """
        daily_counts = defaultdict(int)
        
        for problem in problems:
            date = problem.get('date_solved', '')
            if date:
                daily_counts[date] += 1
        
        # Convert to list format for sheets
        progress_entries = []
        total_solved = 0
        
        for date in sorted(daily_counts.keys()):
            daily_count = daily_counts[date]
            total_solved += daily_count
            
            # Calculate weekly and monthly counts
            week_start = self._get_week_start(date)
            month_start = self._get_month_start(date)
            
            weekly_count = sum(
                daily_counts[d] for d in daily_counts.keys()
                if self._get_week_start(d) == week_start
            )
            
            monthly_count = sum(
                daily_counts[d] for d in daily_counts.keys()
                if self._get_month_start(d) == month_start
            )
            
            progress_entries.append({
                'date': date,
                'daily_count': daily_count,
                'weekly_count': weekly_count,
                'monthly_count': monthly_count,
                'total_solved': total_solved,
                'streak': 0  # Will be calculated separately
            })
        
        return progress_entries
    
    def _get_week_start(self, date_str: str) -> str:
        """Get the start of the week for a given date"""
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            # Monday is 0, Sunday is 6
            days_since_monday = date.weekday()
            week_start = date - timedelta(days=days_since_monday)
            return week_start.strftime('%Y-%m-%d')
        except ValueError:
            return date_str
    
    def _get_month_start(self, date_str: str) -> str:
        """Get the start of the month for a given date"""
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            month_start = date.replace(day=1)
            return month_start.strftime('%Y-%m-%d')
        except ValueError:
            return date_str
    
    def _calculate_streaks(self, problems: List[Dict]) -> tuple:
        """
        Calculate current and longest streaks
        
        Args:
            problems: List of problems sorted by date
            
        Returns:
            Tuple of (current_streak, longest_streak)
        """
        if not problems:
            return 0, 0
        
        dates = [p.get('date_solved', '') for p in problems if p.get('date_solved')]
        if not dates:
            return 0, 0
        
        # Convert dates to datetime objects
        date_objects = []
        for date_str in dates:
            try:
                date_objects.append(datetime.strptime(date_str, '%Y-%m-%d'))
            except ValueError:
                continue
        
        if not date_objects:
            return 0, 0
        
        date_objects.sort()
        
        # Calculate streaks
        current_streak = 0
        longest_streak = 0
        temp_streak = 0
        
        today = datetime.now().date()
        
        for i, date in enumerate(date_objects):
            if i == 0:
                temp_streak = 1
            else:
                # Check if consecutive days
                prev_date = date_objects[i-1].date()
                curr_date = date.date()
                
                if (curr_date - prev_date).days == 1:
                    temp_streak += 1
                else:
                    # Streak broken
                    longest_streak = max(longest_streak, temp_streak)
                    temp_streak = 1
            
            # Check if this is part of current streak
            if (today - date.date()).days <= 1:
                current_streak = temp_streak
        
        longest_streak = max(longest_streak, temp_streak)
        
        return current_streak, longest_streak
    
    def _analyze_solving_patterns(self, problems: List[Dict]) -> Dict:
        """
        Analyze solving patterns and trends
        
        Args:
            problems: List of problems sorted by date
            
        Returns:
            Dictionary with pattern analysis
        """
        if not problems:
            return {}
        
        # Analyze by time of day (if timestamp available)
        time_patterns = defaultdict(int)
        
        # Analyze by day of week
        day_patterns = defaultdict(int)
        
        # Analyze by difficulty over time
        difficulty_trends = []
        
        for problem in problems:
            date_str = problem.get('date_solved', '')
            difficulty = problem.get('difficulty', '').lower()
            
            if date_str:
                try:
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    day_name = date.strftime('%A')
                    day_patterns[day_name] += 1
                    
                    # Track difficulty progression
                    difficulty_trends.append({
                        'date': date_str,
                        'difficulty': difficulty
                    })
                    
                except ValueError:
                    continue
        
        # Calculate most productive days
        most_productive_day = max(day_patterns.items(), key=lambda x: x[1]) if day_patterns else ('', 0)
        
        # Analyze difficulty progression
        difficulty_progression = self._analyze_difficulty_progression(problems)
        
        patterns = {
            'most_productive_day': most_productive_day[0],
            'day_distribution': dict(day_patterns),
            'difficulty_trends': difficulty_trends,
            'difficulty_progression': difficulty_progression
        }
        
        return patterns
    
    def _analyze_difficulty_progression(self, problems: List[Dict]) -> Dict:
        """
        Analyze how difficulty level changes over time
        
        Args:
            problems: List of problems sorted by date
            
        Returns:
            Dictionary with difficulty progression analysis
        """
        if not problems:
            return {}
        
        # Group by month to see progression
        monthly_difficulty = defaultdict(lambda: {'easy': 0, 'medium': 0, 'hard': 0})
        
        for problem in problems:
            date_str = problem.get('date_solved', '')
            difficulty = problem.get('difficulty', '').lower()
            
            if date_str and difficulty:
                try:
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    month_key = date.strftime('%Y-%m')
                    
                    if difficulty in ['easy', 'medium', 'hard']:
                        monthly_difficulty[month_key][difficulty] += 1
                        
                except ValueError:
                    continue
        
        # Calculate progression metrics
        progression = {
            'monthly_breakdown': dict(monthly_difficulty),
            'difficulty_ratio': self._calculate_difficulty_ratio(problems),
            'complexity_trend': self._calculate_complexity_trend(problems)
        }
        
        return progression
    
    def _calculate_difficulty_ratio(self, problems: List[Dict]) -> Dict:
        """Calculate ratio of problems by difficulty"""
        difficulty_counts = Counter(p.get('difficulty', '').lower() for p in problems)
        total = sum(difficulty_counts.values())
        
        if total == 0:
            return {}
        
        return {
            'easy': difficulty_counts.get('easy', 0) / total,
            'medium': difficulty_counts.get('medium', 0) / total,
            'hard': difficulty_counts.get('hard', 0) / total
        }
    
    def _calculate_complexity_trend(self, problems: List[Dict]) -> str:
        """Determine if user is solving more complex problems over time"""
        if len(problems) < 10:
            return "Insufficient data"
        
        # Split problems into first and second half
        mid_point = len(problems) // 2
        first_half = problems[:mid_point]
        second_half = problems[mid_point:]
        
        # Calculate average difficulty (1=easy, 2=medium, 3=hard)
        def get_difficulty_score(problem):
            diff = problem.get('difficulty', '').lower()
            if diff == 'easy':
                return 1
            elif diff == 'medium':
                return 2
            elif diff == 'hard':
                return 3
            return 0
        
        first_avg = sum(get_difficulty_score(p) for p in first_half) / len(first_half)
        second_avg = sum(get_difficulty_score(p) for p in second_half) / len(second_half)
        
        if second_avg > first_avg + 0.2:
            return "Improving - solving harder problems"
        elif second_avg < first_avg - 0.2:
            return "Focusing on easier problems"
        else:
            return "Consistent difficulty level"
    
    def generate_analytics(self, problems: List[Dict]) -> Dict:
        """
        Generate comprehensive analytics for sheets
        
        Args:
            problems: List of problem dictionaries
            
        Returns:
            Dictionary with all analytics data
        """
        logger.info(f"Generating analytics for {len(problems)} problems")
        
        # Generate all analytics
        topic_analytics = self.categorize_by_topic(problems)
        progress_metrics = self.calculate_progress_metrics(problems)
        
        # Prepare data for sheets
        analytics_data = {
            'topic_analytics': topic_analytics,
            'progress_data': progress_metrics.get('daily_progress', []),
            'summary_stats': {
                'total_problems': len(problems),
                'total_solved': len([p for p in problems if p.get('date_solved')]),
                'current_streak': progress_metrics.get('current_streak', 0),
                'longest_streak': progress_metrics.get('longest_streak', 0),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        logger.info("Analytics generation completed")
        return analytics_data
    
    def validate_problem_data(self, problems: List[Dict]) -> List[Dict]:
        """
        Validate and clean problem data
        
        Args:
            problems: List of problem dictionaries
            
        Returns:
            List of validated problem dictionaries
        """
        validated_problems = []
        
        for problem in problems:
            # Basic validation
            if not problem.get('title'):
                logger.warning(f"Skipping problem without title: {problem}")
                continue
            
            # Clean and normalize data
            cleaned_problem = {
                'title': str(problem.get('title', '')).strip(),
                'problem_id': str(problem.get('problem_id', '')).strip(),
                'title_slug': str(problem.get('title_slug', '')).strip(),
                'difficulty': str(problem.get('difficulty', '')).strip().title(),
                'topics': [str(topic).strip() for topic in problem.get('topics', []) if topic],
                'companies': [str(company).strip() for company in problem.get('companies', []) if company],
                'date_solved': str(problem.get('date_solved', '')).strip(),
                'language': str(problem.get('language', '')).strip(),
                'runtime': str(problem.get('runtime', '')).strip(),
                'memory': str(problem.get('memory', '')).strip(),
                'submission_id': str(problem.get('submission_id', '')).strip(),
                'is_paid_only': bool(problem.get('is_paid_only', False)),
                'category': str(problem.get('category', '')).strip(),
                'acceptance_rate': float(problem.get('acceptance_rate', 0)),
                'attempts': int(problem.get('attempts', 1)),
                'status': str(problem.get('status', 'Solved')).strip()
            }
            
            # Validate difficulty
            if cleaned_problem['difficulty'].lower() not in ['easy', 'medium', 'hard']:
                cleaned_problem['difficulty'] = 'Unknown'
            
            # Validate date format
            if cleaned_problem['date_solved']:
                try:
                    datetime.strptime(cleaned_problem['date_solved'], '%Y-%m-%d')
                except ValueError:
                    logger.warning(f"Invalid date format for problem {cleaned_problem['title']}: {cleaned_problem['date_solved']}")
                    cleaned_problem['date_solved'] = ''
            
            validated_problems.append(cleaned_problem)
        
        logger.info(f"Validated {len(validated_problems)} problems")
        return validated_problems
