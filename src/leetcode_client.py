import requests
import json
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class LeetCodeClient:
    """Production-ready LeetCode GraphQL client.

    Features & design decisions:
    - Robust handling of GraphQL responses (handles dict or JSON-string fields).
    - Brotli avoided by restricting Accept-Encoding to gzip/deflate.
    - Defensive programming: None checks, safe logging of binary content.
    - Deduplication of solved problems by slug to avoid redundant requests.
    - Concurrent fetching with a configurable worker pool and polite rate-limiting.
    - Clear errors and retry/backoff strategy.
    """

    DEFAULT_TIMEOUT = 30

    def __init__(
        self,
        username: str,
        session_id: Optional[str] = None,
        csrf_token: Optional[str] = None,
        max_workers: int = 5,
        request_timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        """
        Initialize the LeetCode client
        
        Args:
            username: LeetCode username (required)
            session_id: Optional session ID for private profiles (not required for public profiles)
            csrf_token: Optional CSRF token for private profiles (not required for public profiles)
            max_workers: Maximum number of concurrent workers
            request_timeout: Request timeout in seconds
        """
        self.username = username
        self.base_url = "https://leetcode.com/graphql"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LeetCodeClient/1.0)",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Referer": "https://leetcode.com/",
        })

        if session_id and csrf_token:
            self.session.cookies.set("LEETCODE_SESSION", session_id, domain="leetcode.com")
            self.session.headers["X-CSRFToken"] = csrf_token
       
        self.request_timeout = request_timeout
        self.max_workers = max_workers

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _safe_json_load(self, value):
        """Return dict from JSON string or return dict as-is; otherwise None."""
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                logger.debug("_safe_json_load: value is string but not valid JSON")
                return None
        return None

    def _parse_numeric_value(self, value):
        """Parse numeric values that might include percentage signs or other suffixes."""
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            # Remove percentage signs and other common suffixes
            value = value.strip().rstrip('%').rstrip('ms').rstrip('MB').strip()
            try:
                return float(value)
            except ValueError:
                logger.debug(f"Could not parse numeric value: {value}")
                return 0
        return 0

    def _post(self, payload: Dict, retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(1, retries + 1):
            try:
                # Log request details
                logger.debug(f"Sending request to {self.base_url}")
                logger.debug(f"Headers: {self.session.headers}")
                logger.debug(f"Cookies: {self.session.cookies.get_dict()}")
                logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
                
                resp = self.session.post(self.base_url, json=payload, timeout=self.request_timeout)
                
                # Log response details
                logger.debug(f"Response status: {resp.status_code}")
                logger.debug(f"Response headers: {dict(resp.headers)}")
                logger.debug(f"Response content: {resp.text[:1000]}")
                
                # Accept code 200 only
                if resp.status_code == 200:
                    return resp
                    
                logger.warning("GraphQL POST returned status %s (attempt %s)", resp.status_code, attempt)
                
                # Try to get error details from response
                try:
                    error_data = resp.json()
                    if 'errors' in error_data:
                        logger.error(f"GraphQL errors: {error_data['errors']}")
                except:
                    logger.debug("Could not parse error response as JSON")
                
            except requests.exceptions.RequestException as exc:
                logger.warning("Request exception on attempt %s: %s", attempt, exc, exc_info=True)

            # backoff
            sleep_for = 2 ** (attempt - 1)
            time.sleep(sleep_for)

        logger.error("Failed to POST after %s attempts", retries)
        return None

    def _make_graphql_request(self, query: str, variables: Dict = None, retries: int = 3) -> Optional[Dict]:
        payload = {"query": query, "variables": variables or {}}
        resp = self._post(payload, retries=retries)
        if not resp:
            return None

        # Try to decode JSON safely
        try:
            data = resp.json()
        except ValueError:
            # Not valid JSON â€" log safe binary preview and return None
            logger.warning("Non-JSON response received; content preview: %r", resp.content[:300])
            return None

        if not isinstance(data, dict):
            logger.warning("Unexpected response format from GraphQL: %r", type(data))
            return None

        if "errors" in data:
            logger.error("GraphQL errors: %s", data.get("errors"))
            return None

        return data.get("data")

    # -----------------------------
    # Public API
    # -----------------------------
    def get_user_statistics(self) -> Optional[Dict]:
        """Fetch high-level user statistics. Returns dict or None on failure."""
        query = """
        query userStats($username: String!) {
          matchedUser(username: $username) {
            username
            profile { realName countryName starRating aboutMe userAvatar ranking reputation }
            submitStats { acSubmissionNum { difficulty count submissions } totalSubmissionNum { difficulty count submissions } }
            badges { id displayName icon category }
            upcomingBadges { name icon progress }
          }
          userContestRanking(username: $username) { attendedContestsCount rating globalRanking totalParticipants topPercentage badge { name icon } }
        }
        """

        data = self._make_graphql_request(query, {"username": self.username})
        if not data:
            return None

        matched_user = data.get("matchedUser") or {}
        contest_ranking = data.get("userContestRanking") or {}

        submit_stats = matched_user.get("submitStats") or {}

        submission_stats = {}
        for stat in submit_stats.get("acSubmissionNum", []) or []:
            difficulty = (stat.get("difficulty") or "").lower()
            submission_stats[f"{difficulty}_solved"] = stat.get("count", 0)
            submission_stats[f"{difficulty}_submissions"] = stat.get("submissions", 0)

        for stat in submit_stats.get("totalSubmissionNum", []) or []:
            difficulty = (stat.get("difficulty") or "").lower()
            submission_stats[f"{difficulty}_total"] = stat.get("count", 0)

        easy = submission_stats.get("easy_solved", 0)
        medium = submission_stats.get("medium_solved", 0)
        hard = submission_stats.get("hard_solved", 0)
        total_solved = easy + medium + hard

        total_submissions = (
            submission_stats.get("easy_submissions", 0)
            + submission_stats.get("medium_submissions", 0)
            + submission_stats.get("hard_submissions", 0)
        )

        user_stats = {
            "username": matched_user.get("username", ""),
            "real_name": (matched_user.get("profile") or {}).get("realName", ""),
            "country": (matched_user.get("profile") or {}).get("countryName", ""),
            "star_rating": (matched_user.get("profile") or {}).get("starRating", 0),
            "ranking": (matched_user.get("profile") or {}).get("ranking", 0),
            "reputation": (matched_user.get("profile") or {}).get("reputation", 0),
            "total_solved": total_solved,
            "total_submissions": total_submissions,
            "acceptance_rate": (total_solved / total_submissions * 100) if total_submissions > 0 else 0,
            "easy_solved": easy,
            "medium_solved": medium,
            "hard_solved": hard,
            "contest_rating": contest_ranking.get("rating", 0),
            "contest_rank": contest_ranking.get("globalRanking", 0),
            "contests_attended": contest_ranking.get("attendedContestsCount", 0),
            "badges": [
                {"name": b.get("displayName", ""), "category": b.get("category", ""), "icon": b.get("icon", "")} for b in (matched_user.get("badges") or [])
            ],
        }

        return user_stats

    def get_user_submissions(self, limit: int = 10000) -> List[Dict]:
        """Fetch all submission list and calendar. Returns list of accepted submissions."""
        # First, get all-time submission calendar
        calendar_query = """
        query userProgressCalendar($username: String!) {
          allQuestionsCount {
            difficulty
            count
          }
          matchedUser(username: $username) {
            userCalendar {
              submissionCalendar
            }
          }
        }
        """
        
        # Then get all submissions with a large limit
        submissions_query = """
        query recentSubmissions($username: String!, $limit: Int!) {
          recentSubmissionList(username: $username, limit: $limit) {
            title 
            titleSlug 
            timestamp 
            statusDisplay 
            lang 
            runtime 
            memory
          }
          matchedUser(username: $username) { 
            submitStats { 
              acSubmissionNum { 
                difficulty 
                count 
                submissions 
              } 
            } 
          }
        }
        """

        # Get all-time calendar data
        calendar_data = self._make_graphql_request(calendar_query, {"username": self.username})
        
        # Get all submissions data with increased limit
        data = self._make_graphql_request(submissions_query, {"username": self.username, "limit": limit})
        if not data:
            return []

        recent = data.get("recentSubmissionList") or []
        submissions = []
        for s in recent:
            if s.get("statusDisplay") != "Accepted":
                continue
            timestamp = s.get("timestamp")
            try:
                date_solved = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d") if timestamp else ""
            except Exception:
                date_solved = ""

            submission_data = {
                "title": s.get("title", ""),
                "title_slug": s.get("titleSlug", ""),
                "timestamp": timestamp,
                "status": s.get("statusDisplay", ""),
                "language": s.get("lang", ""),
                "runtime": s.get("runtime", ""),
                "memory": s.get("memory", ""),
                "submission_id": "",  # submissionId is no longer available in the API
                "date_solved": date_solved,
            }
            submissions.append(submission_data)

        # Process calendar data if available
        if calendar_data and calendar_data.get("matchedUser"):
            user_calendar = calendar_data.get("matchedUser", {}).get("userCalendar", {})
            cal_raw = user_calendar.get("submissionCalendar")
            cal = self._safe_json_load(cal_raw) or {}
            if isinstance(cal, dict):
                # merge daily counts into submissions where dates match
                date_to_count = {}
                for timestamp, count in cal.items():
                    try:
                        date_str = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
                        date_to_count[date_str] = count
                    except (ValueError, TypeError):
                        continue
                        
                for sub in submissions:
                    sub["daily_count"] = date_to_count.get(sub.get("date_solved", ""), 0)

        logger.info("Fetched %s accepted submissions for %s", len(submissions), self.username)
        return submissions

    def get_problem_details(self, problem_slug: str) -> Optional[Dict]:
        """Fetch detailed problem metadata for a given slug."""
        query = """
        query problemDetails($titleSlug: String!) {
          question(titleSlug: $titleSlug) {
            questionId title titleSlug difficulty topicTags { name slug }
            companyTagStats stats content isPaidOnly categoryTitle
          }
        }
        """

        data = self._make_graphql_request(query, {"titleSlug": problem_slug})
        if not data:
            return None

        q = data.get("question")
        if not q:
            return None

        # topics are typically a list
        topics = [t.get("name", "") for t in (q.get("topicTags") or [])]

        # companyTagStats may be a string or dict
        companies = []
        cts = q.get("companyTagStats")
        cdata = self._safe_json_load(cts)
        if isinstance(cdata, dict):
            if "stats" in cdata and isinstance(cdata["stats"], list):
                companies = [entry.get("tagName", "") for entry in cdata["stats"]]
        elif isinstance(cdata, list):
            companies = [entry.get("tagName", "") for entry in cdata]

        stats_raw = q.get("stats")
        stats_parsed = self._safe_json_load(stats_raw) or {}
        acceptance_rate = stats_parsed.get("acRate", 0)
        total_accepted = stats_parsed.get("totalAccepted", 0)
        total_submissions = stats_parsed.get("totalSubmission", 0)

        return {
            "problem_id": q.get("questionId", ""),
            "title": q.get("title", ""),
            "title_slug": q.get("titleSlug", ""),
            "difficulty": q.get("difficulty", ""),
            "topics": topics,
            "companies": companies,
            "is_paid_only": q.get("isPaidOnly", False),
            "category": q.get("categoryTitle", ""),
            "acceptance_rate": acceptance_rate,
            "total_accepted": total_accepted,
            "total_submissions": total_submissions,
        }

    def get_all_solved_problems(self) -> List[Dict]:
        """Return deduplicated solved problems with details. Concurrent and rate-limited."""
        submissions = self.get_user_submissions(limit=2000)
        if not submissions:
            return []

        # Deduplicate by slug keeping earliest occurrence
        unique = {}
        for s in submissions:
            slug = s.get("title_slug")
            if not slug:
                continue
            if slug not in unique:
                unique[slug] = s

        slugs = list(unique.keys())
        results = []

        # concurrent fetch with polite rate-limiting
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.get_problem_details, slug): slug for slug in slugs}
            for fut in as_completed(futures):
                slug = futures[fut]
                try:
                    details = fut.result()
                    if details:
                        sub = unique.get(slug, {})
                        merged = {
                            "title": details.get("title", sub.get("title", "")),
                            "problem_id": details.get("problem_id", ""),
                            "title_slug": slug,
                            "difficulty": details.get("difficulty", ""),
                            "topics": details.get("topics", []),
                            "companies": details.get("companies", []),
                            "date_solved": sub.get("date_solved", ""),
                            "language": sub.get("language", ""),
                            "runtime": self._parse_numeric_value(sub.get("runtime", "")),
                            "memory": self._parse_numeric_value(sub.get("memory", "")),
                            "submission_id": sub.get("submission_id", ""),
                            "is_paid_only": details.get("is_paid_only", False),
                            "category": details.get("category", ""),
                            "acceptance_rate": self._parse_numeric_value(details.get("acceptance_rate", 0)),
                            "attempts": 1,
                            "status": "Solved",
                        }
                        results.append(merged)
                except Exception as exc:
                    logger.error("Failed to fetch details for %s: %s", slug, exc)
                finally:
                    # small sleep to avoid aggressive hammering
                    time.sleep(0.05)

        logger.info("Fetched details for %s solved problems", len(results))
        return results

    def _test_basic_connection(self) -> bool:
        """A simple non-GraphQL check: fetch the user profile page HTML and ensure 200."""
        try:
            url = f"https://leetcode.com/{self.username}/"
            r = self.session.get(url, timeout=self.request_timeout)
            logger.debug("Basic connection status: %s", r.status_code)
            return r.status_code == 200
        except requests.RequestException as exc:
            logger.warning("Basic connection failed: %s", exc)
            return False

    def test_connection(self) -> bool:
        """Comprehensive test: GraphQL stats + fallback basic HTML check."""
        logger.info("Testing LeetCode connection for user: %s", self.username)
        stats = self.get_user_statistics()
        if stats and isinstance(stats, dict) and stats.get("username"):
            logger.info("GraphQL connection ok for %s", stats.get("username"))
            return True

        logger.warning("GraphQL stats failed; attempting basic connectivity check")
        basic = self._test_basic_connection()
        if basic:
            logger.info("Basic connectivity succeeded, but GraphQL failed  investigate rate limiting or headers")
        else:
            logger.error("Both GraphQL and basic checks failed")
        return basic