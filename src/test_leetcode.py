import requests

url = "https://leetcode.com/graphql"
query = """
query getUser($username: String!) {
  matchedUser(username: $username) {
    username
    submitStats {
      acSubmissionNum {
        difficulty
        count
      }
    }
  }
}
"""
variables = {"username": "maybemnv"}

res = requests.post(url, json={"query": query, "variables": variables})
print(res.status_code)
print(res.text[:300])
