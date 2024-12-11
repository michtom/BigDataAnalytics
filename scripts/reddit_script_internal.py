import praw
import datetime
import json

user_agent = "Crypto Data Collector by /u//ShapeNeither6830"
username = "ShapeNeither6830"
password = '?8J_5"VT76v?kH-'
client_id = "-SdHvxTFoQIroQKWG3zg1Q"
client_secret = "f2klLxbZy48GxvpS0aLbIoRz8jI3Zg"

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    password=password,
    user_agent=user_agent,
    username=username,
)

subreddit = reddit.subreddit("CryptoMarkets")

current_time = datetime.datetime.now(datetime.timezone.utc).timestamp()
search_time_upper = current_time - 86400
search_time_lower = current_time - 2 * 86400

def reddit_posts_comments(subreddit):
    posts = []
    for submission in subreddit.new(limit=1000):
        if (submission.created_utc >= search_time_lower) and (submission.created_utc <= search_time_upper):
            post_id = submission.id
            post_title = submission.title
            post_time = submission.created_utc
            post_score = submission.score
            post_upvote_ratio = submission.upvote_ratio

            # get top-level comments
            submission.comments.replace_more(limit=0)
            comments = []
            for top_level_comment in submission.comments:
                comm_dict = {
                    "comm_id": top_level_comment.id,
                    "comm_body": top_level_comment.body,
                    "comm_time_utc": top_level_comment.created_utc,
                    "comm_score": top_level_comment.score
                }
                comments.append(comm_dict)

            post_dict = {
                "post_id": post_id,
                "post_title": post_title,
                "post_time": post_time,
                "post_score": post_score,
                "post_upvote": post_upvote_ratio,
                "comments": comments
            }
            posts.append(post_dict)
    return posts

# Write the output as JSON
output = json.dumps(reddit_posts_comments(subreddit), indent=4)
print(output)  # For debugging purposes

