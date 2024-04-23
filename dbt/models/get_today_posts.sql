select
    id,
    title,
    num_comments,
    score,
    author,
    created_utc,
    url,
    upvote_ratio,
    created_utc::date as utc_date,
    created_utc::time as utc_time
from dev.public.reddit
where DATE(created_utc) = CURRENT_DATE
