import os
import redis
from dotenv import load_dotenv

load_dotenv()

r = redis.Redis.from_url(
    os.getenv(
        'REDIS_URL',
        'redis://redis:6379/0'
    ),
    decode_responses=True
)

RECENTS_KEY = 'cache:recents:all'
RECENTS_TTL = 300


def get_cached_recents() -> str | None:
    return r.get(RECENTS_KEY)


def set_cached_recents(data_json: str):
    r.setex(
        RECENTS_KEY,
        RECENTS_TTL,
        data_json
    )


def invalidate_recents_cache():
    r.delete(RECENTS_KEY)


def get_cached_preview(run_id: str) -> str | None:
    return r.get(f'cache:preview:{run_id}')


def set_cached_preview(
    run_id: str,
    html: str
):
    r.set(
        f'cache:preview:{run_id}',
        html
    )


RATE_LIMIT = int(
    os.getenv(
        'BLOG_GENERATE_RATE_LIMIT',
        '3'
    )
)

RATE_WINDOW = int(
    os.getenv(
        'BLOG_GENERATE_RATE_WINDOW_SECONDS',
        '60'
    )
)


def check_rate_limit(client_ip: str) -> bool:
    key = f'ratelimit:{client_ip}'

    current = r.incr(key)

    if current == 1:
        r.expire(key, RATE_WINDOW)

    return current <= RATE_LIMIT


def set_pipeline_status(
    run_id: str,
    node_name: str,
    progress_pct: int
):
    r.hset(
        f'pipeline:{run_id}',
        mapping={
            'current_node': node_name,
            'progress_pct': progress_pct
        }
    )


def get_pipeline_status(run_id: str):
    return r.hgetall(f'pipeline:{run_id}')


def append_completed_node(
    run_id: str,
    node_name: str
):
    r.rpush(
        f'pipeline:{run_id}:completed',
        node_name
    )


def get_completed_nodes(run_id: str):
    return r.lrange(
        f'pipeline:{run_id}:completed',
        0,
        -1
    )