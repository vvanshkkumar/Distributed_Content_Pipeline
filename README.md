# Distributed Content Delivery Pipeline

A distributed system that takes a topic, researches it on the web, writes a full blog post using parallel AI workers, and delivers it by email — either immediately or on a schedule. Built to demonstrate real distributed systems engineering, not toy architecture.

---

## What This Actually Is


The writing itself is done by **three Kafka consumers running in parallel**, each picking up a different section of the blog and calling Gemini AI independently. The orchestrator does not wait for them — it publishes tasks and moves on. The reducer waits at the end, collects everything, and assembles the result in order. If any consumer crashes mid-write, Kafka re-delivers the task. If the file was already written before the crash, the consumer detects it and skips the AI call entirely. No section is ever written twice. No section is ever lost.

Email scheduling is handled by Celery with exponential backoff retries and a dead-letter queue. If sending fails three times, the job is archived rather than dropped. On AWS, two app instances both run the Beat scheduler — which normally causes duplicate sends — solved here with `celery-redbeat`, a Redis-distributed lock that lets only one Beat instance fire at a time.

Every pipeline step is observed by three independent observers simultaneously. One writes to PostgreSQL. One writes to Redis. One writes structured JSON logs. A Streamlit frontend polls Redis every two seconds to show live progress. The pipeline nodes do not know any of this is happening.

---

## Architecture

```
                              User
                               |
                    [ Streamlit Frontend ]
                    (login, generate, monitor)
                               |
                        HTTP + JWT token
                               |
                    [ Application Load Balancer ]
                               |
               +---------------+---------------+
               |                               |
    [ App EC2 Instance 1 ]         [ App EC2 Instance 2 ]
    FastAPI + Celery Worker        FastAPI + Celery Worker
    Celery Beat (ACTIVE)           Celery Beat (STANDBY)
    Blog Task Consumer             Blog Task Consumer
               |                               |
               +---------------+---------------+
                               |
          +--------------------+--------------------+
          |                    |                    |
   [ RDS PostgreSQL ]  [ ElastiCache Redis ]  [ Kafka EC2 ]
     (permanent data)    (cache, status,       (KRaft mode,
                          rate limits,          no ZooKeeper)
                          Celery broker)
                                                    |
                               +--------------------+
                               |
                     [ Kafka blog.tasks topic ]
                               |
             +-----------------+-----------------+
             |                 |                 |
     [ Worker 1 ]      [ Worker 2 ]      [ Worker 3 ]
       (2 partitions)   (2 partitions)   (2 partitions)
       Calls Gemini AI  Calls Gemini AI  Calls Gemini AI
             |                 |                 |
             +-----------------+-----------------+
                               |
                     [ Kafka blog.sections topic ]
                               |
                     [ Reducer / merge_content ]
                               |
                     [ Final Blog Saved to S3 ]
```

---

## Tech Stack

| Technology | Version | Role |
|---|---|---|
| Python | 3.12 or newer | Core language |
| FastAPI | 0.115 | HTTP API framework |
| LangGraph | 0.2.28 | Multi-agent pipeline orchestration |
| Google Gemini | gemini-2.0-flash-exp or newer | Blog content and image generation |
| Tavily | Latest | Web research API |
| Apache Kafka | 7.6.0 (KRaft) | Distributed parallel task queue |
| Celery | 5.4 | Background task runner |
| celery-redbeat | 2.2.0 | Distributed Beat scheduler with Redis lock |
| Redis | 7 | Caching, rate limiting, live status, Celery broker |
| PostgreSQL | 15 | Persistent storage |
| SQLAlchemy | 2.0.35 | ORM |
| python-jose | 3.3.0 | JWT token creation and verification |
| passlib + bcrypt | 1.7.4 | Password hashing |
| boto3 | 1.35 | AWS S3 file storage |
| Streamlit | 1.35 | Frontend UI |
| Docker + Compose | Latest | Local development |
| AWS EC2, RDS, ElastiCache, S3, ALB | — | Production cloud infrastructure |

**Kafka runs in KRaft mode throughout.** No ZooKeeper. One container instead of two, less memory, faster startup, and the correct modern approach — ZooKeeper mode is deprecated and removed in Kafka 4.0.

---

## Project Structure

```
Distributed_Content_Pipeline/
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py                          # FastAPI entry point
│       ├── database.py                      # SQLAlchemy engine + session factory
│       ├── models.py                        # All PostgreSQL table definitions
│       ├── kafka_config.py                  # Topic names, broker address, limits
│       ├── celery_app.py                    # Celery + redbeat configuration
│       ├── cache.py                         # All Redis operations
│       ├── auth/
│       │   ├── jwt_handler.py               # Token creation + verification
│       │   └── routing.py                   # /register and /login endpoints
│       ├── blog/
│       │   ├── schemas.py                   # Pydantic request/response models
│       │   ├── service.py                   # Pipeline runner + file saver
│       │   ├── routing.py                   # All 15 API endpoints
│       │   └── graph/
│       │       ├── state.py                 # BlogState TypedDict
│       │       ├── graph.py                 # Assembles and compiles the graph
│       │       └── nodes/
│       │           ├── router_node.py       # Decides research depth
│       │           ├── research_node.py     # Tavily web search
│       │           ├── orchestrator_node.py # Builds plan, publishes to Kafka
│       │           └── reducer/
│       │               ├── merge_content.py             # Waits for Kafka, assembles
│       │               ├── decide_images.py             # Identifies image positions
│       │               └── generate_and_place_images.py # Calls Google image API
│       ├── email/
│       │   └── strategy.py                 # EmailStrategy ABC, SMTP, factory
│       ├── observers/
│       │   ├── base.py                     # PipelineObserver abstract class
│       │   ├── publisher.py                # Singleton broadcaster
│       │   ├── audit_log_observer.py       # Writes to PostgreSQL
│       │   ├── redis_status_observer.py    # Writes live status to Redis
│       │   └── structured_log_observer.py  # Writes JSON log lines
│       ├── tasks/
│       │   └── email_tasks.py              # dispatch_due_emails + send_scheduled_email
│       └── workers/
│           └── blog_task_consumer.py       # Kafka consumer (3 replicas)
├── streamlit_app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                              # Landing page + login/register
│   ├── .streamlit/config.toml
│   ├── utils/
│   │   └── api_client.py                   # All HTTP calls to FastAPI
│   └── pages/
│       ├── 1_Generate.py                   # Blog generation + live progress bar
│       ├── 2_Live_Pipeline.py              # Infrastructure story (Redis/Kafka/PG/S3)
│       ├── 3_Analytics.py                  # Charts and metrics
│       ├── 4_Library.py                    # Browse, preview, send blogs
│       └── 5_Monitor.py                    # Failed jobs, scheduled emails
├── docker-compose.yml                      # Local development (7 services)
├── docker-compose.app.yml                  # AWS app instances (no PG/Redis/Kafka)
├── docker-compose.kafka.yml                # AWS Kafka EC2 (KRaft mode)
├── docker-compose.streamlit.yml            # AWS Streamlit EC2
└── .env                                    # Secrets — never committed
```

---

## How the Pipeline Works

A request to `POST /api/blog/generate` does the following, in order:

**1. Authentication and rate limiting**

FastAPI verifies the JWT token from the Authorization header. If valid, checks Redis — if this user has made 3 or more generate requests in the last 60 seconds, returns 429. Creates a `BlogRun` row in PostgreSQL with status `RUNNING`.

**2. Router node**

Scans the topic for keywords like `latest`, `compare`, `2025`, `best`. Assigns a mode: `closed_book` (no research), `hybrid` (some), or `open_book` (heavy). Sets `BlogState.needs_research`.

**3. Research node**

If research is needed, calls the Tavily Search API. Retrieves up to 8 results. Converts each into an `EvidenceItem` with content, source URL, and relevance score. Stores in `BlogState.evidence`. Gracefully skips if `TAVILY_API_KEY` is not set.

**4. Orchestrator node**

Sends the topic and evidence to Gemini AI with a prompt to create a structured blog plan. Parses the JSON response into a `Plan` object with 5-6 `Task` items. Publishes one Kafka message per task to the `blog.tasks` topic. Returns immediately — does not wait for workers.

**5. Kafka fan-out (parallel)**

Three blog worker containers, each assigned two Kafka partitions, consume tasks simultaneously. For each task:
- Check `SectionAttempt` table — if attempts exceed the limit, skip permanently
- Check if the section file already exists on disk (idempotency guard)
- If not, call Gemini AI with the section instructions
- Write the result to `data/blog_runs/{run_id}/sections/{task_id}.md`
- Publish the result to `blog.sections` Kafka topic
- Only then commit the Kafka offset

If a worker crashes after writing but before committing, Kafka re-delivers. The idempotency check finds the file and skips the AI call. The result is published again. The offset is committed. No duplicate AI call. No lost section.

**6. Reducer**

`merge_content_node` subscribes to `blog.sections` with a consumer group unique to this `run_id`. Waits until all expected sections arrive, with a 180-second timeout. Sorts sections by the original task order, not arrival order. Assembles into a single markdown document.

**7. Images**

`decide_images_node` identifies the first three headings as image candidates. `generate_and_place_images_node` calls the Google Imagen API. If image generation fails for any reason, a descriptive note is inserted instead. The blog always completes.

**8. Save and notify**

`service.py` saves the final markdown to S3 (if `S3_BUCKET_NAME` is set) or local disk. Updates `BlogRun.status` to `SUCCESS`. Invalidates the Redis recents cache. Returns the preview URL.

Throughout all of this, every node calls `publisher.on_node_enter()` and `publisher.on_node_exit()`. The publisher notifies three observers simultaneously — one writing to PostgreSQL, one to Redis, one to a structured log. An exception in one observer never affects the others or the pipeline.

---

## Design Patterns

### Observer Pattern — Pipeline Visibility

**Problem:** How do you give the Streamlit frontend real-time visibility into what a background process is doing, without coupling the pipeline to the frontend?

**Implementation:** Every LangGraph node calls a shared publisher singleton at its start and end. The publisher holds a list of observers and notifies all of them. Three observers are attached at application startup:

- `AuditLogObserver` — writes append-only rows to `pipeline_events` in PostgreSQL. Powers the history array in the `/status` endpoint.
- `RedisStatusObserver` — writes the current node name to a Redis hash. Streamlit polls this every two seconds.
- `StructuredLogObserver` — writes JSON log lines searchable by `run_id`, `node`, or `status`.

Adding a fourth observer — for example, a Slack alert on pipeline failure — requires one line in `main.py`. No node changes.

### Strategy Pattern — Email Provider

**Problem:** Gmail has a 500 emails/day limit. Switching providers at scale would require rewriting every email-sending call.

**Implementation:** `EmailStrategy` is an abstract base class. `SMTPStrategy` implements it for Gmail. `SendGridStrategy` is a placeholder. `email_strategy_factory()` reads `EMAIL_PROVIDER` from `.env` and returns the correct implementation. `email_tasks.py` calls `strategy.send()` and never knows which provider is underneath.

### JWT Authentication

Tokens are signed with `HS256` using a secret key from `.env`. Tokens expire after 24 hours. `get_current_user()` is a FastAPI `Depends()` function — protected endpoints declare it as a parameter and FastAPI handles the rest. The login endpoint uses the same error message for wrong username and wrong password, preventing user enumeration.

---

## Running Locally

**Prerequisites:** Docker Desktop, Python 3.12 or newer, Git.

```bash
# Clone and enter the project
git clone https://github.com/YOURUSERNAME/Distributed_Content_Pipeline.git
cd Distributed_Content_Pipeline

# Create the environment file
cp .env.example .env
# Edit .env with your actual keys

# Start all 7 services
# Kafka runs in KRaft mode — no ZooKeeper container needed
docker compose up --build

# Open the API documentation
open http://localhost:8000/docs

# Open the Streamlit frontend
open http://localhost:8501
```

**Required environment variables:**

```
GOOGLE_API_KEY         # Google AI Studio — aistudio.google.com/app/apikey
GEMINI_MODEL_NAME      # gemini-2.0-flash-exp
TAVILY_API_KEY         # Optional — app.tavily.com
EMAIL_ADDRESS          # Your Gmail address
EMAIL_PASSWORD         # Gmail App Password (16 characters, not your real password)
JWT_SECRET_KEY         # Generate: python -c "import secrets; print(secrets.token_hex(32))"
```

Database, Redis, and Kafka addresses are pre-configured for Docker and do not need to be changed for local development.

---

## API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/auth/register` | No | Create user account |
| `POST` | `/api/auth/login` | No | Get JWT token |
| `GET` | `/api/health/` | No | Database connection check |
| `GET` | `/api/recents/` | No | Last 10 blog runs (Redis cached) |
| `POST` | `/api/blog/generate` | **JWT** | Start blog generation |
| `GET` | `/api/blog/runs/{id}/status` | No | Live pipeline progress |
| `GET` | `/api/blog/runs/{id}/sections` | No | Per-section Kafka worker status |
| `GET` | `/api/blog/runs/{id}/preview` | No | Rendered HTML preview |
| `GET` | `/api/blog/runs/{id}/markdown` | No | Raw markdown content |
| `POST` | `/api/blog/send-existing` | **JWT** | Send blog by email immediately |
| `POST` | `/api/blog/schedule-existing` | **JWT** | Schedule email delivery |
| `GET` | `/api/blog/scheduled` | No | All scheduled emails |
| `GET` | `/api/jobs/failed` | **JWT** | Dead letter queue |
| `POST` | `/api/jobs/failed/{id}/retry` | **JWT** | Re-queue failed email job |

The rate limiter on `/api/blog/generate` uses the authenticated user's ID, not the IP address. A user cannot bypass the limit by switching networks.

---

## AWS Production Deployment

Production runs on AWS with the following infrastructure:

```
EC2 App Instance x2  (t4g.small, ARM/Graviton)
EC2 Kafka Instance   (t3.micro, KRaft mode)
EC2 Streamlit        (t4g.small)
ALB                  (Application Load Balancer)
RDS                  (PostgreSQL db.t3.micro)
ElastiCache          (Redis cache.t3.micro)
S3                   (Blog file storage)
Auto Scaling Group   (min 2, max 2, health check via /api/health/)
```

**Celery Beat across two instances:** Both app instances run `celery-beat`. Without coordination, both would fire the email dispatch schedule every 60 seconds, sending every email twice. `celery-redbeat` solves this with a Redis distributed lock — only the instance that wins the lock fires. If that instance goes down, the other picks up the lock within 60 seconds.

**S3 as shared file storage:** Blog files are written to S3 instead of local disk. When the ALB routes a preview request to Instance 2 but the blog was generated on Instance 1, Instance 2 reads the file from S3. Both instances always see the same files.

**Blog workers on both app instances:** All three `blog-task-consumer` replicas on both instances share the same Kafka consumer group. Kafka distributes partitions across all six consumers. If one app instance goes down, its consumers leave the group, Kafka rebalances, and the surviving instance's consumers take over.

---

## Database Schema

| Table | Purpose |
|---|---|
| `users` | Registered user accounts with bcrypt-hashed passwords |
| `blog_runs` | Every generation attempt — status, S3 key, word count, timestamps |
| `pipeline_events` | Append-only audit log — one row per node entry/exit event |
| `scheduled_emails` | Emails queued for future delivery — PENDING → QUEUED → SENT |
| `failed_jobs` | Dead letter queue — emails that failed all retry attempts |
| `section_attempts` | Kafka task delivery tracking — attempt count, status per section |

---

## Streamlit Frontend Pages

| Page | Description |
|---|---|
| Landing / Login | Authentication gate. Login and register tabs. Shows API health status. |
| Generate | Topic input, optional email and schedule. Live progress bar polling Redis every 2 seconds. Per-section Kafka worker status. |
| Live Infrastructure | Real-time narrative cards for PostgreSQL, Redis, Kafka, and S3 — each showing what it is doing at the current pipeline stage. Full audit trail from `pipeline_events`. |
| Analytics | pandas DataFrames, plotly charts — status distribution, generation volume over time, topic word frequency, email delivery funnel. |
| Library | All generated blogs. Preview, send now, or schedule email delivery. |
| Monitor | System health check, dead letter queue with retry button, scheduled email queue. |

---

## Known Limitations

**Rate limiter uses a fixed window, not a sliding window.** The current implementation resets the counter every 60 seconds from the first request. A user can send 3 requests at 11:59 and 3 more at 12:00 without hitting the limit. A sliding window using a Redis sorted set would close this gap. The fix is known, not yet implemented.

**Kafka is a single-node cluster.** One Kafka instance with replication factor 1. If the Kafka EC2 goes down, in-flight section tasks are lost. A production deployment would run a three-node Kafka cluster with replication factor 3. Single-node is an accepted trade-off for this project's cost and complexity goals.

**Celery Beat failover takes up to 60 seconds.** When the active Beat instance crashes, the other picks up the Redis lock within one lock timeout period. Any email that was due during those 60 seconds is sent on the next tick instead. Not a data loss — just a delay.

---

**Build by Vansh Kumar.**