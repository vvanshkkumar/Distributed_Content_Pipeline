import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from src.database import engine
from src import models


from src.observers.publisher import publisher
from src.observers.audit_log_observer import AuditLogObserver
from src.observers.redis_status_observer import RedisStatusObserver
from src.observers.structured_log_observer import StructuredLogObserver


from src.auth.routing import router as auth_router
from src.blog.routing import router as blog_router


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Code inside this function runs ONCE at application startup.
    The 'yield' separates startup (above) from shutdown (below).
    """
    logger.info("*" * 50)
    logger.info("Distributed Content Pipeline starting up")
    logger.info("*" * 50)
    
   
    logger.info("Creating database tables...")
    models.Base.metadata.create_all(bind=engine)
    logger.info("All tables ready.")
    
   
    logger.info("Attaching pipeline observers...")
    publisher.attach(AuditLogObserver())        # writes to PostgreSQL
    publisher.attach(RedisStatusObserver())     # writes to Redis
    publisher.attach(StructuredLogObserver())   # writes JSON log lines
    logger.info("3 observers attached.")
    
    logger.info("API ready. Visit /docs for interactive documentation.")
    
    
    
   
    logger.info("Application shutting down...")



app = FastAPI(
    title="Distributed Content Delivery Pipeline",
    description="AI blog generation with JWT auth, Kafka parallelism, Celery scheduling, and real-time pipeline monitoring.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware allows the Streamlit frontend to call the FastAPI backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(auth_router)
app.include_router(blog_router)