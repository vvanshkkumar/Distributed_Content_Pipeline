import os, json, time, logging
from pathlib import Path
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
load_dotenv()

from src.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    BLOG_TASKS_TOPIC,
    BLOG_SECTIONS_TOPIC,
    SECTION_MAX_ATTEMPTS
)
from src.database import SessionLocal
from src.models import SectionAttempt
import google.generativeai as genai

genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
MODEL = os.getenv('BLOG_GEMINI_MODEL_NAME', 'gemini-2.0-flash-exp')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

def call_gemini(instructions: str, evidence: list) -> str:
    ev_text = ""
    if evidence:
        ev_text = "\n\nUse these research facts:\n"
        for e in evidence[:5]:
            ev_text += f"- {e.get('content', '')}\n"
    
    prompt = f"{instructions}{ev_text}\nWrite in markdown format."
    
    response = genai.GenerativeModel(MODEL).generate_content(prompt)
    return response.text

def section_exists(run_id: str, task_id: str) -> bool:
    path = Path(f'data/blog_runs/{run_id}/sections/{task_id}.md')
    return path.exists() and path.stat().st_size > 0

def write_section(run_id: str, task_id: str, content: str):
    path = Path(f'data/blog_runs/{run_id}/sections/{task_id}.md')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')

def run_consumer():
    consumer = KafkaConsumer(
        BLOG_TASKS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='blog-worker-group',
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    
    logger.info('Kafka consumer started KRaft mode (no ZooKeeper)')
    
    while True:
        try:
            records = consumer.poll(timeout_ms=5000)
            
            for topic_partition, messages in records.items():
                for msg in messages:
                    payload = msg.value
                    run_id = payload['run_id']
                    task_id = payload['task_id']
                    
                    db = SessionLocal()
                    try:
                        rec = db.query(SectionAttempt).filter_by(
                            run_id=run_id, task_id=task_id
                        ).first()
                        
                        if not rec:
                            rec = SectionAttempt(run_id=run_id, task_id=task_id)
                            db.add(rec); db.commit()
                            
                        if rec.attempts > SECTION_MAX_ATTEMPTS:
                            logger.error(json.dumps({
                                'event': 'section_permanently_failed',
                                'run_id': run_id, 'task_id': task_id,
                                'attempts': rec.attempts
                            }))
                            rec.status = 'PERMANENTLY_FAILED'; db.commit()
                            consumer.commit()
                            continue
                            
                        if section_exists(run_id, task_id):
                            logger.info(json.dumps({
                                'event': 'section_cache_hit',
                                'run_id': run_id, 'task_id': task_id,
                                'note': 'File exists, skipping Gemini call'
                            }))
                            md = Path(f'data/blog_runs/{run_id}/sections/{task_id}.md').read_text(encoding="utf-8")
                        else:
                            rec.attempts += 1
                            rec.status = 'PROCESSING'
                            rec.last_attempt_at = datetime.utcnow()
                            db.commit()
                            
                            md = call_gemini(
                                payload['instructions'],
                                payload.get('evidence', [])
                            )
                            
                        write_section(run_id, task_id, md)
                        
                        rec.status = 'DONE'; db.commit()
                        
                        producer.send(BLOG_SECTIONS_TOPIC, value={
                            'run_id': run_id,
                            'task_id': task_id,
                            'section_name': payload['section_name'],
                            'content': md
                        })
                        producer.flush()
                        
                        consumer.commit()
                        
                    except Exception as e:
                        rec.status = 'FAILED'
                        rec.error_message = str(e); db.commit()
                        logger.error(json.dumps({
                            'event': 'section_failed_no_commit',
                            'run_id': run_id, 'task_id': task_id,
                            'error': str(e),
                            'note': 'Kafka will re-deliver this message'
                        }))
                    finally:
                        db.close()
                        
        except Exception as e:
            logger.error(f'Consumer loop error: {e}. Restarting in 5s...')
            time.sleep(5)

if __name__ == '__main__':
    run_consumer()