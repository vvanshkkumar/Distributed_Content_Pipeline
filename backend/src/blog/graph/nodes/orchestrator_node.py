import os
import json
import uuid
import logging
from kafka import KafkaProducer
import google.generativeai as genai
from src.observers.publisher import publisher
from src.kafka_config import KAFKA_BOOTSTRAP_SERVERS, BLOG_TASKS_TOPIC

logger = logging.getLogger(__name__)

def orchestrator_node(state: dict) -> dict:
    run_id = state.get("run_id")
    
    # Trigger Observer: Node Started
    publisher.on_node_enter(run_id, "orchestrator_node")

    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    model = genai.GenerativeModel(os.getenv("GEMINI_MODEL_NAME", "gemini-2.0-flash-exp"))

    prompt = f"""
    Create a detailed blog plan for the topic: "{state['topic']}".
    Return ONLY a valid JSON object with exactly this structure (no markdown tags):
    {{
        "title": "Your Blog Title",
        "sections": [
            {{"name": "Introduction", "instructions": "Write a compelling hook...", "order": 1}}
        ]
    }}
    Ensure there are 5-6 sections.
    """

    try:
        response = model.generate_content(prompt)
        raw_text = response.text.strip().replace("```json", "").replace("```", "")
        plan = json.loads(raw_text)
    except Exception as e:
        logger.error(f"Failed to generate plan: {e}")
        # Safe fallback plan so the pipeline doesn't crash completely
        plan = {
            "title": state['topic'],
            "sections": [
                {"name": "Introduction", "instructions": "Write an engaging introduction.", "order": 1},
                {"name": "Main Analysis", "instructions": "Analyze the core concepts.", "order": 2},
                {"name": "Conclusion", "instructions": "Summarize the key takeaways.", "order": 3}
            ]
        }

    # Publish tasks to Kafka for parallel writing
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    tasks = []
    for sec in plan.get("sections", []):
        task_id = str(uuid.uuid4())
        task_payload = {
            "run_id": run_id,
            "task_id": task_id,
            "section_name": sec["name"],
            "instructions": sec["instructions"],
            "order": sec["order"],
            "evidence": state.get("evidence", [])
        }
        producer.send(BLOG_TASKS_TOPIC, value=task_payload)
        tasks.append(task_payload)

    producer.flush()

    meta = {"section_count": len(tasks), "mode": state.get("mode")}
    
    # Trigger Observer: Node Finished
    publisher.on_node_exit(run_id, "orchestrator_node", "SUCCESS", meta)

    return {"plan": plan, "pending_task_count": len(tasks)}