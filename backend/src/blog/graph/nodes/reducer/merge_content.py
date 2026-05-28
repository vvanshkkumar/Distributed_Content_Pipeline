import json
import time
import logging
from kafka import KafkaConsumer
from src.observers.publisher import publisher
from src.kafka_config import KAFKA_BOOTSTRAP_SERVERS, BLOG_SECTIONS_TOPIC, SECTION_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

def merge_content(state: dict) -> dict:
    run_id = state.get("run_id")
    
    # Trigger Observer: Node Started
    publisher.on_node_enter(run_id, "merge_content")

    expected_count = state.get("pending_task_count", 0)
    plan = state.get("plan", {})
    collected_sections = []

    if expected_count > 0:
        consumer = KafkaConsumer(
            BLOG_SECTIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"reducer-{run_id}", # Unique group so it processes everything for this run
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        start_time = time.time()
        
        # Wait for all sections to arrive
        while len(collected_sections) < expected_count:
            if time.time() - start_time > SECTION_TIMEOUT_SECONDS:
                logger.warning(f"Timeout waiting for sections for run {run_id}")
                break

            records = consumer.poll(timeout_ms=2000)
            for tp, messages in records.items():
                for msg in messages:
                    payload = msg.value
                    if payload.get("run_id") == run_id:
                        collected_sections.append({
                            "name": payload.get("section_name"),
                            "content": payload.get("content")
                        })
        consumer.close()

    # Assemble sections in the original plan order
    merged_content = f"# {plan.get('title', 'Generated Blog')}\n\n"
    
    for sec in plan.get("sections", []):
        sec_name = sec["name"]
        matching = next((item for item in collected_sections if item["name"] == sec_name), None)
        
        if matching:
            merged_content += f"## {sec_name}\n\n{matching['content']}\n\n"
        else:
            merged_content += f"## {sec_name}\n\n*Section failed to generate due to worker timeout/error.*\n\n"

    # Trigger Observer: Node Finished
    publisher.on_node_exit(run_id, "merge_content", "SUCCESS", {"assembled_sections": len(collected_sections)})

    return {"merged_content": merged_content}