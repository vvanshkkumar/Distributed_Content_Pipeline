import re
import logging
from src.observers.publisher import publisher

logger = logging.getLogger(__name__)

def decide_images(state: dict) -> dict:
    run_id = state.get("run_id")
    
    
    publisher.on_node_enter(run_id, "decide_images")

    content = state.get("merged_content", "")
    topic = state.get("topic", "Technology")
    
   
    headings = re.findall(r'^##\s+(.*)', content, re.MULTILINE)

    image_prompts = []
    
    for heading in headings[:3]: 
        image_prompts.append({
            "heading": heading,
            "prompt": f"Professional illustration for: {heading}, focusing on {topic}"
        })

    meta = {"image_count": len(image_prompts)}
    
    
    publisher.on_node_exit(run_id, "decide_images", "SUCCESS", meta)

    return {"image_prompts": image_prompts}