import logging
from src.observers.publisher import publisher

logger = logging.getLogger(__name__)

def generate_and_place_images(state: dict) -> dict:
    run_id = state.get("run_id")
    
    
    publisher.on_node_enter(run_id, "generate_and_place_images")

    content = state.get("merged_content", "")
    prompts = state.get("image_prompts", [])

    
    
    for img in prompts:
        heading = img["heading"]
        placeholder = f"\n\n> *[Image Placeholder: {img['prompt']}]*\n\n"
        
        
        content = content.replace(f"## {heading}", f"## {heading}{placeholder}", 1)

    
    publisher.on_node_exit(run_id, "generate_and_place_images", "SUCCESS", {})

    return {"merged_content": content}