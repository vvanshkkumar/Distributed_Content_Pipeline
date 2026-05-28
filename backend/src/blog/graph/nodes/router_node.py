import logging
from src.observers.publisher import publisher

logger = logging.getLogger(__name__)

def router_node(state: dict) -> dict:
    run_id = state.get("run_id")
    topic = state.get("topic", "").lower()
    
   
    publisher.on_node_enter(run_id, "router_node")

    
    keywords = ['latest', 'compare', 'best', '2025', '2026', 'trend']
    match_count = sum(1 for k in keywords if k in topic)

    if match_count == 0:
        mode = "closed_book"
        needs_research = False
    elif match_count == 1:
        mode = "hybrid"
        needs_research = True
    else:
        mode = "open_book"
        needs_research = True

    meta = {"mode": mode, "needs_research": needs_research}
    
    
    publisher.on_node_exit(run_id, "router_node", "SUCCESS", meta)
    
    return {"mode": mode, "needs_research": needs_research}