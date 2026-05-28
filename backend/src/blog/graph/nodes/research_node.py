import os
import logging
from tavily import TavilyClient
from src.observers.publisher import publisher

logger = logging.getLogger(__name__)

def research_node(state: dict) -> dict:
    run_id = state.get("run_id")
    
    # Trigger Observer: Node Started
    publisher.on_node_enter(run_id, "research_node")

    if not state.get("needs_research"):
        publisher.on_node_exit(run_id, "research_node", "SUCCESS", {"note": "Skipped research"})
        return {"evidence": []}

    tavily_key = os.getenv("TAVILY_API_KEY")
    evidence = []
    
    if tavily_key:
        try:
            client = TavilyClient(api_key=tavily_key)
            response = client.search(query=state["topic"], search_depth="advanced", max_results=8)
            for res in response.get("results", []):
                evidence.append({
                    "url": res.get("url"),
                    "content": res.get("content")
                })
        except Exception as e:
            logger.warning(f"Tavily research failed: {e}")
    else:
        logger.warning("No TAVILY_API_KEY found, skipping web research.")

    meta = {"evidence_count": len(evidence)}
    
    # Trigger Observer: Node Finished
    publisher.on_node_exit(run_id, "research_node", "SUCCESS", meta)
    
    return {"evidence": evidence}