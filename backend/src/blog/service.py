import os
import uuid
import boto3
import logging
from datetime import datetime
from pathlib import Path

from langgraph.graph import StateGraph, END
from src.database import SessionLocal
from src.models import BlogRun
from src.cache import invalidate_recents_cache

# Import the State and all 6 Nodes
from src.blog.graph.state import BlogState
from src.blog.graph.nodes.router_node import router_node
from src.blog.graph.nodes.research_node import research_node
from src.blog.graph.nodes.orchestrator_node import orchestrator_node
from src.blog.graph.nodes.reducer.merge_content import merge_content
from src.blog.graph.nodes.reducer.decide_images import decide_images
from src.blog.graph.nodes.reducer.generate_and_place_images import generate_and_place_images

logger = logging.getLogger(__name__)

# ==========================================
# 1. BUILD THE LANGGRAPH GRAPH (Runs at startup)
# ==========================================
workflow = StateGraph(BlogState)

# Add all 6 nodes
workflow.add_node("router_node", router_node)
workflow.add_node("research_node", research_node)
workflow.add_node("orchestrator_node", orchestrator_node)
workflow.add_node("merge_content", merge_content)
workflow.add_node("decide_images", decide_images)
workflow.add_node("generate_and_place_images", generate_and_place_images)

# Define the edges (the flow)
workflow.add_edge("router_node", "research_node")
workflow.add_edge("research_node", "orchestrator_node")
workflow.add_edge("orchestrator_node", "merge_content")
workflow.add_edge("merge_content", "decide_images")
workflow.add_edge("decide_images", "generate_and_place_images")
workflow.add_edge("generate_and_place_images", END)

# Set starting point and compile
workflow.set_entry_point("router_node")
APP = workflow.compile()


# ==========================================
# 2. THE PIPELINE RUNNER & FILE SAVER
# ==========================================
def run_blog_generation(topic: str, run_id: str = None) -> dict:
    """
    Executes the blog generation pipeline.
    Expects to be called via FastAPI BackgroundTasks so it doesn't block the HTTP response.
    """
    if not run_id:
        run_id = str(uuid.uuid4())
        
    db = SessionLocal()
    try:
        # Step 2: Create (or fetch) BlogRun row with status RUNNING
        existing_run = db.query(BlogRun).filter(BlogRun.run_id == run_id).first()
        if not existing_run:
            db_run = BlogRun(run_id=run_id, topic=topic, status="RUNNING")
            db.add(db_run)
            db.commit()
        else:
            db_run = existing_run

        # Step 3: Build the initial BlogState dict
        initial_state = {
            "run_id": run_id,
            "topic": topic
        }

        # Step 4: Call APP.invoke(state) - runs all 6 nodes in sequence
        logger.info(f"Starting LangGraph pipeline for run_id: {run_id}")
        final_state = APP.invoke(initial_state)

        # Step 5: Save markdown to S3 (Production) or local disk (Development)
        content = final_state.get("merged_content", "")
        word_count = len(content.split())
        blog_title = final_state.get("plan", {}).get("title", topic)
        
        s3_bucket = os.getenv("S3_BUCKET_NAME")
        if s3_bucket:
            # Production: Boto3 uses EC2 Instance Profile credentials automatically
            s3 = boto3.client('s3', region_name=os.getenv("AWS_REGION", "us-east-1"))
            s3_key = f"blog_runs/{run_id}/title.md"
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=content.encode('utf-8'))
            md_file_path = f"s3://{s3_bucket}/{s3_key}"
            logger.info(f"Saved to S3: {md_file_path}")
        else:
            # Local: Save to disk volume
            save_dir = Path("data/app/data/blogs")
            save_dir.mkdir(parents=True, exist_ok=True)
            local_file = save_dir / f"{run_id}.md"
            local_file.write_text(content, encoding="utf-8")
            md_file_path = str(local_file)
            logger.info(f"Saved locally: {md_file_path}")

        # Step 6: Update BlogRun to status SUCCESS
        db_run.status = "SUCCESS"
        db_run.blog_title = blog_title
        db_run.md_file_path = md_file_path
        db_run.word_count = word_count
        db_run.mode = final_state.get("mode", "unknown")
        db_run.completed_at = datetime.utcnow()
        db.commit()

        # Step 7: Invalidate Redis recents cache
        invalidate_recents_cache()

        # Step 8: Return dict
        return {
            "run_id": run_id,
            "status": "SUCCESS",
            "blog_title": blog_title,
            "preview_url": f"/api/blog/runs/{run_id}/preview"
        }

    except Exception as e:
        logger.error(f"Pipeline failed for run {run_id}: {e}")
        db.rollback()
        db_run = db.query(BlogRun).filter(BlogRun.run_id == run_id).first()
        if db_run:
            db_run.status = "FAILED"
            db_run.error_message = str(e)
            db.commit()
        return {"run_id": run_id, "status": "FAILED", "error": str(e)}
    finally:
        db.close()