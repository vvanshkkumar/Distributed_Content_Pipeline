import time
import streamlit as st
from utils.api_client import get_run_status, get_run_sections

st.set_page_config(page_title='Live Pipeline', page_icon='⚙️', layout='wide')
st.title('Live Infrastructure Story')
st.caption('Watch each component of your distributed system work in real time')

NARRATIVE = {
    'router_node': {
        'title': 'Step 1 - Router Decision',
        'story': 'FastAPI received your request. PostgreSQL created a new BlogRun record. Redis checked the rate limit counter and initialised the live status key. The router is now analysing your topic to decide: no research, some research, or heavy research.',
        'pg': 'BlogRun row CREATED | PipelineEvent: router_node RUNNING',
        'redis': 'Rate limit checked | Status key initialised: run:{id}',
        'kafka': 'No tasks yet - waiting for orchestrator',
        's3': 'Waiting for final content'
    },
    'research_node': {
        'title': 'Step 2 - Web Research',
        'story': 'Router decided the research depth. The research agent is now calling the Tavily web search API, scanning multiple sources, and extracting structured EvidenceItems. Redis live status updated after router finished.',
        'pg': 'PipelineEvent: router_node SUCCESS logged',
        'redis': 'Current node: research_node | Status updated',
        'kafka': 'Waiting',
        's3': 'Waiting'
    },
    'orchestrator_node': {
        'title': 'Step 3 - Orchestrator Planning',
        'story': 'Research complete. Gemini AI is building a structured plan with N sections. Each section becomes a Task object with specific writing instructions. In the next instant, ALL tasks will be published to Kafka simultaneously - this is the moment sequential work becomes parallel.',
        'pg': 'PipelineEvent: research_node SUCCESS | Evidence cached',
        'redis': 'Status: orchestrator_node | Research results in memory',
        'kafka': 'Tasks about to be published to blog.tasks topic...',
        's3': 'Waiting'
    },
    'merge_content': {
        'title': 'Step 4 - Kafka Fan-out ACTIVE',
        'story': 'ALL section tasks published to Kafka blog.tasks topic simultaneously. Three parallel workers each picked up different partitions. They are calling Gemini AI independently, writing sections to disk, and publishing results to blog.sections. Redis tracks each attempt. The reducer is collecting sections as they arrive.',
        'pg': 'SectionAttempt rows created | Offsets committed per section',
        'redis': 'Section tracking ACTIVE | Status: merge_content',
        'kafka': 'ACTIVE - 3 workers on blog.tasks | Sections arriving on blog.sections',
        's3': 'Waiting for final assembly'
    },
    'decide_images': {
        'title': 'Step 5 - Assembling and Planning Images',
        'story': 'All sections arrived from Kafka blog.sections topic and were assembled in the correct order (by task_id, not arrival order). Now scanning the merged content to identify where images would add value.',
        'pg': 'All PipelineEvents logged | BlogRun content cached',
        'redis': 'Status: decide_images | All sections complete',
        'kafka': 'All sections collected | All Kafka offsets committed',
        's3': 'Upload coming soon...'
    },
    'generate_and_place_images': {
        'title': 'Step 6 - Generating Images',
        'story': 'Google image API is generating images. Each image is placed at the correct position in the markdown. If generation fails, a descriptive placeholder is inserted - the blog always completes.',
        'pg': 'BlogRun updated with merged content',
        'redis': 'Status: generate_and_place_images',
        'kafka': 'Complete - all work done',
        's3': 'Uploading blog file now...'
    }
}

COMPLETE = {
    'title': 'Generation Complete!',
    'story': 'Blog saved to S3 (or local disk in development). PostgreSQL BlogRun updated to SUCCESS with word count. Redis recents cache invalidated - your blog appears immediately in the library. Preview will be cached in Redis on first load.',
    'pg': 'BlogRun.status = SUCCESS | word_count saved | md_file_path = s3 key',
    'redis': 'Recents cache INVALIDATED | Preview cached on first load',
    'kafka': 'All offsets committed | Consumer groups closed',
    's3': 'blog_runs/{run_id}/title.md UPLOADED'
}

def status_card(col, emoji, name, message, active=False, done=False):
    with col:
        if done:
            st.success(f'{emoji} **{name}**\n\n{message}')
        elif active:
            st.warning(f'{emoji} **{name}**\n\n{message}')
        else:
            st.info(f'{emoji} **{name}**\n\n{message}')

run_id = st.text_input('Paste your run_id to watch live:', value=st.session_state.get('current_run_id', ''), placeholder='xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx')

if not run_id:
    st.info('Generate a blog on the Generate page, then paste the run_id here.')
    st.stop()

auto_refresh = st.toggle('Auto-refresh every 2 seconds', value=True)

status = get_run_status(run_id)
sections = get_run_sections(run_id)

pct = status.get('progress_pct', 0)
current = status.get('current_node', 'starting')
history = status.get('history', [])
completed = [e['node'] for e in history if e.get('status') == 'SUCCESS']

st.progress(min(pct, 100) / 100)
st.markdown(f'**Progress:** {pct}% | **Current node:** {current}')

st.divider()

col_pg, col_redis, col_kafka, col_s3 = st.columns(4)

if pct >= 100:
    narr = COMPLETE
elif current in NARRATIVE:
    narr = NARRATIVE[current]
else:
    narr = list(NARRATIVE.values())[0]
    for node_name in reversed(list(NARRATIVE.keys())):
        if any(node_name in c for c in completed):
            narr = NARRATIVE[node_name]
            break

is_complete = pct >= 100
is_kafka_active = 'merge' in current or 'orchestrator' in current

status_card(col_pg, '🐘', 'PostgreSQL', narr['pg'], active=(not is_complete), done=is_complete)
status_card(col_redis, '⚡', 'Redis', narr['redis'], active=(not is_complete), done=is_complete)
status_card(col_kafka, '📨', 'Kafka', narr['kafka'], active=is_kafka_active, done=(pct >= 70 and not is_kafka_active))
status_card(col_s3, '🪣', 'S3', narr['s3'], done=is_complete)

st.divider()

st.subheader(narr['title'])
st.markdown(f'> {narr["story"]}')

st.divider()
st.subheader('Kafka Worker Status - blog.tasks & blog.sections')

sec_list = sections.get('sections', [])
if not sec_list:
    st.info('No section tasks yet - waiting for orchestrator to publish to Kafka...')
else:
    done_count = sections.get('completed', 0)
    total = sections.get('total', 0)
    in_prog = sections.get('in_progress', 0)

    cola, colb, colc = st.columns(3)
    cola.metric('Total tasks published to Kafka', total)
    colb.metric('Sections on blog.sections topic', done_count)
    colc.metric('Workers active', in_prog)

    for sec in sec_list:
        icon = {'DONE': '✅', 'PROCESSING': '⏳', 'PENDING': '⏳', 'PERMANENTLY_FAILED': '❌'}.get(sec['status'], '')
        st.write(f"{icon} Task {sec['task_id']} | {sec['status']} | (Kafka deliveries: {sec['attempts']} time(s))")

st.divider()
st.subheader('PostgreSQL Audit Trail (pipeline_events table)')

if history:
    for event in history:
        icon = '✅' if event['status'] == 'SUCCESS' else '❌' if event['status'] == 'FAILED' else '⏳'
        ts = (event.get('timestamp') or '')[:19]
        meta = event.get('meta') or {}
        meta_str = ' | '.join(f"{k}: {v}" for k, v in meta.items()) if meta else ""
        st.write(f"{icon} {ts} | **{event['node']}** | {event['status']}" + (f" | {meta_str}" if meta_str else ""))
else:
    st.info('No events yet. Waiting for pipeline to start...')

if auto_refresh and pct < 100:
    time.sleep(2)
    st.rerun()
elif pct >= 100:
    st.balloons()