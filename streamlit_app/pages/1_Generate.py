import time
import streamlit as st
from utils.api_client import generate_blog, get_run_status, get_run_sections, get_preview_url

st.set_page_config(page_title='Generate', page_icon='✍️', layout='wide')

if 'jwt_token' not in st.session_state:
    st.warning('Please log in first.')
    st.page_link('app.py', label='Go to Login')
    st.stop()

st.title('Generate a Blog')
st.caption('Powered by LangGraph, Gemini AI, Kafka KRaft workers')

col1, col2 = st.columns([2, 1])

with col1:
    topic = st.text_input('Blog topic', placeholder='e.g. Why Python dominates data science in 2025')

with col2:
    to_email = st.text_input('Send to email (optional)')
    schedule_at = st.text_input('Schedule at ISO datetime (optional)', placeholder='2025-12-31T10:00:00')

if st.button('Generate Blog', type='primary', disabled=not topic):
    st.session_state.pop('current_run_id', None)

    with st.spinner('Sending to FastAPI...'):
        result = generate_blog(topic, to_email=to_email or None, schedule_at=schedule_at or None)

    if 'error' in result:
        if '401' in str(result.get('error', '')):
            st.error('Session expired. Please log in again.')
        else:
            st.error(f'Error: {result["error"]}')
        st.stop()

    if result.get('detail'):
        st.error(f'API error: {result["detail"]}')
        st.stop()

    run_id = result.get('run_id')
    st.session_state['current_run_id'] = run_id
    st.success(f'Generation started! run_id: {run_id[:16]}...')

    st.subheader('Live Pipeline Progress')

    progress_bar = st.progress(0)
    status_text = st.empty()
    node_container = st.empty()
    section_container = st.empty()

    for _ in range(200):
        data = get_run_status(run_id)
        sections = get_run_sections(run_id)

        pct = data.get('progress_pct', 0)
        node = data.get('current_node', 'starting...')
        completed = data.get('completed_nodes', [])

        progress_bar.progress(min(pct, 100) / 100)
        status_text.markdown(f'**Current step:** {node} &nbsp; &nbsp; Progress: {pct}%')

        NODE_NAMES = ['router', 'research', 'orchestrator', 'merge', 'images', 'complete']
        with node_container.container():
            cols = st.columns(6)
            for col, name in zip(cols, NODE_NAMES):
                if any(name in c for c in completed):
                    col.success(f'{name}')
                elif name in node:
                    col.warning(f'{name}')
                else:
                    col.info(f'{name}')

        sec_list = sections.get('sections', [])
        if sec_list:
            done = sections.get('completed', 0)
            total = sections.get('total', 0)
            with section_container.container():
                st.caption(f'Kafka workers: {done}/{total} sections complete')
                for sec in sec_list:
                    icon = {'DONE': '✅', 'PROCESSING': '⏳', 'PENDING': '⏳', 'PERMANENTLY_FAILED': '❌'}.get(sec['status'], '⏳')
                    st.write(f"{icon} {sec['task_id']} | {sec['status']} | (Kafka deliveries: {sec['attempts']})")

        if pct >= 100:
            break

        time.sleep(2)
        st.rerun()

    st.balloons()
    st.success('Blog generation complete!')
    preview_url = get_preview_url(run_id)
    st.markdown(f'### [📄 Open Blog Preview]({preview_url})')
    st.code(run_id, language=None)