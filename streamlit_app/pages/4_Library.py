import streamlit as st
from utils.api_client import get_recents, send_now, schedule_email, get_preview_url

st.set_page_config(page_title='Blog Library', page_icon='📚', layout='wide')
st.title('Blog Library')
st.caption('Browse all generated blogs. Preview, send, or schedule delivery.')

blogs = get_recents()

if not blogs:
    st.info('No blogs yet. Generate one first!')
    st.stop()

search = st.text_input('Search by topic or title')
if search:
    blogs = [b for b in blogs if search.lower() in (b.get('topic', '') + b.get('blog_title', '')).lower()]

for blog in blogs:
    icon = {'SUCCESS': '✅', 'RUNNING': '⏳', 'FAILED': '❌', 'PENDING': '⏳'}.get(blog.get('status'), '⏳')
    title = blog.get('blog_title') or blog.get('topic', 'Untitled')

    with st.expander(f"{icon} {title} ({(blog.get('created_at') or '')[:10]})"):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        c1.caption(f"Topic: {blog.get('topic', '')}")
        c1.caption(f"Run ID: {blog.get('run_id', '')[:16]}...")

        if blog.get('status') == 'SUCCESS':
            with c2:
                st.link_button('Preview', get_preview_url(blog['run_id']))
                
            with c3:
                if 'jwt_token' in st.session_state:
                    with st.popover('Send Now'):
                        em = st.text_input('Email', key=f'sn_{blog["run_id"]}')
                        if st.button('Send', key=f'sb_{blog["run_id"]}') and em:
                            res = send_now(blog['run_id'], em)
                            if 'error' not in res:
                                st.success('Sent!')
                            else:
                                st.error(res['error'])
                else:
                    st.caption('Log in to send')
                    
            with c4:
                if 'jwt_token' in st.session_state:
                    with st.popover('Schedule'):
                        em2 = st.text_input('Email', key=f'sc_{blog["run_id"]}')
                        dt = st.text_input('When (ISO)', key=f'scd_{blog["run_id"]}', placeholder='2025-12-31T10:00:00')
                        if st.button('Schedule', key=f'scb_{blog["run_id"]}') and em2 and dt:
                            res = schedule_email(blog['run_id'], em2, dt)
                            if 'error' not in res:
                                st.success('Scheduled!')
                            else:
                                st.error(res['error'])