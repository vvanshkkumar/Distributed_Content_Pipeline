import streamlit as st
from utils.api_client import get_failed_jobs, retry_job, get_scheduled, health_check

st.set_page_config(page_title='Monitor', page_icon='🖥️', layout='wide')
st.title('System Monitor')

if 'jwt_token' not in st.session_state:
    st.warning('Please log in to access system monitoring.')
    st.page_link('app.py', label='Go to Login')
    st.stop()

st.subheader('System Health')
health = health_check()
c1, c2 = st.columns(2)
c1.metric('API Status', health.get('status', 'unknown').upper())
c2.metric('Database', health.get('database', 'unknown').upper())

if health.get('status') != 'ok':
    st.error('API is not healthy. Check your EC2 instances and RDS.')

st.divider()

st.subheader('Dead Letter Queue (Failed Email Jobs)')
failed = get_failed_jobs()

if not failed:
    st.success('No failed jobs - email delivery is healthy.')
else:
    st.warning(f"{len(failed)} job(s) in dead letter queue.")
    for job in failed:
        with st.expander(f"Job #{job['id']} - {job['task_name']} ({(job.get('failed_at') or '')[:19]})"):
            st.write(f"**Attempts:** {job['attempts']} | **Error:** {job['error']}")
            
            if st.button(f'Retry Job #{job["id"]}', key=f'r_{job["id"]}'):
                res = retry_job(job['id'])
                if 'error' not in res:
                    st.success('Re-queued into Celery!')
                else:
                    st.error(res['error'])
                st.rerun()

st.divider()

st.subheader('Scheduled Email Queue')
scheduled = get_scheduled()

if not scheduled:
    st.info('No scheduled emails.')
else:
    for e in scheduled:
        icon = {'PENDING': '⏳', 'QUEUED': '🔄', 'SENT': '✅', 'FAILED': '❌'}.get(e.get('status'), '⏳')
        st.write(f"{icon} **To:** {e['to_email']} at {e['scheduled_at'][:16]} (**{e['status']}**)")