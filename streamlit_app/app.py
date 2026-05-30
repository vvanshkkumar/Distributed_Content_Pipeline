import streamlit as st
from utils.api_client import login, register, health_check

st.set_page_config(
    page_title="vvanshkkumar's Content Pipeline",
    layout='wide',
    initial_sidebar_state='expanded'
)

is_logged_in = 'jwt_token' in st.session_state and st.session_state['jwt_token']

if not is_logged_in:
    st.title("V's Distributed Content Pipeline")
    st.caption('Please log in or create an account to continue.')

    health = health_check()
    if health.get('status') == 'ok':
        st.success('API is connected')
    else:
        st.error('Cannot reach API. Make sure docker compose is running.')
        st.stop()

    tab_login, tab_register = st.tabs(['Log In', 'Register'])

    with tab_login:
        st.subheader('Log in to your account')
        username = st.text_input('Username', key='login_user')
        password = st.text_input('Password', type='password', key='login_pass')

        if st.button('Log In', type='primary', disabled=not (username and password)):
            result = login(username, password)
            if 'error' in result or 'access_token' not in result:
                st.error(result.get('detail', result.get('error', 'Login failed')))
            else:
                st.session_state['jwt_token'] = result['access_token']
                st.session_state['username'] = result.get('username', username)
                st.success(f'Welcome back, {username}!')
                st.rerun()

    with tab_register:
        st.subheader('Create a new account')
        new_user = st.text_input('Choose a username', key='reg_user')
        new_pass = st.text_input('Choose a password', type='password', key='reg_pass')
        new_pass2 = st.text_input('Confirm password', type='password', key='reg_pass2')

        if st.button('Create Account', type='primary', disabled=not (new_user and new_pass)):
            if new_pass != new_pass2:
                st.error('Passwords do not match')
            else:
                result = register(new_user, new_pass)
                if 'error' in result:
                    st.error(result.get('detail', result['error']))
                else:
                    st.success('Account created! Please log in.')

    st.stop()


st.title('Distributed Content Pipeline')
st.caption(f'Logged in as **{st.session_state.get("username", "user")}** | FastAPI LangGraph Kafka KRaft Celery Redis AWS')

health = health_check()
col1, col2, col3 = st.columns(3)
col1.metric('API Status', health.get('status', 'unknown').upper())
col2.metric('Database', health.get('database', 'unknown').upper())
col3.metric('Logged in as', st.session_state.get('username', ''))

st.divider()

col1, col2, col3 = st.columns(3)
col1.page_link('pages/1_Generate.py', label='Generate Blog', icon='✍️')
col1.page_link('pages/2_Live_Pipeline.py', label='Live Infrastructure', icon='⚙️')
col2.page_link('pages/3_Analytics.py', label='Analytics Dashboard', icon='📊')
col2.page_link('pages/4_Library.py', label='Blog Library', icon='📚')
col3.page_link('pages/5_Monitor.py', label='System Monitor', icon='🖥️')

st.divider()

if st.button('Log Out'):
    st.session_state.clear()
    st.rerun()

st.caption(f'API URL: {__import__("os").getenv("API_URL", "http://localhost:8000")}')