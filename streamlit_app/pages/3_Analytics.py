import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from utils.api_client import get_recents, get_scheduled, get_failed_jobs

st.set_page_config(page_title='Analytics', page_icon='📊', layout='wide')

if 'jwt_token' not in st.session_state:
    st.warning('Please log in first.')
    st.page_link('app.py', label='Go to Login')
    st.stop()

st.title('Analytics Dashboard')
st.caption('Metrics powered by pandas and plotly - data from your PostgreSQL database')

blogs = get_recents()
scheduled = get_scheduled()
failed = get_failed_jobs()

if not blogs:
    st.info('No blog data yet. Generate some blogs first!')
    st.stop()

df = pd.DataFrame(blogs)

c1, c2, c3, c4 = st.columns(4)
c1.metric('Total Blogs Generated', len(df))
c2.metric('Successful', len(df[df['status'] == 'SUCCESS']))
c3.metric('Failed', len(df[df['status'] == 'FAILED']))
c4.metric('Emails Scheduled', len(scheduled))

st.divider()

cola, colb = st.columns(2)

with cola:
    st.subheader('Blog Status Distribution')
    counts = df['status'].value_counts().reset_index()
    counts.columns = ['status', 'count']
    fig = px.pie(counts, names='status', values='count', color_discrete_sequence=['#059669', '#dc2626', '#f59e0b'])
    st.plotly_chart(fig, use_container_width=True)

with colb:
    st.subheader('Generation Volume Over Time')
    if 'created_at' in df.columns:
        df['date'] = pd.to_datetime(df['created_at']).dt.date
        by_date = df.groupby('date').size().reset_index(name='count')
        fig2 = px.bar(by_date, x='date', y='count', color_discrete_sequence=['#6366f1'])
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

st.subheader('Most Common Topic Words')
STOP = ['the', 'a', 'an', 'of', 'in', 'for', 'and', 'to', 'with', 'how', 'why', 'what', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'i', 'you', 'it', 'we', 'they']
words = pd.Series(' '.join(df['topic'].dropna()).lower().split())
freq = words[~words.isin(STOP)].value_counts().head(15).reset_index()
freq.columns = ['word', 'count']
fig3 = px.bar(freq, x='count', y='word', orientation='h', color='count', color_continuous_scale='Viridis')
fig3.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig3, use_container_width=True)

st.divider()

if scheduled:
    st.subheader('Email Delivery Funnel')
    df_e = pd.DataFrame(scheduled)
    e_counts = df_e['status'].value_counts()
    fig4 = go.Figure(go.Funnel(
        y=e_counts.index.tolist(), 
        x=e_counts.values.tolist(),
        textinfo='value+percent initial',
        marker={'color': ['#6366f1', '#059669', '#dc2626', '#f59e0b']}
    ))
    st.plotly_chart(fig4, use_container_width=True)