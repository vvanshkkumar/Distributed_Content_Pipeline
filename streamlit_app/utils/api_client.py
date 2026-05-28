import os
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

API_BASE = os.getenv('API_URL', 'http://localhost:8000')

def get_token() -> str | None:
    return st.session_state.get('jwt_token')

def auth_headers() -> dict:
    token = get_token()
    if not token:
        return {}
    return {'Authorization': f'Bearer {token}'}

def _get(path: str, **kwargs) -> dict | list:
    try:
        r = requests.get(f'{API_BASE}{path}', timeout=15, **kwargs)
        return r.json()
    except Exception as e:
        return {'error': str(e)}

def _post(path: str, **kwargs) -> dict:
    try:
        r = requests.post(f'{API_BASE}{path}', timeout=30, **kwargs)
        return r.json()
    except Exception as e:
        return {'error': str(e)}

def register(username: str, password: str) -> dict:
    return _post('/api/auth/register', json={'username': username, 'password': password})

def login(username: str, password: str) -> dict:
    return _post('/api/auth/login', data={'username': username, 'password': password})

def health_check() -> dict:
    try:
        r = requests.get(f'{API_BASE}/api/health/', timeout=5)
        return r.json()
    except:
        return {'status': 'error', 'database': 'unreachable'}

def generate_blog(topic: str, to_email: str = None, schedule_at: str = None) -> dict:
    payload = {'topic': topic}
    if to_email: 
        payload['to_email'] = to_email
    if schedule_at: 
        payload['schedule_at'] = schedule_at

    return _post('/api/blog/generate', json=payload, headers=auth_headers())

def get_run_status(run_id: str) -> dict:
    return _get(f'/api/blog/runs/{run_id}/status')

def get_run_sections(run_id: str) -> dict:
    return _get(f'/api/blog/runs/{run_id}/sections')

def get_preview_url(run_id: str) -> str:
    return f"{API_BASE}/api/blog/runs/{run_id}/preview"

def get_recents() -> list:
    result = _get('/api/recents/')
    return result if isinstance(result, list) else []

def send_now(run_id: str, to_email: str) -> dict:
    return _post('/api/blog/send-existing',
        json={'run_id': run_id, 'to_email': to_email},
        headers=auth_headers())

def schedule_email(run_id: str, to_email: str, scheduled_at: str) -> dict:
    return _post('/api/blog/schedule-existing',
        json={'run_id': run_id, 'to_email': to_email, 'scheduled_at': scheduled_at},
        headers=auth_headers())

def get_scheduled() -> list:
    result = _get('/api/blog/scheduled')
    return result if isinstance(result, list) else []

def get_failed_jobs() -> list:
    result = _get('/api/jobs/failed', headers=auth_headers())
    return result if isinstance(result, list) else []

def retry_job(job_id: int) -> dict:
    return _post(f'/api/jobs/failed/{job_id}/retry', headers=auth_headers())