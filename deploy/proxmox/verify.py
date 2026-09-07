#!/usr/bin/env python3
"""Read-only deployment checks, apart from creating/revoking a login session."""
import base64
import hashlib
import hmac
import http.cookiejar
import json
from pathlib import Path
import struct
import time
import urllib.request

root = Path(__file__).resolve().parents[2]
settings = dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines() if line and not line.startswith('#') and '=' in line)
base = 'http://127.0.0.1'
secret = settings['AUTH_TOTP_SECRET']
key = base64.b32decode(secret + '=' * (-len(secret) % 8))
digest = hmac.new(key, struct.pack('>Q', int(time.time()) // 30), hashlib.sha1).digest()
offset = digest[-1] & 15
code = str((struct.unpack('>I', digest[offset:offset+4])[0] & 0x7fffffff) % 1000000).zfill(6)
cookies = http.cookiejar.CookieJar()
client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))

def request(path, body=None):
    headers = {'Content-Type': 'application/json'}
    for cookie in cookies:
        if cookie.name == 'wsa_csrf': headers['X-CSRF-Token'] = cookie.value
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    with client.open(req, timeout=60) as response:
        content = response.read()
        return response.status, json.loads(content) if 'application/json' in response.headers.get('Content-Type', '') else content.decode()

for path in ('/', '/dashboard', '/tools/fpp'):
    status, html = request(path)
    assert status == 200 and 'id="root"' in html
    print(path, 'React OK')
request('/api/auth/login', {'username': settings['AUTH_USERNAME'], 'password': settings['AUTH_PASSWORD'], 'totp': code})
try:
    for path in ('/api/health', '/api/openapi.json', '/api/chat/account', '/api/fpp/status', '/api/fpp/playlists'):
        status, result = request(path)
        assert status == 200
        if isinstance(result, dict): assert result.get('ok', True) is not False, path
        if path.endswith('/openapi.json'):
            assert '/api/chat/threads/{thread_id}/messages' in result['paths']
            assert all(p.startswith('/api') for p in result['paths'])
            print(path, 'OK;', len(result['paths']), 'API paths')
        elif path.endswith('/chat/account'):
            print(path, 'runtime OK; ChatGPT connected =', result['connected'])
        elif path.endswith('/fpp/status'):
            print(path, 'OK;', json.dumps(result)[:160])
        else:
            print(path, 'OK')
finally:
    request('/api/auth/logout', {})
