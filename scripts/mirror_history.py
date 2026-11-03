import os, json, urllib.request, urllib.parse, urllib.error, pathlib, datetime

BASE = 'https://billowing-voice-0907.sandomky.workers.dev'
TOKEN = os.environ['HISTORY_READ_TOKEN']
ROOT = pathlib.Path('history')
SOURCES = {'market': 'timestamp', 'doa_snapshots': 'collected_ms'}

def fetch(source, start, end):
    cursor = None
    result = []
    seen = set()
    while True:
        q = {'source': source, 'from': str(start), 'to': str(end), 'limit': '200'}
        if cursor:
            q['cursor'] = cursor
        url = BASE + '/history/data?' + urllib.parse.urlencode(q)
        req = urllib.request.Request(url, headers={
            'Authorization': 'Bearer ' + TOKEN,
            'Accept': 'application/json',
            'User-Agent': 'SOL-History-Mirror/1.1',
        })
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                body = json.load(resp)
        except urllib.error.HTTPError as exc:
            # Do not print request headers, Authorization, token, or full response body.
            safe_headers = ['content-type', 'server', 'cf-ray', 'cf-mitigated', 'www-authenticate', 'location']
            info = {k: exc.headers.get(k) for k in safe_headers if exc.headers.get(k)}
            print(json.dumps({'error': 'HISTORY_HTTP_ERROR', 'status': exc.code,
                'source': source, 'date_utc': datetime.datetime.fromtimestamp(start / 1000, datetime.timezone.utc).date().isoformat(),
                'headers': info}, ensure_ascii=False), flush=True)
            raise SystemExit(1)
        except urllib.error.URLError as exc:
            print(json.dumps({'error': 'HISTORY_NETWORK_ERROR', 'source': source,
                'reason_type': type(exc.reason).__name__}, ensure_ascii=False), flush=True)
            raise SystemExit(1)
        if not body.get('ok') or body.get('source') != source:
            raise RuntimeError('Invalid history response')
        result.extend(body['rows'])
        if not body.get('has_more'):
            break
        nxt = body.get('next_cursor')
        if not nxt or nxt in seen:
            raise RuntimeError('Cursor pagination stalled')
        seen.add(nxt)
        cursor = nxt
        if len(seen) > 2000:
            raise RuntimeError('Excessive pages')
    return result

def main():
    mode = os.getenv('MIRROR_MODE', 'incremental')
    now = datetime.datetime.now(datetime.timezone.utc)
    first = datetime.date(2026, 10, 1) if mode == 'backfill' else (now - datetime.timedelta(days=2)).date()
    last = now.date()
    summary = {}
    for source, tc in SOURCES.items():
        day = first
        while day <= last:
            start = int(datetime.datetime.combine(day, datetime.time(), datetime.timezone.utc).timestamp() * 1000)
            end = start + 86400000
            rows = fetch(source, start, end)
            path = ROOT / source / (day.isoformat() + '.json')
            if rows:
                path.parent.mkdir(parents=True, exist_ok=True)
                data = {'source': source, 'date_utc': day.isoformat(), 'time_column': tc, 'count': len(rows), 'rows': rows}
                serialized = json.dumps(data, ensure_ascii=False, separators=(',', ':')) + '\n'
                if not path.exists() or path.read_text() != serialized:
                    path.write_text(serialized)
                summary[str(path)] = len(rows)
            day += datetime.timedelta(days=1)
    print(json.dumps({'mode': mode, 'files': summary}, ensure_ascii=False))

if __name__ == '__main__':
    main()
