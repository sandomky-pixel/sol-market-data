import datetime as dt
import json
import os
import pathlib
import random
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import http.client

BASE = 'https://billowing-voice-0907.sandomky.workers.dev'
TOKEN = os.environ['HISTORY_READ_TOKEN']
ROOT = pathlib.Path('history')
SOURCES = {'market': 'timestamp', 'doa_snapshots': 'collected_ms'}
PAGE_SIZE = 50
MAX_ATTEMPTS = 6
UTC = dt.timezone.utc


def fetch(source, start, end):
    cursor, result, seen = None, [], set()
    date = dt.datetime.fromtimestamp(start / 1000, UTC).date().isoformat()
    while True:
        q = {'source': source, 'from': str(start), 'to': str(end), 'limit': str(PAGE_SIZE)}
        if cursor:
            q['cursor'] = cursor
        req = urllib.request.Request(BASE + '/history/data?' + urllib.parse.urlencode(q), headers={
            'Authorization': 'Bearer ' + TOKEN,
            'Accept': 'application/json',
            'User-Agent': 'SOL-History-Mirror/1.2',
        })
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(req, timeout=40) as resp:
                    body = json.load(resp)
                if not isinstance(body, dict) or not body.get('ok') or body.get('source') != source or not isinstance(body.get('rows'), list):
                    raise RuntimeError('Invalid history response')
                break
            except urllib.error.HTTPError as exc:
                if exc.code not in (408, 429, 500, 502, 503, 504):
                    raise RuntimeError(f'Non-retryable HTTP {exc.code} source={source} date={date}') from None
                reason = f'HTTP {exc.code}'
            except (urllib.error.URLError, ConnectionResetError, TimeoutError, socket.timeout,
                    http.client.IncompleteRead, http.client.RemoteDisconnected, OSError) as exc:
                reason = type(exc).__name__
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f'History request failed after {MAX_ATTEMPTS} attempts: source={source} date={date} page={len(seen)+1} reason={reason}')
            delay = min(2 ** (attempt - 1), 20) + random.uniform(0, 1)
            print(json.dumps({'retry': attempt, 'source': source, 'date': date, 'page': len(seen)+1,
                              'reason': reason, 'wait_seconds': round(delay, 1)}), flush=True)
            time.sleep(delay)
        result.extend(body['rows'])
        if not body.get('has_more'):
            break
        nxt = body.get('next_cursor')
        if not nxt or nxt in seen:
            raise RuntimeError(f'Cursor stalled source={source} date={date}')
        seen.add(nxt)
        cursor = nxt
        if len(seen) > 10000:
            raise RuntimeError(f'Excessive pages source={source} date={date}')
    return result


def parse_day(s):
    return dt.date.fromisoformat(s)


def main():
    mode = os.getenv('MIRROR_MODE', 'incremental')
    if mode not in ('incremental', 'backfill'):
        raise ValueError('Invalid MIRROR_MODE')
    today = dt.datetime.now(UTC).date()
    start = parse_day(os.environ['HISTORY_START_DATE']) if os.getenv('HISTORY_START_DATE') else (dt.date(2026, 10, 1) if mode == 'backfill' else today - dt.timedelta(days=2))
    last = parse_day(os.environ['HISTORY_END_DATE']) if os.getenv('HISTORY_END_DATE') else today
    if start > last or last > today:
        raise ValueError('Invalid date range')
    summary = {}
    for source, tc in SOURCES.items():
        day = start
        while day <= last:
            ts = int(dt.datetime.combine(day, dt.time(), UTC).timestamp() * 1000)
            rows = fetch(source, ts, ts + 86400000)
            path = ROOT / source / (day.isoformat() + '.json')
            if rows:
                path.parent.mkdir(parents=True, exist_ok=True)
                data = {'source': source, 'date_utc': day.isoformat(), 'time_column': tc,
                        'count': len(rows), 'rows': rows}
                serialized = json.dumps(data, ensure_ascii=False, separators=(',', ':')) + '\n'
                if not path.exists() or path.read_text(encoding='utf-8') != serialized:
                    path.write_text(serialized, encoding='utf-8')
                summary[str(path)] = len(rows)
            print(json.dumps({'source': source, 'date': day.isoformat(), 'rows': len(rows)}, ensure_ascii=False), flush=True)
            day += dt.timedelta(days=1)
    print(json.dumps({'mode': mode, 'files': summary}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
