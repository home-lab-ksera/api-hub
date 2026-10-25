#!/usr/bin/env python3
"""隔離HTTP fixtureで64/256MiB転送を比較する。本番へ接続しない。"""
import argparse
import http.client
import http.server
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from urllib.parse import urlsplit

CHUNK = bytes(65536)


class Storage(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def consume(self):
        left = int(self.headers.get('content-length', '0'))
        while left:
            data = self.rfile.read(min(left, len(CHUNK)))
            if not data:
                raise ConnectionError('incomplete fixture upload')
            left -= len(data)

    def reply(self, body):
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('ETag', 'fixture-tag')
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        self.consume()
        if 'uploads' in urlsplit(self.path).query:
            self.reply(b'<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><UploadId>fixture</UploadId></InitiateMultipartUploadResult>')
        else:
            self.reply(b'<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><ETag>fixture-tag</ETag></CompleteMultipartUploadResult>')

    def do_PUT(self):
        self.consume()
        self.reply(b'')

    def do_GET(self):
        left = int(urlsplit(self.path).path.rsplit('/', 1)[-1])
        self.send_response(200)
        self.send_header('Content-Length', str(left))
        self.end_headers()
        while left:
            part = CHUNK[:min(left, len(CHUNK))]
            self.wfile.write(part)
            left -= len(part)


def run(binary, size, mode):
    storage = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Storage)
    threading.Thread(target=storage.serve_forever, daemon=True).start()
    env = {k: v for k, v in os.environ.items() if not k.startswith('PG') and k != 'DATABASE_URL'}
    env.update(SLACK_BOT_TOKEN='fixture', RUSTFS_S3_ACCESS_KEY_ID='fixture',
               RUSTFS_S3_SECRET_ACCESS_KEY='fixture',
               RUSTFS_S3_ENDPOINT=f'http://127.0.0.1:{storage.server_port}')
    child = subprocess.Popen([binary], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(200):
            if child.poll() is not None:
                raise RuntimeError('API fixture did not start')
            try:
                connection = http.client.HTTPConnection('127.0.0.1', 3000, timeout=5)
                connection.request('GET', '/health')
                assert connection.getresponse().status == 200
                connection.close()
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise TimeoutError('API fixture readiness')
        connection = http.client.HTTPConnection('127.0.0.1', 3000, timeout=60)
        if mode == 'upload':
            connection.putrequest('POST', '/s3/put_object_stream?bucket=b&key=k')
            connection.putheader('Content-Length', str(size))
            connection.endheaders()
            left = size
            while left:
                data = CHUNK[:min(left, len(CHUNK))]
                connection.send(data)
                left -= len(data)
        else:
            connection.request('GET', f'/s3/preview/b/{size}')
        response = connection.getresponse()
        assert response.status == 200, response.read(4096)
        received = 0
        while data := response.read(len(CHUNK)):
            received += len(data)
        if mode == 'download':
            assert received == size
        connection.close()
        peak = next(int(line.split()[1]) for line in Path(f'/proc/{child.pid}/status').read_text().splitlines()
                    if line.startswith('VmHWM:'))
        return peak
    finally:
        child.terminate()
        try:
            child.wait(timeout=85)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
        storage.shutdown()
        storage.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    args = parser.parse_args()
    # 固定portを使う実binaryの起動試験。既存プロセスを上書きしない。
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 3000))
    for mode in ['upload', 'download']:
        small = run(str(args.binary.resolve()), 64 * 1024 * 1024, mode)
        large = run(str(args.binary.resolve()), 256 * 1024 * 1024, mode)
        print(f'{mode}: 64MiB peak={small}KiB; 256MiB peak={large}KiB', flush=True)
        assert large - small < 32 * 1024, 'memory grew with fixture size'


if __name__ == '__main__':
    main()
