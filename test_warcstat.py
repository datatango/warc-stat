import io

from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders

from warcstat import collect_stats


def build_warc(responses):
    buffer = io.BytesIO()
    writer = WARCWriter(buffer, gzip=False)
    for uri, status, content_type in responses:
        headers = StatusAndHeaders(status, [('Content-Type', content_type)], protocol='HTTP/1.0')
        record = writer.create_warc_record(uri, 'response', payload=io.BytesIO(b'body'), http_headers=headers)
        writer.write_record(record)
    buffer.seek(0)
    return buffer


def test_collect_stats():
    warc = build_warc([
        ('http://example.com/', '200 OK', 'text/html; charset=utf-8'),
        ('http://example.com/missing', '404 Not Found', 'text/html'),
        ('http://other.org/image.png', '200 OK', 'image/png'),
    ])
    stats = collect_stats(warc)

    assert stats['total_records'] == 3
    assert stats['record_types'] == {'response': 3}
    assert stats['http_status_codes'] == {'200': 2, '404': 1}
    assert stats['hosts'] == {'example.com': 2, 'other.org': 1}
    # charset is stripped off the Content-Type, and the 404 is not counted
    assert stats['mime_types'] == {'text/html': 1, 'image/png': 1}
    assert stats['errors'] == [{'url': 'http://example.com/missing', 'status': '404'}]
    assert stats['total_bytes'] > 0


def test_redirects_are_not_errors():
    warc = build_warc([
        ('http://example.com/old', '301 Moved Permanently', 'text/html'),
        ('http://example.com/tmp', '302 Found', 'text/html'),
        ('http://example.com/boom', '500 Server Error', 'text/html'),
        ('http://example.com/ok', '200 OK', 'text/html'),
    ])
    stats = collect_stats(warc)

    assert stats['errors'] == [{'url': 'http://example.com/boom', 'status': '500'}]
    assert stats['redirects'] == [
        {'url': 'http://example.com/old', 'status': '301'},
        {'url': 'http://example.com/tmp', 'status': '302'},
    ]


def test_mime_types_ignore_non_2xx():
    warc = build_warc([
        ('http://example.com/ok', '200 OK', 'text/html'),
        # a redirect stub is labelled text/html but holds no real content
        ('http://example.com/old', '301 Moved Permanently', 'text/html'),
        ('http://example.com/gone', '404 Not Found', 'text/html'),
    ])
    stats = collect_stats(warc)

    assert stats['mime_types'] == {'text/html': 1}
    assert stats['http_status_codes'] == {'200': 1, '301': 1, '404': 1}


def test_malformed_status_is_skipped():
    warc = build_warc([('http://example.com/', 'BAD nonsense', 'text/html')])
    stats = collect_stats(warc)

    assert stats['errors'] == []
    assert stats['redirects'] == []
    assert stats['total_records'] == 1


if __name__ == '__main__':
    test_collect_stats()
    test_redirects_are_not_errors()
    test_mime_types_ignore_non_2xx()
    test_malformed_status_is_skipped()
    print('ok')
