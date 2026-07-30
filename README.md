# warcstat

Summary stats for WARC files, as JSON.

## Usage

```bash
uv run warcstat path/to/file.warc.gz          # prints JSON stats to stdout
uv run warcstat file.warc.gz -o stats.json    # writes to a file
uv run warcstat file.warc.gz -o out/          # writes out/warcstat_output.json
uv run warcstat file.warc.gz --log-dir /tmp/logs
```

Or install it as a standalone command with `uv tool install .`.

## Arguments

| Argument | Description |
| --- | --- |
| `warc_file` | Path to the WARC (`.warc` or `.warc.gz`, both handled by warcio). Required. |
| `-o`, `--output` | Output file path, or a directory (auto-names `warcstat_output.json`). Defaults to stdout. |
| `--log-dir` | Log directory. Defaults to `./logs`. Always writes `warcstat.log` and mirrors to stderr. |

## Output

| Key | Description |
| --- | --- |
| `total_records` | Total records in the WARC |
| `total_bytes` | Total size of all records |
| `errors` | List of `{url, status}` for 4xx/5xx responses |
| `redirects` | List of `{url, status}` for 3xx responses |
| `hosts` | Request count per host |
| `http_status_codes` | Count per status code |
| `mime_types` | Count per MIME type |
| `record_types` | Count per WARC record type |

## Tests

```bash
uv run test_warcstat.py
```
