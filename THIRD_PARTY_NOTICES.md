# Third-party notices

## postgres-mcp

Parts of this project are adapted from [postgres-mcp](https://github.com/crystaldba/postgres-mcp) (commit 15c8e33):

- the read-only function allow list in `internal/sqlguard/allowlist.go`, taken from `src/postgres_mcp/sql/safe_sql.py` with some functions removed;
- the health check queries and thresholds in `internal/diag`, from `src/postgres_mcp/database_health/`;
- the index advisor search in `internal/advisor`, from `src/postgres_mcp/index/dta_calc.py` and `src/postgres_mcp/index/index_opt_base.py`.

```text
MIT License

Copyright (c) 2025, Crystal Corp.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
