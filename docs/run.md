# Run it

The local loop first, then the offline checks — which are free, take seconds, and are a lot
cheaper than finding the same mistake in a run that spends capacity.

```bash
pip install -r requirements/duckrun.txt
export FILES_PATH=./landing ONELAKE_TABLES_PATH=./warehouse
python download_aemo.py
cd dbt1 && dbt build --target duckrun --profiles-dir .
```

That works on a laptop with no Fabric account: `duckrun` writes Delta to a local directory.
Swap `--target` for any of the other four once the Fabric env vars are set.

Check the wiring without credentials, and **before spending any capacity**:

```bash
pip install -r requirements/dev.txt
python -m pytest tests_py/ -q                    # column spec, adapter overrides, SQL dialects

pip install -r requirements/duckrun.txt
python .github/scripts/check_gating.py duckrun   # one engine, in that engine's own env
```

Gating runs **one engine per environment**, because two of the adapters cannot share one:
`dbt-fabric` and `dbt-fabricspark` shadow each other under the `dbt.adapters` namespace. The
three DuckDB legs can, though — `duckrun` brings `dbt-duckdb`, which is what `iceberg` and
`ducklake` both run on, so one `pip install -r requirements/duckrun.txt` reaches three of the
five in the no-argument form. CI runs it as a five-way matrix anyway, so each engine is
validated against exactly its own pins.
