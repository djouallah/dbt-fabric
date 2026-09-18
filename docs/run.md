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

Gating runs **one engine per environment**, because the adapters cannot share one:
`dbt-fabric` and `dbt-fabricspark` shadow each other under the `dbt.adapters` namespace.
CI runs it as a five-way matrix; with no argument the script does every engine whose adapter
it can import.
