# Run it

Both engines need Fabric: there is no Fabric-free end-to-end dbt run. What is free, takes
seconds and needs no credentials are the offline checks — run them **before spending any
capacity**, because they catch the mistakes a real run would find only after paying for them.

## Offline checks

```bash
pip install -r requirements/dev.txt
python -m pytest tests_py/ -q                  # column spec, SQL dialects, workflows, scripts

pip install -r requirements/dwh.txt            # or requirements/spark.txt, in its own env
python .github/scripts/check_gating.py dwh     # one engine, in that engine's own env
```

`check_gating.py` runs `dbt parse` with dummy env vars and asserts that exactly the eight
models and that engine's tests are enabled. It is not optional: the default failure mode of
this layout is a run that builds nothing and exits 0 ([layout.md](layout.md)).

Gating runs **one engine per environment**, because the two adapters cannot share one:
`dbt-fabric` and `dbt-fabricspark` shadow each other under the `dbt.adapters` namespace. CI
runs it as a two-way matrix, so each engine is validated against exactly its own pins.

## Landing, dry run

`download_aemo.py` writes to a local directory as readily as to OneLake, so the landing step
can be tried without a Fabric account:

```bash
pip install -r requirements/ops.txt
LANDING_PATH=./landing python download_aemo.py
```

## Against Fabric

The same steps as one leg of `pipeline.yml`, by hand. `provision.py` creates the items if they
are missing and prints the `KEY=value` lines the profile reads; the Azure CLI login is what
every script and both adapters authenticate with (`FABRIC_AUTH=CLI`).

```bash
az login
export FABRIC_WORKSPACE_ID=<workspace GUID>
pip install -r requirements/ops.txt -r requirements/dwh.txt     # or spark.txt

while IFS= read -r kv; do export "$kv"; done < <(python .github/scripts/provision.py dwh)
python download_aemo.py                                         # writes to LANDING_PATH
cd dbt1 && dbt build --target dwh --profiles-dir .
```

For `spark`, also `export DBT_FABRICSPARK_SKIP_OPTIMIZE=true` (see
[engine nuances](engine-nuances.md)). `process_limit` (default `1000`) caps how many archive
files each fact model folds per run; set it low for a first build.
