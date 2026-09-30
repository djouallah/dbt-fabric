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

## Against Fabric

The run is `run_pipeline`, in the workspace: see the [README](../README.md) for the two ways
to install it. Its `ingest` notebook is the only thing that lands the AEMO files.

`run` and `parity` also run from a laptop, in VS Code on a local kernel, against your DEV
workspace, as your `az login`. The setup is in the
[README](../README.md#develop-in-vs-code).
