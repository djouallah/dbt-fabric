"""Pin `pipeline.yml`'s `local_runner` toggle, and the partition of engines across build steps.

`local_runner` is a VERIFICATION MODE, off by default: it takes the three DuckDB legs off
Fabric compute and onto the GitHub runner, which is the only place their profiles' OWN
credentials are exercised. Inside a Fabric notebook the notebook's identity already authorises
OneLake and `remote_dbt.py`'s setup hook injects every token from `notebookutils`, so a green
in-Fabric run says nothing about whether the profile is right -- run 35424929721 built all
eight iceberg models with no secret at all, and the same profile on a runner died on the first
model with `Unauthorized`.

EVERY WAY THIS BREAKS IS GREEN. The leg builds in Fabric anyway, or it reaches no build step
at all and the fingerprint step publishes the previous run's numbers, or the runner's OneLake
transport leaks into the notebook. None of those is a red build; all of them are only visible
once a run has been paid for.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
import sys

import yaml

from _layout import ENGINES, REPO

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"
DUCKDB_LEGS = {"duckrun", "iceberg", "ducklake"}
CLIENT_LEGS = {"dwh", "spark"}


def workflow() -> dict:
    return yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))


def build() -> dict:
    return workflow()["jobs"]["build"]


def steps_matching(needle: str) -> list[dict]:
    return [s for s in build()["steps"] if needle in str(s.get("run", ""))]


def test_local_runner_takes_every_duckdb_leg_off_fabric():
    """ducklake used to be carved out of this expression -- `|| matrix.engine == 'ducklake'` --
    because only the in-Fabric script carried the 40613 loop that waits out its auto-paused
    catalog SQL DB, while the runner ran a bare `dbt build || dbt retry`. Both places run
    .github/scripts/run_dbt.py now, so a per-engine carve-out here would mean one engine is
    quietly exempt from the mode: you dispatch the run, it builds in Fabric, it is green, and
    the question you paid to answer is still unanswered."""
    remote = build()["env"]["REMOTE"]
    assert "inputs.local_runner" in remote, (
        "the REMOTE expression ignores local_runner, so the toggle does nothing and the DuckDB "
        "legs still build inside Fabric"
    )
    assert "matrix.engine ==" not in remote, (
        f"REMOTE is {remote!r} -- an engine is pinned to Fabric whatever the toggle says. Every "
        f"reason for that carve-out went when both paths started running run_dbt.py"
    )
    for engine in DUCKDB_LEGS:
        assert f'"{engine}"' in remote, f"{engine} is not in REMOTE's list, so it never goes to Fabric"
    for engine in CLIENT_LEGS:
        assert f'"{engine}"' not in remote, (
            f"{engine} is in REMOTE's list -- dbt is only a client for it and there is no "
            f"notebook path to send it down"
        )


def test_fabric_cores_stays_a_number():
    """Why local_runner is a boolean input of its own and not a `local` value on fabric_cores:
    that is a SIZE and this is a PLACE. remote_dbt.py reads FABRIC_CORES as int() on every run
    with the toggle OFF, so folding one into the other puts a non-numeric value in front of
    that int() in the mode nobody is testing -- a ValueError before anything starts."""
    cores = build()["env"]["FABRIC_CORES"]
    assert "local" not in cores, (
        f"build's FABRIC_CORES is {cores!r} -- remote_dbt.py does int() on it"
    )
    opts = workflow()
    trig = opts.get("on") or opts[True]  # YAML 1.1 reads a bare `on:` key as True
    values = trig["workflow_dispatch"]["inputs"]["fabric_cores"]["options"]
    assert all(v.isdigit() for v in values), f"fabric_cores has a non-numeric option: {values}"


def test_a_local_duckdb_leg_mints_the_tokens_its_profile_needs():
    """In Fabric remote_dbt.py's setup hook mints these from notebookutils, which is exactly why
    an in-Fabric run cannot prove the profile's own credentials. On the runner nothing mints
    them, and the two fail DIFFERENTLY: env_var('ONELAKE_TOKEN') has no default, so dbt dies at
    profile render (loud, but only once you have paid for the job), while
    DBT_ENV_SECRET_SQL_TOKEN defaults to '' -- the profile renders fine and the leg dies later
    at the DuckLake ATTACH, on a login error that reads like a catalog fault."""
    onelake = steps_matching("ONELAKE_TOKEN=")
    assert onelake, "no build step mints ONELAKE_TOKEN; a local iceberg or ducklake leg cannot " \
                    "render its profile"
    cond = str(onelake[0].get("if", ""))
    assert "env.REMOTE != 'true'" in cond, (
        f"the ONELAKE_TOKEN step's condition is {cond!r}; in Fabric the token must come from "
        f"notebookutils and never travel"
    )
    for engine in ("iceberg", "ducklake"):
        assert engine in cond, f"{engine} reads env_var('ONELAKE_TOKEN') but is not in {cond!r}"
    assert "duckrun" not in cond, (
        "duckrun gets no azure/login -- it mints from the OIDC assertion -- so `az` would fail"
    )
    assert "add-mask" in str(onelake[0]["run"]), "the minted token is not masked in the log"

    sql = steps_matching("DBT_ENV_SECRET_SQL_TOKEN=")
    assert sql, (
        "no build step mints DBT_ENV_SECRET_SQL_TOKEN; a local ducklake leg then attaches its "
        "catalog SQL DB with the profile's '' default"
    )
    cond = str(sql[0].get("if", ""))
    assert "env.REMOTE != 'true'" in cond and "ducklake" in cond, (
        f"the catalog-token step's condition is {cond!r}"
    )
    run = str(sql[0]["run"])
    assert "https://database.windows.net/" in run, (
        "a Fabric SQL DB wants the database.windows.net audience, not the storage one"
    )
    assert "add-mask" in run, "the minted token is not masked in the log"


def test_every_engine_reaches_exactly_one_build_step():
    """An engine matching NO build step does not fail -- the step is simply skipped. The
    `Fingerprint gold layer` step then runs its run-operation against whatever that engine's
    tables held from the LAST run, and parity.py compares a stale fingerprint as if it were
    this run's: green, and wrong in the one measurement this repo exists to make."""
    src = (REPO / ".github" / "scripts" / "run_dbt.py").read_text(encoding="utf-8")
    m = re.search(r"^PROJECT = \{(.*?)\}", src, re.M | re.S)
    assert m, "run_dbt.py has no PROJECT dict"
    driver = set(re.findall(r'"([a-z]+)":', m.group(1)))

    client_step = [s for s in build()["steps"]
                   if s.get("name") == "dbt build" and "working-directory" in s]
    assert len(client_step) == 1, "the dwh/spark build step is not where this test expects it"
    cond = str(client_step[0]["if"])
    client = {e for e in ENGINES if f"matrix.engine == '{e}'" in cond}

    assert driver & client == set(), f"{driver & client} matches two build steps"
    assert driver | client == set(ENGINES), (
        f"{set(ENGINES) - (driver | client)} reaches no build step at all"
    )

    driver_step = [s for s in build()["steps"] if s.get("name") == "dbt build on this runner"]
    assert len(driver_step) == 1
    driver_cond = str(driver_step[0]["if"])
    for engine in client:
        assert f"matrix.engine != '{engine}'" in driver_cond, (
            f"{engine} would reach run_dbt.py, which does not know it, instead of its own step"
        )


def test_a_local_leg_fingerprints_exactly_once():
    """A leg that writes no history/parity/<engine>.json is INVISIBLE: the upload is
    `if-no-files-found: warn`, and `parity.py compare` with fewer than two fingerprints prints
    "need at least 2 to compare" and returns 0. The acceptance test silently does not run."""
    steps = build()["steps"]
    driver = [s for s in steps if s.get("name") == "dbt build on this runner"]
    assert driver and "--no-fingerprint" in str(driver[0]["run"]), (
        "the runner's build step does not pass --no-fingerprint, so a local DuckDB leg "
        "fingerprints twice -- a second ATTACH and a second gold-table scan per run"
    )
    fp = [s for s in steps if s.get("name") == "Fingerprint gold layer"]
    assert len(fp) == 1
    assert str(fp[0]["if"]).strip() == "env.REMOTE != 'true'", (
        f"the fingerprint step's condition is {fp[0]['if']!r} -- an engine carve-out here leaves "
        f"a local leg with no fingerprint at all"
    )
    remote_dbt = (REPO / ".github" / "scripts" / "remote_dbt.py").read_text(encoding="utf-8")
    args = re.search(r"args=(\[[^]]*\])", remote_dbt)
    assert args, "remote_dbt.py no longer passes an args list to run_python"
    assert "--no-fingerprint" not in args.group(1), (
        f"the Fabric path passes {args.group(1)}; it must NOT skip the fingerprint -- "
        f"remote_dbt.py lifts that JSON out of the streamed notebook log, and there is no "
        f"second step out there to take it"
    )


def test_the_runner_transport_never_reaches_fabric():
    """run_dbt.py used to delete AZURE_TRANSPORT_OPTION_TYPE and CURL_CA_INFO on arrival, which
    is right in a notebook and the OPPOSITE of right on the runner, where pipeline.yml sets them
    on purpose and DuckDB fails the OneLake TLS handshake without them. The pops are gone now
    that one script runs in both places, so this allowlist is all that keeps them out of Fabric
    -- and forwarding them would break every REMOTE leg in a way only a paid run would show."""
    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    import remote_dbt  # noqa: PLC0415

    for name in ("AZURE_TRANSPORT_OPTION_TYPE", "CURL_CA_INFO", "DUCKDB_TEMP_DIR"):
        assert name not in remote_dbt.FORWARD, (
            f"{name} is forwarded into the Fabric notebook; it is a RUNNER setting"
        )
        assert name in build()["env"], (
            f"{name} is no longer set at job level, so a local DuckDB leg loses it"
        )
