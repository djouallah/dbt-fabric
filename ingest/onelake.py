"""OneLake I/O for the scripts, on Microsoft's own SDKs: azure-identity for the token,
azure-storage-file-datalake for bytes.

`Store` is one item section (`abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>/Files`
or `.../Tables`) or, for a dry run, a local directory -- the same operations either way.
ingest/download_aemo.py lands through it; .github/scripts/layout.py reads Delta logs through it.

DuckDB is a LIBRARY in those scripts, never a dbt adapter. Where it reads OneLake directly
(layout.py's parquet footers), `duckdb_secret` hands it the same azure-identity token.
"""
from __future__ import annotations

import os
import time

STORAGE_SCOPE = "https://storage.azure.com/.default"


def credential():
    """notebookutils inside a Fabric notebook; DefaultAzureCredential (the Azure CLI after
    `azure/login` on CI, or `az login` on a laptop) everywhere else."""
    try:
        import notebookutils  # type: ignore
        from azure.core.credentials import AccessToken

        class _Notebook:
            def get_token(self, *scopes, **kw):
                return AccessToken(notebookutils.credentials.getToken("storage"),
                                   int(time.time()) + 3000)

        return _Notebook()
    except ImportError:
        from azure.identity import DefaultAzureCredential

        return DefaultAzureCredential(exclude_interactive_browser_credential=True)


class Store:
    def __init__(self, root: str):
        self.root = root.rstrip("/")
        self.fs = None
        self.base = ""
        if self.root.startswith("abfss://"):
            from azure.storage.filedatalake import DataLakeServiceClient

            fs, rest = self.root[len("abfss://"):].split("@", 1)
            host, _, self.base = rest.partition("/")
            self._cred = credential()
            svc = DataLakeServiceClient(f"https://{host}", credential=self._cred)
            self.fs = svc.get_file_system_client(fs)

    def _path(self, rel: str) -> str:
        return "/".join(p for p in (self.base if self.fs else self.root, rel.strip("/")) if p)

    def uri(self, rel: str) -> str:
        """What DuckDB opens: the abfss URL, or the local path."""
        return "/".join(p for p in (self.root, rel.strip("/")) if p)

    def read(self, rel: str) -> bytes | None:
        """The file's bytes, or None when it does not exist."""
        if self.fs is None:
            p = self._path(rel)
            return open(p, "rb").read() if os.path.isfile(p) else None
        f = self.fs.get_file_client(self._path(rel))
        return f.download_file().readall() if f.exists() else None

    def listdir(self, rel: str, dirs: bool) -> list[str]:
        """Names directly under `rel` -- directories when `dirs`, else files. [] if absent."""
        if self.fs is None:
            p = self._path(rel)
            if not os.path.isdir(p):
                return []
            return sorted(n for n in os.listdir(p)
                          if os.path.isdir(os.path.join(p, n)) == dirs)
        try:
            return sorted(x.name.rsplit("/", 1)[-1]
                          for x in self.fs.get_paths(self._path(rel), recursive=False)
                          if bool(x.is_directory) == dirs)
        except Exception as e:  # noqa: BLE001 -- ResourceNotFound, and nothing else is expected
            if type(e).__name__ == "ResourceNotFoundError":
                return []
            raise

    def push(self, local_folder: str, rel: str, overwrite: bool) -> None:
        """Upload every file in `local_folder` to `rel/`. overwrite=False keeps a file that is
        already there (landed CSVs are immutable); overwrite=True replaces it."""
        for n in os.listdir(local_folder):
            src = os.path.join(local_folder, n)
            dst = self._path(f"{rel}/{n}" if rel else n)
            if self.fs is None:
                os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
                if overwrite or not os.path.exists(dst):
                    with open(src, "rb") as a, open(dst, "wb") as b:
                        b.write(a.read())
                continue
            f = self.fs.get_file_client(dst)
            if not overwrite and f.exists():
                continue
            with open(src, "rb") as data:
                f.upload_data(data, overwrite=True)

    def duckdb_secret(self, con) -> None:
        """Let a DuckDB connection read this store's abfss URLs. A no-op for a local store."""
        if self.fs is None:
            return
        con.sql("INSTALL azure; LOAD azure;")
        transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
        if transport:
            # The runner's TLS stack fails DuckDB's default transport; curl works.
            con.sql(f"SET GLOBAL azure_transport_option_type = '{transport}'")
        tok = self._cred.get_token(STORAGE_SCOPE).token
        con.execute(f"CREATE OR REPLACE SECRET onelake (TYPE azure, PROVIDER access_token, "
                    f"ACCESS_TOKEN '{tok}')")
