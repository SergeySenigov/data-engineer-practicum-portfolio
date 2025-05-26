import os
from logging import Logger
from pathlib import Path

from lib import PgConnect


class SchemaDdl:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: str) -> None:

        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} files to apply changes.")

        i = 1
        for fp in file_paths:
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            i += 1
