"""Execute the transformation notebooks as the production pipeline.

Notebooks run in-process (no Jupyter kernel needed, so it works on Streamlit Cloud and CI).
Parameters are injected right after the cell tagged ``parameters`` (papermill convention).
With ``save_outputs=True`` printed text and displayed tables are written back into the
.ipynb so the committed notebooks show the results of the last run.
"""
from __future__ import annotations

import contextlib
import io
import json
import logging
import time
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


class NotebookError(RuntimeError):
    def __init__(self, notebook: str, cell_index: int, cause: BaseException) -> None:
        super().__init__(f"{notebook} failed in cell {cell_index}: {type(cause).__name__}: {cause}")
        self.notebook = notebook
        self.cell_index = cell_index
        self.cause = cause


def _display_payload(obj: Any) -> dict[str, Any]:
    if isinstance(obj, pd.DataFrame):
        return {"text/html": obj.to_html(index=False, border=0, max_rows=60),
                "text/plain": obj.to_string(index=False, max_rows=60)}
    if isinstance(obj, pd.Series):
        return {"text/plain": obj.to_string()}
    return {"text/plain": str(obj)}


def run_notebook(path: Path, params: dict[str, Any], save_outputs: bool = False) -> dict[str, Any]:
    nb = json.loads(path.read_text(encoding="utf-8"))
    outputs: list[dict[str, Any]] = []

    def display(*objs: Any, **_kw: Any) -> None:
        for obj in objs:
            outputs.append({"output_type": "display_data", "data": _display_payload(obj), "metadata": {}})

    namespace: dict[str, Any] = {"__name__": "__main__", "display": display}
    started = time.time()
    counter = 0
    for index, cell in enumerate(nb["cells"]):
        if cell["cell_type"] != "code":
            continue
        source = "".join(cell["source"])
        if not source.strip():
            continue
        counter += 1
        outputs = []
        stdout = io.StringIO()
        try:
            with contextlib.redirect_stdout(stdout):
                exec(compile(source, f"{path.name}[cell {index}]", "exec"), namespace)
                if "parameters" in cell.get("metadata", {}).get("tags", []):
                    namespace.update(params)
        except BaseException as exc:
            if save_outputs:
                _store(cell, counter, stdout.getvalue(), outputs, error=exc)
                path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
            con = namespace.get("con")
            if con is not None:
                with contextlib.suppress(Exception):
                    con.close()
            raise NotebookError(path.name, index, exc) from exc
        text = stdout.getvalue()
        for line in text.splitlines():
            log.info("[%s] %s", path.stem, line)
        if save_outputs:
            _store(cell, counter, text, outputs)

    if save_outputs:
        path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return {"notebook": path.name, "seconds": round(time.time() - started, 1), "cells": counter}


def _store(cell: dict[str, Any], counter: int, text: str, displays: list[dict[str, Any]],
           error: BaseException | None = None) -> None:
    cell["execution_count"] = counter
    cell["outputs"] = []
    if text:
        cell["outputs"].append({"output_type": "stream", "name": "stdout", "text": text.splitlines(keepends=True)})
    cell["outputs"].extend(displays)
    if error is not None:
        cell["outputs"].append({"output_type": "error", "ename": type(error).__name__, "evalue": str(error),
                                "traceback": traceback.format_exception_only(type(error), error)})
