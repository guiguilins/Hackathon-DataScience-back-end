from __future__ import annotations

from pathlib import Path
from typing import Iterable

import kagglehub


def download_dataset(dataset_ref: str) -> Path:
    """
    Faz o download do dataset e retorna o caminho local da pasta.
    """
    path = kagglehub.dataset_download(dataset_ref)
    return Path(path)


def list_dataset_files(dataset_path: Path) -> list[Path]:
    """
    Lista todos os arquivos do dataset recursivamente.
    """
    return sorted([p for p in dataset_path.rglob("*") if p.is_file()])


def filter_files_by_extension(files: Iterable[Path], extensions: tuple[str, ...]) -> list[Path]:
    return [f for f in files if f.suffix.lower() in extensions]