# 
from __future__ import annotations

import logging
import os
import tempfile
import zipfile

from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.configuration import conf
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

TIMEZONE = "Europe/Moscow"
RETENTION_DAYS = 30

DAGS_FOLDER = Path(
    conf.get("core", "dags_folder")
).resolve()

BACKUP_FOLDER = Path(
    os.environ.get(
        "AIRFLOW_DAG_BACKUP_DIR",
        "/opt/airflow/dag_backups",
    )
).resolve()

EXCLUDED_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".git",
    ".venv",
    "venv",
}

EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def is_inside(path: Path, parent: Path) -> bool:
    """Проверяет, находится ли путь внутри parent или совпадает с ним."""
    try:
        return os.path.commonpath(
            [str(path.resolve()), str(parent.resolve())]
        ) == str(parent.resolve())
    except (ValueError, OSError):
        return False


def collect_files() -> list[Path]:
    """Собирает файлы DAG с сохранением структуры каталогов."""
    if not DAGS_FOLDER.is_dir():
        raise RuntimeError(
            f"Папка DAG не существует: {DAGS_FOLDER}"
        )

    if DAGS_FOLDER == BACKUP_FOLDER:
        raise RuntimeError(
            "Папка бэкапа не должна совпадать с папкой DAG"
        )

    files = []

    for root, dirs, filenames in os.walk(
        DAGS_FOLDER,
        followlinks=False,
    ):
        root_path = Path(root)

        dirs[:] = sorted(
            d for d in dirs
            if d not in EXCLUDED_DIRS
            and not (root_path / d).is_symlink()
            and not is_inside(root_path / d, BACKUP_FOLDER)
        )

        for filename in sorted(filenames):
            path = root_path / filename

            if path.is_symlink():
                continue

            if path.suffix.lower() in EXCLUDED_SUFFIXES:
                continue

            if not path.is_file():
                continue

            files.append(path)

    return sorted(files)


def cleanup_old_backups(today: date) -> None:
    """Удаляет только папки резервных копий старше срока хранения."""
    cutoff = today - timedelta(days=RETENTION_DAYS)

    for folder in BACKUP_FOLDER.iterdir():
        if not folder.is_dir() or folder.is_symlink():
            continue

        try:
            folder_date = datetime.strptime(
                folder.name, "%Y-%m-%d"
            ).date()
        except ValueError:
            # Не трогаем папки с другими именами.
            continue

        if folder_date >= cutoff:
            continue

        try:
            # Удаляем только ожидаемую структуру:
            # дата / dags_backup.zip
            archive = folder / "dags_backup.zip"

            if not archive.is_file() or archive.is_symlink():
                logger.warning(
                    "Пропуск старой папки с неизвестным содержимым: %s",
                    folder,
                )
                continue

            for item in folder.iterdir():
                if item.is_file() and not item.is_symlink():
                    item.unlink()
                else:
                    logger.warning(
                        "Неизвестный объект в папке бэкапа: %s",
                        item,
                    )

            # Удаляем папку только если она стала пустой.
            folder.rmdir()
            logger.info("Удалена старая копия: %s", folder)

        except OSError:
            logger.exception(
                "Не удалось удалить старую копию: %s", folder
            )


def backup_dags() -> None:
    now = pendulum.now(TIMEZONE)
    today = now.format("YYYY-MM-DD")

    BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

    if is_inside(BACKUP_FOLDER, DAGS_FOLDER):
        logger.warning(
            "Папка бэкапа находится внутри папки DAG. "
            "Она будет исключена из архива."
        )

    daily_folder = BACKUP_FOLDER / today
    daily_folder.mkdir(parents=True, exist_ok=True)

    archive_path = daily_folder / "dags_backup.zip"
    temp_path = None

    files = collect_files()

    if not files:
        raise RuntimeError(
            f"Нет файлов для резервного копирования: {DAGS_FOLDER}"
        )

    try:
        # Временный архив создаётся в той же папке,
        # чтобы os.replace мог атомарно заменить файл.
        with tempfile.NamedTemporaryFile(
            prefix=".dags_backup_",
            suffix=".tmp",
            dir=daily_folder,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

        with zipfile.ZipFile(
            temp_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            for path in files:
                relative_path = path.relative_to(DAGS_FOLDER)

                archive.write(
                    path,
                    arcname=relative_path.as_posix(),
                )

        # Проверяем целостность ZIP и наличие файлов.
        with zipfile.ZipFile(temp_path, "r") as archive:
            members = archive.namelist()

            if not members:
                raise RuntimeError("Создан пустой ZIP-архив")

            bad_file = archive.testzip()

            if bad_file is not None:
                raise RuntimeError(
                    f"Ошибка целостности архива: {bad_file}"
                )

        # Основной архив заменяется только после проверки.
        os.replace(temp_path, archive_path)
        temp_path = None

        logger.info(
            "Бэкап успешно создан: %s",
            archive_path,
        )
        logger.info("Количество файлов: %d", len(files))
        logger.info(
            "Размер архива: %d байт",
            archive_path.stat().st_size,
        )

    finally:
        # Убираем временный файл при ошибке.
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                logger.exception(
                    "Не удалось удалить временный архив: %s",
                    temp_path,
                )

    # Ротация выполняется только после успешного бэкапа.
    cleanup_old_backups(now.date())


with DAG(
    dag_id="dags_backup_to_zip",
    description="Ежедневный проверяемый бэкап DAG-файлов в ZIP",
    start_date=pendulum.datetime(
        2026, 1, 1, tz=TIMEZONE
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["maintenance", "backup"],
) as dag:

    PythonOperator(
        task_id="backup_dags_to_zip",
        python_callable=backup_dags,
    )