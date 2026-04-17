import os
import subprocess
import tempfile
import shutil
import logging
import time
from pathlib import Path
# from typing import List, Dict, Tuple
# from collections import defaultdict
from clickhouse_driver import Client, errors

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== Конфигурация ==========
INPUT_DIR = os.environ.get('INPUT_DIR', '/var/lib/clickhouse/user_files/input')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/var/lib/clickhouse/user_files/output')
PROCESSED_PARQUET_DIR = os.environ.get('PROCESSED_PARQUET_DIR', '/var/lib/clickhouse/user_files/output/processed')
DBC_DIR = os.environ.get('DBC_DIR', '/var/lib/clickhouse/user_files/dbc')
DECODER_PATH = '/usr/local/bin/mdf2parquet_decode'

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse123')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'default')

CLICKHOUSE_RETRIES = 10
CLICKHOUSE_DELAY = 3


# # old function to delete #
# def apply_permissions_to_new_files(before: set[Path], output_dir: Path) -> None:
#     after = set(Path(output_dir).rglob('*'))
#     new_paths = after - before
#     if not new_paths:
#         return
#     for p in new_paths:
#         if p.is_dir():
#             p.chmod(0o755)
#         else:
#             p.chmod(0o644)
#     logger.info(f"Права доступа обновлены для {len(new_paths)} новых элементов в {output_dir}")


# ========== Вспомогательные функции для ClickHouse ==========
def wait_for_clickhouse() -> None:
    for attempt in range(1, CLICKHOUSE_RETRIES + 1):
        try:
            client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
                connect_timeout=2
            )
            client.execute("SELECT 1")
            client.disconnect()
            logger.info("ClickHouse готов к работе")
            return
        except errors.NetworkError:
            logger.warning(f"Попытка {attempt}/{CLICKHOUSE_RETRIES}: ClickHouse недоступен, ждём {CLICKHOUSE_DELAY}с...")
            time.sleep(CLICKHOUSE_DELAY)
    raise RuntimeError("Не удалось подключиться к ClickHouse")


def get_clickhouse_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def get_parquet_schema(client: Client, parquet_path: Path) -> list:
    query = f"DESCRIBE TABLE file('{parquet_path}', Parquet)"
    rows = client.execute(query)
    return [(row[0], row[1]) for row in rows]


def table_exists(client: Client, table_name: str) -> bool:
    result = client.execute(
        "SELECT 1 FROM system.tables WHERE database = %(db)s AND name = %(table)s",
        {'db': CLICKHOUSE_DATABASE, 'table': table_name}
    )
    return len(result) > 0


def ensure_table_for_message(client: Client, device_id: str, message_name: str, sample_file: Path) -> None:
    device_clean = device_id.replace('-', '_').replace('.', '_')
    msg_clean = message_name.replace('-', '_').replace('.', '_')
    table_name = f"tbl_{device_clean}_{msg_clean}"

    if table_exists(client, table_name):
        logger.debug(f"Таблица {table_name} уже существует")
        return

    schema = get_parquet_schema(client, sample_file)
    if not schema:
        raise RuntimeError(f"Пустая схема для {sample_file}")

    columns_def = ', '.join([f"`{name}` {typ}" for name, typ in schema])
    order_by = schema[0][0] if schema else '1'

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {columns_def}
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(t)
        ORDER BY {order_by}
    """
    client.execute(create_sql)
    logger.info(f"Создана таблица {table_name} (ReplacingMergeTree) для {device_id}/{message_name}")


def import_parquet_from_directory(client: Client, parquet_dir: Path) -> bool:
    """
    Импортирует все Parquet-файлы из указанной директории (ищет *.parquet) в соответствующую таблицу.
    Возвращает True, если импорт успешен, иначе False.
    """
    # Определяем device_id и message_name из пути относительно OUTPUT_DIR
    try:
        rel = parquet_dir.relative_to(Path(OUTPUT_DIR))
        parts = rel.parts
        if len(parts) < 2:
            logger.error(f"Неверная структура директории: {parquet_dir} (ожидается OUTPUT_DIR/device/message/...)")
            return False
        device_id = parts[0]
        message_name = parts[1]
    except ValueError:
        logger.error(f"Директория {parquet_dir} не находится внутри OUTPUT_DIR")
        return False

    # Создаём таблицу, если её нет (на основе первого найденного файла)
    parquet_files = list(parquet_dir.glob('*.parquet')) + list(parquet_dir.glob('*.PARQUET'))
    if not parquet_files:
        logger.warning(f"Нет Parquet-файлов в {parquet_dir}")
        return False

    ensure_table_for_message(client, device_id, message_name, parquet_files[0])

    device_clean = device_id.replace('-', '_').replace('.', '_')
    msg_clean = message_name.replace('-', '_').replace('.', '_')
    table_name = f"tbl_{device_clean}_{msg_clean}"

    # Используем wildcard для всех parquet-файлов в директории
    # ClickHouse поддерживает * в функции file()
    pattern = str(parquet_dir / '*.parquet')
    insert_sql = f"INSERT INTO `{table_name}` SELECT * FROM file('{pattern}', Parquet)"

    try:
        client.execute(insert_sql)
        logger.info(f"Импортированы файлы из {parquet_dir} -> {table_name}")
        return True
    except Exception as e:
        logger.error(f"Ошибка импорта из {parquet_dir}: {e}")
        return False


def move_parquet_files_to_processed(parquet_dir: Path) -> None:
    """
    Перемещает все Parquet-файлы из parquet_dir в PROCESSED_PARQUET_DIR,
    сохраняя относительный путь от OUTPUT_DIR.
    После перемещения удаляет пустые директории.
    """
    processed_root = Path(PROCESSED_PARQUET_DIR)
    # Определяем относительный путь от OUTPUT_DIR
    try:
        rel_path = parquet_dir.relative_to(Path(OUTPUT_DIR))
    except ValueError:
        logger.error(f"Не удаётся определить относительный путь для {parquet_dir}")
        return

    target_dir = processed_root / rel_path
    target_dir.mkdir(parents=True, exist_ok=True)

    # Перемещаем все parquet-файлы
    moved_count = 0
    for ext in ['*.parquet', '*.PARQUET']:
        for f in parquet_dir.glob(ext):
            shutil.move(str(f), str(target_dir / f.name))
            moved_count += 1

    if moved_count:
        logger.info(f"Перемещено {moved_count} файлов из {parquet_dir} в {target_dir}")

    # Удаляем пустые директории
    try:
        # Удаляем саму директорию, если она пуста
        if not any(parquet_dir.iterdir()):
            parquet_dir.rmdir()
            logger.debug(f"Удалена пустая директория {parquet_dir}")
        # Поднимаемся вверх и удаляем пустые родительские директории
        parent = parquet_dir.parent
        while parent != Path(OUTPUT_DIR) and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
            logger.debug(f"Удалена пустая директория {parent}")
            parent = parent.parent
    except OSError as e:
        logger.warning(f"Не удалось удалить директорию {parquet_dir}: {e}")


def import_all_new_parquet_files(client: Client) -> None:
    """
    Сканирует OUTPUT_DIR, находит все директории, содержащие Parquet-файлы,
    импортирует их и перемещает обработанные файлы.
    """
    output_root = Path(OUTPUT_DIR)
    if not output_root.exists():
        logger.warning(f"Директория {OUTPUT_DIR} не существует")
        return

    # Собираем все директории, которые содержат хотя бы один parquet-файл
    # (не заходя в processed)
    parquet_dirs = set()
    for ext in ['*.parquet', '*.PARQUET']:
        for file_path in output_root.rglob(ext):
            # Пропускаем файлы внутри processed
            if PROCESSED_PARQUET_DIR in str(file_path.parent) or 'processed' in file_path.parts:
                continue
            parquet_dirs.add(file_path.parent)

    if not parquet_dirs:
        logger.info("Нет директорий с Parquet-файлами для импорта")
        return

    logger.info(f"Найдено {len(parquet_dirs)} директорий с Parquet-файлами")
    imported_count = 0
    for dir_path in sorted(parquet_dirs):
        if import_parquet_from_directory(client, dir_path):
            move_parquet_files_to_processed(dir_path)
            imported_count += 1

    logger.info(f"Обработано директорий: {imported_count} из {len(parquet_dirs)}")


# ========== Обработка MF4 ==========
class MF4Handler:
    def __init__(self, clickhouse_client: Client):
        self.processed_dir = Path(INPUT_DIR) / 'processed'
        self.processed_dir.mkdir(exist_ok=True)
        self.client = clickhouse_client
        self.input_dir = Path(INPUT_DIR)

    def process_file(self, file_path: Path) -> None:
        logger.info(f"Обработка файла: {file_path.name}")

        with tempfile.TemporaryDirectory() as temp_input:
            temp_input_path = Path(temp_input)
            temp_mf4 = temp_input_path / file_path.name
            shutil.copy(file_path, temp_mf4)

            with tempfile.TemporaryDirectory() as temp_work:
                work_dir = Path(temp_work)
                for dbc_file in Path(DBC_DIR).glob('*.dbc'):
                    shutil.copy(dbc_file, work_dir)

                cmd = [
                    DECODER_PATH,
                    '-i', str(temp_input_path),
                    '-O', OUTPUT_DIR,
                    '-X'
                ]

                try:
                    subprocess.run(
                        cmd,
                        cwd=str(work_dir),
                        capture_output=True,
                        text=True,
                        check=True,
                        timeout=3600
                    )
                    logger.info(f"Декодирование успешно: {file_path.name}")

                    # Перемещаем исходный MF4 в обработанные
                    relative_path = file_path.relative_to(self.input_dir)
                    processed_path = self.processed_dir / relative_path
                    processed_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(file_path), str(processed_path))
                    logger.info(f"Файл перемещён в {processed_path}")

                    # Удаляем пустые директории, начиная с той, где лежал исходный файл
                    parent_dir = file_path.parent
                    try:
                        # Если директория пуста — удаляем её
                        if not any(parent_dir.iterdir()):
                            parent_dir.rmdir()
                            logger.debug(f"Удалена пустая директория {parent_dir}")
                            # Поднимаемся вверх, пока не дойдём до INPUT_DIR
                            parent = parent_dir.parent
                            while parent != self.input_dir and parent.exists() and not any(parent.iterdir()):
                                parent.rmdir()
                                logger.debug(f"Удалена пустая директория {parent}")
                                parent = parent.parent
                    except OSError as e:
                        logger.warning(f"Не удалось удалить директорию {parent_dir}: {e}")

                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка декодирования {file_path.name}: код {e.returncode}")
                    logger.error(f"STDERR: {e.stderr}")
                except Exception as e:
                    logger.error(f"Неожиданная ошибка: {e}", exc_info=True)


def process_existing_mf4(client: Client) -> None:
    input_path = Path(INPUT_DIR)
    mf4_files = []
    for ext in ['*.MF4', '*.mf4']:
        for file in input_path.rglob(ext):
            if 'processed' not in file.parts:
                mf4_files.append(file)

    if not mf4_files:
        logger.info("Нет существующих .MF4 файлов для обработки")
        return

    logger.info(f"Найдено {len(mf4_files)} существующих файлов. Начинаю обработку...")
    handler = MF4Handler(client)
    for mf4_file in mf4_files:
        handler.process_file(mf4_file)


# ========== Основная функция ==========
def main() -> None:
    logger.info("Запуск оркестратора обработки MF4 -> Parquet -> ClickHouse")
    if not os.path.exists(DECODER_PATH):
        logger.error(f"Декодер не найден по пути {DECODER_PATH}")
        return

    if not os.path.exists(INPUT_DIR):
        logger.error(f"Входная директория {INPUT_DIR} не существует")
        return

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_PARQUET_DIR).mkdir(parents=True, exist_ok=True)

    try:
        wait_for_clickhouse()
    except RuntimeError as e:
        logger.error(str(e))
        return

    client = get_clickhouse_client()
    try:
        # import_all_new_parquet_files(client)

        # old # Сохраняем состояние OUTPUT_DIR до декодирования
        # old # before_paths = set(Path(OUTPUT_DIR).rglob('*'))

        process_existing_mf4(client)
        # apply_permissions_to_new_files(before_paths, Path(OUTPUT_DIR))

        import_all_new_parquet_files(client)

    finally:
        client.disconnect()

    logger.info("Оркестратор завершил работу.")


if __name__ == "__main__":
    main()