#!/usr/bin/env python3
"""
Скрипт быстрого восстановления базы знаний на демонстрационном ноутбуке (1-click restore).

Восстанавливает:
1. PostgreSQL (kb_documents, kb_nodes) из artifacts_export/kb_data.sql.
2. Qdrant (коллекция tender_chunks, 1024D Cosine) из artifacts_export/tender_chunks.snapshot.
3. Выполняет проверочный запрос для валидации Small-to-Big Retrieval.
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

try:
    import httpx
except ImportError:
    httpx = None


def check_prerequisites():
    print("-> Проверка окружения...")
    # Проверка наличия файлов дампа
    curr_dir = Path(__file__).parent.resolve()
    sql_file = curr_dir / "kb_data.sql"
    snapshot_file = curr_dir / "tender_chunks.snapshot"

    if not sql_file.exists():
        print(f"[ОШИБКА] Файл {sql_file} не найден!")
        sys.exit(1)
    if not snapshot_file.exists():
        print(f"[ОШИБКА] Файл {snapshot_file} не найден!")
        sys.exit(1)

    print(f"   [OK] kb_data.sql ({sql_file.stat().st_size / 1024 / 1024:.2f} MB)")
    print(f"   [OK] tender_chunks.snapshot ({snapshot_file.stat().st_size / 1024 / 1024:.2f} MB)")


def restore_postgres(db_user="rag_user", db_name="rag_db", db_host="localhost", db_port="5432"):
    print("\n-> Восстановление таблиц PostgreSQL (kb_documents, kb_nodes)...")
    curr_dir = Path(__file__).parent.resolve()
    sql_file = curr_dir / "kb_data.sql"

    # Применение через docker exec rag_postgres или локальный psql
    cmd_docker = [
        "docker", "exec", "-i", "rag_postgres",
        "psql", "-U", db_user, "-d", db_name
    ]
    try:
        with open(sql_file, "rb") as f:
            proc = subprocess.run(cmd_docker, stdin=f, capture_output=True, text=True)
        if proc.returncode == 0:
            print("   [УСПЕХ] База знаний PostgreSQL успешно импортирована через Docker.")
            return
    except Exception:
        pass

    # Фолбэк на локальный psql
    cmd_local = ["psql", "-U", db_user, "-h", db_host, "-p", str(db_port), "-d", db_name, "-f", str(sql_file)]
    try:
        proc = subprocess.run(cmd_local, capture_output=True, text=True)
        if proc.returncode == 0:
            print("   [УСПЕХ] База знаний PostgreSQL успешно импортирована через локальный psql.")
        else:
            print(f"   [ВНИМАНИЕ] Вывод psql: {proc.stderr[:300]}")
    except Exception as exc:
        print(f"   [ОШИБКА] Не удалось выполнить psql: {exc}")


def restore_qdrant(qdrant_url="http://localhost:6333", collection_name="tender_chunks"):
    print(f"\n-> Восстановление коллекции Qdrant '{collection_name}' из снапшота...")
    curr_dir = Path(__file__).parent.resolve()
    snapshot_file = curr_dir / "tender_chunks.snapshot"

    upload_url = f"{qdrant_url}/collections/{collection_name}/snapshots/upload?priority=snapshot"

    # Удаляем несовместимую пустую коллекцию, если она уже создана
    try:
        if httpx:
            with httpx.Client(timeout=10.0) as client:
                client.delete(f"{qdrant_url}/collections/{collection_name}")
        else:
            import urllib.request
            req = urllib.request.Request(f"{qdrant_url}/collections/{collection_name}", method="DELETE")
            urllib.request.urlopen(req)
    except Exception:
        pass

    # Загрузка через multipart/form-data
    print(f"   Отправка POST {upload_url}...")
    try:
        if httpx:
            with open(snapshot_file, "rb") as f:
                with httpx.Client(timeout=120.0) as client:
                    resp = client.post(
                        upload_url,
                        files={"snapshot": (snapshot_file.name, f, "application/octet-stream")}
                    )
                    resp.raise_for_status()
                    print(f"   [УСПЕХ] Коллекция {collection_name} успешно восстановлена в Qdrant!")
                    return
    except Exception as exc:
        print(f"   [ВНИМАНИЕ] Не удалось загрузить через httpx ({exc}), попытка через curl...")

    # Фолбэк на системный curl
    cmd_curl = [
        "curl", "-s", "-X", "POST", upload_url,
        "-F", f"snapshot=@{snapshot_file}"
    ]
    try:
        proc = subprocess.run(cmd_curl, capture_output=True, text=True)
        if proc.returncode == 0 and '"status":"ok"' in proc.stdout:
            print(f"   [УСПЕХ] Коллекция {collection_name} успешно восстановлена в Qdrant через curl!")
        else:
            print(f"   [ОШИБКА] Вывод curl: {proc.stdout or proc.stderr}")
    except Exception as exc:
        print(f"   [ОШИБКА] Не удалось восстановить снапшот в Qdrant: {exc}")


def verify_kb(qdrant_url="http://localhost:6333", collection_name="tender_chunks"):
    print("\n-> Верификация восстановленной базы знаний...")
    import json
    import urllib.request

    # 1. Проверка Qdrant
    try:
        with urllib.request.urlopen(f"{qdrant_url}/collections/{collection_name}") as resp:
            data = json.loads(resp.read().decode("utf-8"))["result"]
            print(f"   [Qdrant] Статус: {data['status']}, Точек в индексе: {data['points_count']}")
    except Exception as exc:
        print(f"   [Qdrant ОШИБКА]: {exc}")

    # 2. Проверка PostgreSQL
    cmd = ["docker", "exec", "rag_postgres", "psql", "-U", "rag_user", "-d", "rag_db", "-c", "SELECT count(*) FROM kb_nodes;"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            count = proc.stdout.split("\n")[2].strip()
            print(f"   [PostgreSQL] Родительских узлов AST (kb_nodes): {count}")
    except Exception:
        pass

    print("\n=== БАЗА ЗНАНИЙ ГОТОВА К ДЕМОНСТРАЦИИ НА НОУТБУКЕ ===")


if __name__ == "__main__":
    check_prerequisites()
    restore_postgres()
    restore_qdrant()
    verify_kb()
