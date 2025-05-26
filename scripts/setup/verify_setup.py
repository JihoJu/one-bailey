#!/usr/bin/env python3
"""환경 설정 검증"""

import asyncio
import importlib
import os
import sys

import redis
from influxdb_client import InfluxDBClient
from motor.motor_asyncio import AsyncIOMotorClient


def check_python_version():
    """Python 버전 확인"""
    version = sys.version_info
    print(f"🐍 Python: {version.major}.{version.minor}.{version.micro}")
    return version.major == 3 and version.minor >= 12


def check_packages():
    """패키지 설치 확인"""
    packages = [
        "pandas",
        "numpy",
        "pyupbit",
        "pymongo",
        "motor",
        "beanie",
        "influxdb_client",
        "fastapi",
        "streamlit",
        "redis",
        "celery",
        "websockets",
        "pytest",
    ]

    missing = []
    for pkg in packages:
        try:
            importlib.import_module(pkg.replace("-", "_"))
            print(f"  ✅ {pkg}")
        except ImportError:
            missing.append(pkg)
            print(f"  ❌ {pkg}")

    return len(missing) == 0


async def check_mongodb():
    """MongoDB 연결 확인"""
    mongodb_url = os.getenv("MONGODB_URL")
    if not mongodb_url:
        print("  ❌ MONGODB_URL 없음")
        return False

    try:
        client = AsyncIOMotorClient(mongodb_url)
        await client.admin.command("ping")

        # 컬렉션 존재 확인
        db = client.get_default_database()
        collections = await db.list_collection_names()

        required_collections = ["trades", "portfolio", "settings"]
        missing_collections = [
            col for col in required_collections if col not in collections
        ]

        if missing_collections:
            print(f"  ⚠️  누락된 컬렉션: {missing_collections}")
        else:
            print("  ✅ MongoDB 연결 및 컬렉션 확인")

        client.close()
        return len(missing_collections) == 0

    except Exception as e:
        print(f"  ❌ MongoDB 연결 실패: {e}")
        return False


def check_influxdb():
    """InfluxDB 연결 확인"""
    influxdb_url = os.getenv("INFLUXDB_URL")
    influxdb_token = os.getenv("INFLUXDB_TOKEN")
    influxdb_org = os.getenv("INFLUXDB_ORG")

    if not all([influxdb_url, influxdb_token, influxdb_org]):
        print("  ⚠️  InfluxDB 환경변수 누락")
        return True  # 선택사항이므로 True 반환

    try:
        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org
        )

        health = client.health()
        if health.status == "pass":
            print("  ✅ InfluxDB 연결")
            return True
        else:
            print(f"  ❌ InfluxDB 상태: {health.status}")
            return False

    except Exception as e:
        print(f"  ❌ InfluxDB 연결 실패: {e}")
        return False


def check_redis():
    """Redis 연결 확인"""
    try:
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        r.ping()
        print("  ✅ Redis 연결")
        return True
    except Exception as e:
        print(f"  ❌ Redis 연결 실패: {e}")
        return False


def check_docker():
    """Docker 컨테이너 상태 확인"""
    import subprocess

    try:
        # result = subprocess.run(
        #     ["docker-compose", "ps", "--services", "--filter", "status=running"],
        #     capture_output=True,
        #     text=True,
        #     check=True,
        # )

        # running_services = (
        #     result.stdout.strip().split("\n") if result.stdout.strip() else []
        # )

        # 실제 컨테이너 상태도 확인
        ps_result = subprocess.run(
            ["docker-compose", "ps"], capture_output=True, text=True, check=True
        )

        containers = ["mongodb", "influxdb", "redis"]
        running_containers = []

        for container in containers:
            if container in ps_result.stdout and "running" in ps_result.stdout:
                running_containers.append(container)

        print(f"  ✅ 실행 중인 컨테이너: {running_containers}")
        return len(running_containers) >= 2  # MongoDB + Redis 최소 필요

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Docker Compose 상태 확인 실패: {e}")
        return False
    except FileNotFoundError:
        print("  ❌ Docker Compose 없음")
        return False


async def main():
    """메인 검증 함수"""
    from dotenv import load_dotenv

    load_dotenv()

    print("🔍 One Bailey MVP 환경 검증")
    print("=" * 50)

    checks = [
        ("Python 버전", check_python_version),
        ("패키지 설치", check_packages),
        ("Docker 컨테이너", check_docker),
        ("MongoDB", check_mongodb),
        ("InfluxDB", check_influxdb),
        ("Redis", check_redis),
    ]

    results = []

    for name, check_func in checks:
        print(f"\n{name}")
        if asyncio.iscoroutinefunction(check_func):
            results.append(await check_func())
        else:
            results.append(check_func())

    print("\n" + "=" * 50)

    if all(results):
        print("🎉 환경 설정 완료!")
        print("\n🚀 다음 명령어로 시작:")
        print("  make dev")
        print("\n🌐 접속 주소:")
        print("  • MongoDB GUI: http://localhost:8082 (trader/secure_password)")
        print("  • InfluxDB UI: http://localhost:8086")
        print("  • Redis GUI: http://localhost:8081")
    else:
        print("❌ 일부 설정 미완료")
        print("\n🔧 해결 방법:")
        print("  1. poetry install")
        print("  2. docker-compose up -d")
        print("  3. make db-init")
        print("  4. .env 파일 API 키 설정")


if __name__ == "__main__":
    asyncio.run(main())
