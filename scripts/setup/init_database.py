#!/usr/bin/env python3
"""MongoDB & InfluxDB 데이터베이스 초기화"""

import asyncio
import os

from influxdb_client import InfluxDBClient
from motor.motor_asyncio import AsyncIOMotorClient


async def init_mongodb():
    """MongoDB 초기화"""
    mongodb_url = os.getenv("MONGODB_URL")
    if not mongodb_url:
        print("❌ MONGODB_URL 환경변수가 설정되지 않았습니다")
        return False

    try:
        client = AsyncIOMotorClient(mongodb_url)
        db = client.get_default_database()
        # await client.admin.command('ping')
        await db.command("ping")
        print("✅ MongoDB 연결 성공")

        # 기본 컬렉션 생성 및 인덱스 설정
        collections_indexes = {
            "trades": ["symbol", "timestamp", [("symbol", 1), ("timestamp", -1)]],
            "portfolio": ["symbol"],
            "settings": [[("category", 1), ("key", 1)]],
            "backtest_results": ["strategy_name", "created_at"],
            "daily_performance": ["date"],
        }

        for collection_name, indexes in collections_indexes.items():
            collection = db[collection_name]

            for index in indexes:
                if isinstance(index, list) and isinstance(index[0], tuple):
                    await collection.create_index(index)
                else:
                    await collection.create_index(index)

            print(f"✅ {collection_name} 컬렉션 및 인덱스 생성 완료")

        # 기본 설정 데이터 삽입
        settings_collection = db["settings"]
        default_settings = [
            {
                "category": "trading",
                "key": "risk_management",
                "value": {
                    "max_position_size": 0.02,
                    "stop_loss_ratio": 0.05,
                    "take_profit_ratio": 0.03,
                    "max_daily_trades": 10,
                },
                "description": "리스크 관리 설정",
                "is_active": True,
            },
            {
                "category": "trading",
                "key": "symbols",
                "value": {
                    "primary": "KRW-BTC",
                    "watchlist": ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
                },
                "description": "거래 대상 심볼",
                "is_active": True,
            },
        ]

        for setting in default_settings:
            await settings_collection.update_one(
                {"category": setting["category"], "key": setting["key"]},
                {"$set": setting},
                upsert=True,
            )

        print("✅ 기본 설정 데이터 생성 완료")
        client.close()
        return True

    except Exception as e:
        print(f"❌ MongoDB 초기화 실패: {e}")
        import traceback

        print(traceback.print_exc())
        return False


def init_influxdb():
    """InfluxDB 초기화"""
    influxdb_url = os.getenv("INFLUXDB_URL")
    influxdb_token = os.getenv("INFLUXDB_TOKEN")
    influxdb_org = os.getenv("INFLUXDB_ORG")
    influxdb_bucket = os.getenv("INFLUXDB_BUCKET")

    if not all([influxdb_url, influxdb_token, influxdb_org, influxdb_bucket]):
        print("⚠️  InfluxDB 환경변수가 설정되지 않았습니다.")
        return True

    try:
        client = InfluxDBClient(
            url=influxdb_url, token=influxdb_token, org=influxdb_org
        )

        health = client.health()
        if health.status == "pass":
            print("✅ InfluxDB 연결 성공")

            buckets_api = client.buckets_api()
            buckets = buckets_api.find_buckets()

            bucket_exists = any(b.name == influxdb_bucket for b in buckets.buckets)
            if bucket_exists:
                print(f"✅ 버킷 '{influxdb_bucket}' 확인됨")
            else:
                print(f"⚠️  버킷 '{influxdb_bucket}' 생성이 필요합니다")

            client.close()
            return True
        else:
            print(f"❌ InfluxDB 상태 확인 실패: {health.status}")
            return False

    except Exception as e:
        print(f"❌ InfluxDB 연결 실패: {e}")
        return False


async def main():
    from dotenv import load_dotenv

    load_dotenv()

    print("🚀 One Bailey MVP 데이터베이스 초기화")
    print("=" * 50)

    print("1️⃣  MongoDB 초기화...")
    mongodb_success = await init_mongodb()

    print("\n2️⃣  InfluxDB 초기화...")
    influxdb_success = init_influxdb()

    print("\n" + "=" * 50)

    if mongodb_success:
        print("🎉 데이터베이스 초기화 완료!")
        print("\n📋 다음 단계:")
        print("   • make dev - 개발 서버 시작")
        if not influxdb_success:
            print("   • InfluxDB 토큰 설정 (필요시)")
    else:
        print("❌ 데이터베이스 초기화 실패")


if __name__ == "__main__":
    asyncio.run(main())
