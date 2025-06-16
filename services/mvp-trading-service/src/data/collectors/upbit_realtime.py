#!/usr/bin/env python3
"""
업비트 실시간 데이터 수집기
WebSocket을 통한 현재가 + 오더북 동시 수집 및 InfluxDB 저장
"""

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import psutil
import redis
import websockets

# 환경 설정
from dotenv import load_dotenv

# InfluxDB 클라이언트
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class InfluxConfig:
    """InfluxDB 설정"""

    url: str = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token: str = os.getenv(
        "INFLUXDB_TOKEN", "one-bailey-admin-token-12345678901234567890"
    )
    org: str = os.getenv("INFLUXDB_ORG", "one-bailey")
    bucket: str = os.getenv("INFLUXDB_BUCKET", "trading_data")


class RealtimeInfluxWriter:
    """
    실시간 InfluxDB 저장소
    현재가(ticker) + 오더북(orderbook) 혼합 처리
    """

    def __init__(self, config: InfluxConfig):
        self.config = config
        self.client = None
        self.write_api = None
        self._initialize_client()

        # 통계 분리
        self.ticker_write_count = 0
        self.orderbook_write_count = 0
        self.total_error_count = 0

    def _initialize_client(self):
        """InfluxDB 클라이언트 초기화"""
        try:
            self.client = InfluxDBClient(
                url=self.config.url,
                token=self.config.token,
                org=self.config.org,
                timeout=30000,  # 30초 타임아웃
            )

            # 통합 쓰기 API (더 큰 배치 사이즈)
            self.write_api = self.client.write_api(
                write_options=ASYNCHRONOUS,
                batch_size=1500,  # 현재가 + 오더북 합쳐서 처리
                flush_interval=5000,
                jitter_interval=1000,
                retry_interval=3000,
                max_retries=3,
            )

            # 연결 테스트
            health = self.client.health()
            if health.status == "pass":
                logger.info("✅ 실시간 InfluxDB 연결 성공")
            else:
                logger.error(f"❌ InfluxDB 상태 이상: {health.status}")

        except Exception as e:
            logger.error(f"❌ 실시간 InfluxDB 초기화 실패: {e}")
            raise

    def create_ticker_point(self, ticker_data: Dict[str, Any]) -> Point:
        """현재가 데이터 Point 생성"""
        try:
            point = (
                Point("ticker_data")
                .tag("symbol", ticker_data.get("symbol", "UNKNOWN"))
                .field("trade_price", float(ticker_data.get("trade_price", 0)))
                .field("change_rate", float(ticker_data.get("change_rate", 0)))
                .field("trade_volume", float(ticker_data.get("trade_volume", 0)))
                .field("volume_24h", float(ticker_data.get("acc_trade_volume_24h", 0)))
                .field(
                    "volume_24h_krw", float(ticker_data.get("acc_trade_price_24h", 0))
                )
                .field("high_price", float(ticker_data.get("high_price", 0)))
                .field("low_price", float(ticker_data.get("low_price", 0)))
                .field("prev_close", float(ticker_data.get("prev_closing_price", 0)))
            )

            # 타임스탬프 설정
            if "buffer_timestamp" in ticker_data:
                timestamp_ns = int(ticker_data["buffer_timestamp"] * 1_000_000_000)
                point = point.time(timestamp_ns)
            else:
                point = point.time(datetime.utcnow())

            return point

        except Exception as e:
            logger.error(f"❌ 현재가 Point 생성 실패: {e}")
            return None

    def create_orderbook_point(self, orderbook_data: Dict[str, Any]) -> Point:
        """오더북 데이터 Point 생성"""
        try:
            point = (
                Point("orderbook_summary")
                .tag("symbol", orderbook_data.get("symbol", "UNKNOWN"))
                .field("best_ask", float(orderbook_data.get("best_ask", 0)))
                .field("best_bid", float(orderbook_data.get("best_bid", 0)))
                .field("spread_abs", float(orderbook_data.get("spread", 0)))
                .field("spread_pct", float(orderbook_data.get("spread_percentage", 0)))
                .field("total_ask_size", float(orderbook_data.get("total_ask_size", 0)))
                .field("total_bid_size", float(orderbook_data.get("total_bid_size", 0)))
                .field(
                    "market_pressure", self._calculate_market_pressure(orderbook_data)
                )
                .field(
                    "liquidity_score", self._calculate_liquidity_score(orderbook_data)
                )
            )

            # 타임스탬프 설정
            if "buffer_timestamp" in orderbook_data:
                timestamp_ns = int(orderbook_data["buffer_timestamp"] * 1_000_000_000)
                point = point.time(timestamp_ns)
            else:
                point = point.time(datetime.utcnow())

            return point

        except Exception as e:
            logger.error(f"❌ 오더북 Point 생성 실패: {e}")
            return None

    def _calculate_market_pressure(self, orderbook_data: Dict[str, Any]) -> float:
        """시장 압력 계산"""
        total_ask = orderbook_data.get("total_ask_size", 0)
        total_bid = orderbook_data.get("total_bid_size", 0)

        if total_ask + total_bid == 0:
            return 0.0

        return (total_bid - total_ask) / (total_bid + total_ask)

    def _calculate_liquidity_score(self, orderbook_data: Dict[str, Any]) -> float:
        """유동성 점수 계산"""
        spread_pct = orderbook_data.get("spread_percentage", 0)
        total_volume = orderbook_data.get("total_ask_size", 0) + orderbook_data.get(
            "total_bid_size", 0
        )

        if spread_pct == 0:
            return 1.0

        spread_score = max(0, 1 - (spread_pct / 0.01))
        volume_score = min(1.0, total_volume / 10.0)

        return (spread_score + volume_score) / 2

    async def save_mixed_batch(self, batch_data: List[Dict[str, Any]]) -> bool:
        """혼합 배치 데이터 저장 (현재가 + 오더북)"""
        if not batch_data:
            return True

        try:
            points = []
            ticker_count = 0
            orderbook_count = 0

            for data in batch_data:
                data_type = data.get("data_type", "unknown")

                if data_type == "ticker":
                    point = self.create_ticker_point(data)
                    if point:
                        points.append(point)
                        ticker_count += 1

                elif data_type == "orderbook":
                    point = self.create_orderbook_point(data)
                    if point:
                        points.append(point)
                        orderbook_count += 1

                else:
                    logger.warning(f"⚠️ 알 수 없는 데이터 타입: {data_type}")

            if not points:
                logger.warning("⚠️ 유효한 Point가 없습니다")
                return False

            # 저장 실행
            self.write_api.write(
                bucket=self.config.bucket, org=self.config.org, record=points
            )

            # 통계 업데이트
            self.ticker_write_count += ticker_count
            self.orderbook_write_count += orderbook_count

            logger.info(
                f"✅ InfluxDB 저장: {len(points)}건 (현재가: {ticker_count}, \
                        오더북: {orderbook_count})"
            )

            return True

        except Exception as e:
            self.total_error_count += 1
            logger.error(f"❌ InfluxDB 저장 실패: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """통계 정보"""
        total_writes = self.ticker_write_count + self.orderbook_write_count
        success_rate = (
            total_writes / max(total_writes + self.total_error_count, 1)
        ) * 100

        return {
            "total_write_count": total_writes,
            "ticker_write_count": self.ticker_write_count,
            "orderbook_write_count": self.orderbook_write_count,
            "error_count": self.total_error_count,
            "success_rate": success_rate,
            "connection_status": "connected" if self.client else "disconnected",
        }

    def close(self):
        """연결 종료"""
        if self.write_api:
            self.write_api.close()
        if self.client:
            self.client.close()
        logger.info("🔌 실시간 InfluxDB 연결 종료")


class RealtimeDataBuffer:
    """
    실시간 데이터 버퍼
    현재가 + 오더북 혼합 처리
    """

    def __init__(
        self,
        max_size: int = 1500,
        batch_threshold: int = 150,
        flush_interval: float = 5.0,
    ):
        self.max_size = max_size
        self.batch_threshold = batch_threshold
        self.flush_interval = flush_interval
        self.buffer = deque(maxlen=max_size)
        self.last_flush_time = time.time()
        self._lock = threading.RLock()

        # 데이터 타입별 통계
        self.ticker_added = 0
        self.orderbook_added = 0
        self.total_flushed = 0

    def add_data(self, data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """데이터 추가 (타입 구분)"""
        with self._lock:
            # 타임스탬프 및 메타데이터 추가
            data["buffer_timestamp"] = time.time()

            # 데이터 타입별 카운팅
            data_type = data.get("data_type", "unknown")
            if data_type == "ticker":
                self.ticker_added += 1
            elif data_type == "orderbook":
                self.orderbook_added += 1

            self.buffer.append(data)

            # 플러시 조건 확인
            if self._should_flush():
                return self.flush()

        return None

    def _should_flush(self) -> bool:
        """플러시 조건 확인"""
        size_trigger = len(self.buffer) >= self.batch_threshold
        time_trigger = (time.time() - self.last_flush_time) >= self.flush_interval
        memory_trigger = len(self.buffer) > self.max_size * 0.8

        return size_trigger or time_trigger or memory_trigger

    def flush(self) -> List[Dict[str, Any]]:
        """버퍼 플러시"""
        with self._lock:
            if not self.buffer:
                return []

            batch_data = list(self.buffer)
            self.buffer.clear()
            self.last_flush_time = time.time()
            self.total_flushed += len(batch_data)

            # 배치 내용 분석
            ticker_count = sum(
                1 for data in batch_data if data.get("data_type") == "ticker"
            )
            orderbook_count = sum(
                1 for data in batch_data if data.get("data_type") == "orderbook"
            )

            logger.info(
                f"🔄 버퍼 플러시: {len(batch_data)}건 (현재가: {ticker_count}, \
                        오더북: {orderbook_count})"
            )

            return batch_data

    def get_stats(self) -> Dict[str, Any]:
        """버퍼 통계"""
        return {
            "buffer_size": len(self.buffer),
            "ticker_added": self.ticker_added,
            "orderbook_added": self.orderbook_added,
            "total_flushed": self.total_flushed,
            "buffer_usage": len(self.buffer) / self.max_size * 100,
        }


class SystemMonitor:
    """시스템 성능 모니터링"""

    def get_system_stats(self) -> Dict[str, Any]:
        """시스템 통계 조회"""
        process = psutil.Process()

        return {
            "cpu_percent": psutil.cpu_percent(interval=0.1),
            "memory_percent": psutil.virtual_memory().percent,
            "memory_usage_mb": process.memory_info().rss / 1024 / 1024,
            "process_cpu_percent": process.cpu_percent(),
        }


class UpbitRealtimeCollector:
    """
    업비트 실시간 데이터 수집기
    현재가 + 오더북 WebSocket 동시 관리
    """

    def __init__(self):
        self.websocket_url = "wss://api.upbit.com/websocket/v1"
        self.ticker_websocket = None
        self.orderbook_websocket = None
        self.is_running = False

        # 통합 컴포넌트
        self.data_buffer = RealtimeDataBuffer()
        self.influx_writer = RealtimeInfluxWriter(InfluxConfig())
        self.redis_cache = self._initialize_redis()

        # 통계
        self.ticker_processed = 0
        self.orderbook_processed = 0
        self.start_time = time.time()

        # 성능 모니터링
        self.system_monitor = SystemMonitor()

    def _initialize_redis(self):
        """Redis 초기화"""
        try:
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            client = redis.from_url(redis_url, decode_responses=True)
            client.ping()
            logger.info("✅ Redis 연결 성공")
            return client
        except Exception as e:
            logger.error(f"❌ Redis 연결 실패: {e}")
            return None

    async def start_collection(self, symbols: list = ["KRW-BTC"]):
        """실시간 수집 시작"""
        self.is_running = True

        try:
            # 1. WebSocket 연결
            if not await self._connect_websockets():
                return False

            # 2. 스트림 구독
            await self._subscribe_streams(symbols)

            # 3. 동시 스트림 수신 시작
            logger.info("🎧 업비트 실시간 수집 시작...")

            await asyncio.gather(
                self._collect_ticker_data(),
                self._collect_orderbook_data(),
                return_exceptions=True,
            )

        except Exception as e:
            logger.error(f"❌ 실시간 수집 오류: {e}")
            return False
        finally:
            await self.stop_collection()

    async def stop_collection(self):
        """수집 중지 및 정리"""
        self.is_running = False

        # 남은 버퍼 데이터 플러시
        remaining_batch = self.data_buffer.flush()
        if remaining_batch:
            await self.influx_writer.save_mixed_batch(remaining_batch)
            logger.info(f"🔄 최종 플러시: {len(remaining_batch)}건")

        # WebSocket 연결 종료
        if self.ticker_websocket:
            await self.ticker_websocket.close()
        if self.orderbook_websocket:
            await self.orderbook_websocket.close()

        # InfluxDB 연결 종료
        self.influx_writer.close()

    async def _connect_websockets(self):
        """WebSocket 연결"""
        try:
            # 현재가 WebSocket
            self.ticker_websocket = await websockets.connect(
                self.websocket_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
            )

            # 오더북 WebSocket
            self.orderbook_websocket = await websockets.connect(
                self.websocket_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
            )

            logger.info("✅ 업비트 WebSocket 연결 성공")
            return True

        except Exception as e:
            logger.error(f"❌ WebSocket 연결 실패: {e}")
            return False

    async def _subscribe_streams(self, symbols: list = ["KRW-BTC"]):
        """스트림 구독"""
        try:
            # 현재가 구독
            ticker_message = [
                {"ticket": str(uuid.uuid4())},
                {"type": "ticker", "codes": symbols},
            ]
            await self.ticker_websocket.send(json.dumps(ticker_message))

            # 오더북 구독
            orderbook_message = [
                {"ticket": str(uuid.uuid4())},
                {"type": "orderbook", "codes": symbols},
            ]
            await self.orderbook_websocket.send(json.dumps(orderbook_message))

            logger.info(f"📡 스트림 구독 시작: {symbols}")

        except Exception as e:
            logger.error(f"❌ 스트림 구독 실패: {e}")

    async def _collect_ticker_data(self):
        """현재가 데이터 수집"""
        try:
            async for message in self.ticker_websocket:
                if not self.is_running:
                    break

                try:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    data = json.loads(message)

                    if data.get("type") == "ticker":
                        ticker_data = self._format_ticker_data(data)
                        await self._process_data(ticker_data)

                except Exception as e:
                    logger.error(f"❌ 현재가 메시지 처리 실패: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ 현재가 WebSocket 연결 종료")
        except Exception as e:
            logger.error(f"❌ 현재가 수집 오류: {e}")

    async def _collect_orderbook_data(self):
        """오더북 데이터 수집"""
        try:
            async for message in self.orderbook_websocket:
                if not self.is_running:
                    break

                try:
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    data = json.loads(message)

                    if data.get("type") == "orderbook":
                        orderbook_data = self._format_orderbook_data(data)
                        await self._process_data(orderbook_data)

                except Exception as e:
                    logger.error(f"❌ 오더북 메시지 처리 실패: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ 오더북 WebSocket 연결 종료")
        except Exception as e:
            logger.error(f"❌ 오더북 수집 오류: {e}")

    async def _process_data(self, data: Dict[str, Any]):
        """데이터 처리 (통합)"""
        try:
            # 1. 버퍼에 추가
            batch = self.data_buffer.add_data(data)

            # 2. Redis 캐싱 (현재가만)
            if data.get("data_type") == "ticker" and self.redis_cache:
                cache_key = f"ticker:{data.get('symbol')}:latest"
                self.redis_cache.setex(cache_key, 10, json.dumps(data, default=str))

            # 3. 배치 플러시 시 InfluxDB 저장
            if batch:
                success = await self.influx_writer.save_mixed_batch(batch)
                if success:
                    logger.info(f"💾 InfluxDB 저장 완료: {len(batch)}건")
                else:
                    logger.error(f"❌ InfluxDB 저장 실패: {len(batch)}건")

            # 4. 통계 업데이트
            if data.get("data_type") == "ticker":
                self.ticker_processed += 1
            elif data.get("data_type") == "orderbook":
                self.orderbook_processed += 1

            # 5. 주기적 통계 출력
            total_processed = self.ticker_processed + self.orderbook_processed
            if total_processed % 50 == 0:
                await self._print_collection_statistics()

        except Exception as e:
            logger.error(f"❌ 데이터 처리 실패: {e}")

    def _format_ticker_data(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        """현재가 데이터 포맷팅"""
        formatted = {
            "data_type": "ticker",  # 중요: 데이터 타입 명시
            "symbol": data.get("code", ""),
            "trade_price": data.get("trade_price", 0),
            "change": data.get("change", ""),
            "change_rate": data.get("change_rate", 0),
            "trade_volume": data.get("trade_volume", 0),
            "acc_trade_volume_24h": data.get("acc_trade_volume_24h", 0),
            "acc_trade_price_24h": data.get("acc_trade_price_24h", 0),
            "high_price": data.get("high_price", 0),
            "low_price": data.get("low_price", 0),
            "prev_closing_price": data.get("prev_closing_price", 0),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }
        return formatted

    def _format_orderbook_data(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        """오더북 데이터 포맷팅"""
        orderbook_units = data.get("orderbook_units", [])

        ask_orders = []
        bid_orders = []

        for unit in orderbook_units:
            ask_orders.append(
                {"price": unit.get("ask_price", 0), "size": unit.get("ask_size", 0)}
            )
            bid_orders.append(
                {"price": unit.get("bid_price", 0), "size": unit.get("bid_size", 0)}
            )

        ask_orders.sort(key=lambda x: x["price"])
        bid_orders.sort(key=lambda x: x["price"], reverse=True)

        best_ask = ask_orders[0]["price"] if ask_orders else 0
        best_bid = bid_orders[0]["price"] if bid_orders else 0
        spread = best_ask - best_bid if best_ask and best_bid else 0
        spread_percentage = (spread / best_ask * 100) if best_ask else 0

        total_ask_size = sum(order["size"] for order in ask_orders)
        total_bid_size = sum(order["size"] for order in bid_orders)

        formatted = {
            "data_type": "orderbook",  # 중요: 데이터 타입 명시
            "symbol": data.get("code", ""),
            "best_ask": best_ask,
            "best_bid": best_bid,
            "spread": spread,
            "spread_percentage": spread_percentage,
            "total_ask_size": total_ask_size,
            "total_bid_size": total_bid_size,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }
        return formatted

    async def _print_collection_statistics(self):
        """수집 통계 출력"""
        uptime = time.time() - self.start_time
        total_processed = self.ticker_processed + self.orderbook_processed
        processing_rate = total_processed / uptime if uptime > 0 else 0

        buffer_stats = self.data_buffer.get_stats()
        influx_stats = self.influx_writer.get_stats()
        system_stats = self.system_monitor.get_system_stats()

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ 🚀 업비트 실시간 수집 통계                                       │
├─────────────────────────────────────────────────────────────────┤
│ 총 처리량: {total_processed}건 ({processing_rate:.1f}건/초)      │
│ 현재가: {self.ticker_processed}건 | 오더북: {self.orderbook_processed}건    │
│ 가동시간: {uptime:.0f}초                                       │
├─────────────────────────────────────────────────────────────────┤
│ 🔄 데이터 버퍼:                                                │
│   현재 크기: {buffer_stats['buffer_size']}건                    │
│   총 플러시: {buffer_stats['total_flushed']}건                  │
│   사용률: {buffer_stats['buffer_usage']:.1f}%                  │
├─────────────────────────────────────────────────────────────────┤
│ 💾 InfluxDB:                                                   │
│   총 저장: {influx_stats['total_write_count']}건                │
│   현재가: {influx_stats['ticker_write_count']}건                │
│   오더북: {influx_stats['orderbook_write_count']}건             │
│   성공률: {influx_stats['success_rate']:.1f}%                  │
├─────────────────────────────────────────────────────────────────┤
│ 📊 시스템:                                                     │
│   CPU: {system_stats['cpu_percent']:.1f}%                     │
│   메모리: {system_stats['memory_percent']:.1f}%                │
│   메모리 사용량: {system_stats['memory_usage_mb']:.1f}MB        │
└─────────────────────────────────────────────────────────────────┘
        """
        )


async def test_realtime_connection():
    """실시간 연결 테스트"""
    print("🧪 업비트 실시간 연결 테스트...")

    try:
        # InfluxDB 연결 테스트
        config = InfluxConfig()
        writer = RealtimeInfluxWriter(config)

        # 테스트 데이터 생성
        test_ticker = {
            "data_type": "ticker",
            "symbol": "KRW-BTC",
            "trade_price": 95000000,
            "change_rate": 0.025,
            "buffer_timestamp": time.time(),
        }

        test_orderbook = {
            "data_type": "orderbook",
            "symbol": "KRW-BTC",
            "best_ask": 95001000,
            "best_bid": 95000000,
            "spread": 1000,
            "spread_percentage": 0.001,
            "total_ask_size": 2.5,
            "total_bid_size": 3.2,
            "buffer_timestamp": time.time(),
        }

        # 배치 테스트
        success = await writer.save_mixed_batch([test_ticker, test_orderbook])
        writer.close()

        if success:
            print("✅ 실시간 연결 테스트 성공!")
            return True
        else:
            print("❌ 실시간 연결 테스트 실패!")
            return False

    except Exception as e:
        print(f"❌ 테스트 중 오류: {e}")
        return False


async def main():
    """업비트 실시간 수집기 메인 함수"""
    print("🚀 업비트 실시간 데이터 수집기 시작!")
    print("   WebSocket으로 현재가 + 오더북을 InfluxDB에 저장합니다")
    print("=" * 65)

    # 1. 연결 테스트
    if not await test_realtime_connection():
        print("❌ 연결 테스트 실패. 환경을 확인하세요.")
        print("   docker-compose up -d")
        print("   .env 파일의 InfluxDB 설정 확인")
        return

    # 2. 실시간 수집 시작
    collector = UpbitRealtimeCollector()

    print("\n🎯 수집 목표:")
    print("   • 처리량: 3,600 + 1,800 = 5,400건/시간")
    print("   • 지연시간: < 5초")
    print("   • 메모리: < 200MB")
    print("   • 성공률: > 99%")
    print("\n🎧 실시간 수집을 시작합니다...")

    try:
        await collector.start_collection(["KRW-BTC"])
    except KeyboardInterrupt:
        print("\n🛑 실시간 수집 종료")
        await collector.stop_collection()
    except Exception as e:
        logger.error(f"❌ 수집 중 오류: {e}")


if __name__ == "__main__":
    asyncio.run(main())
