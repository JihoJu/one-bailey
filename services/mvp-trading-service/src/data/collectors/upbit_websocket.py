#!/usr/bin/env python3
"""
업비트 WebSocket BTC 현재가 실시간 수집기
Step 1: 실시간 현재가 수집 → 콘솔 출력
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict

import websockets

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UpbitWebSocketCollector:
    """업비트 WebSocket 실시간 데이터 수집기"""

    def __init__(self):
        self.websocket_url = "wss://api.upbit.com/websocket/v1"
        self.websocket = None
        self.is_running = False

    async def connect(self):
        """WebSocket 연결"""
        try:
            self.websocket = await websockets.connect(
                self.websocket_url,
                ping_interval=30,  # 30초마다 ping
                ping_timeout=10,  # 10초 timeout
                close_timeout=10,
            )
            logger.info("✅ 업비트 WebSocket 연결 성공")
            return True
        except Exception as e:
            logger.error(f"❌ WebSocket 연결 실패: {e}")
            return False

    async def subscribe_ticker(self, symbols: list = ["KRW-BTC"]):
        """현재가 정보 구독"""
        if not self.websocket:
            logger.error("WebSocket이 연결되지 않았습니다")
            return

        # 업비트 WebSocket 구독 메시지 형식
        subscribe_message = [
            {
                "ticket": str(uuid.uuid4()),  # 고유 식별자
            },
            {
                "type": "ticker",  # 현재가 정보
                "codes": symbols,  # 구독할 마켓 코드
            },
        ]

        try:
            await self.websocket.send(json.dumps(subscribe_message))
            logger.info(f"📡 {symbols} 현재가 구독 시작")
        except Exception as e:
            logger.error(f"❌ 구독 메시지 전송 실패: {e}")

    def format_ticker_data(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        """현재가 데이터 포맷팅"""
        return {
            "symbol": data.get("code", ""),  # 마켓 코드 (KRW-BTC)
            "trade_price": data.get("trade_price", 0),  # 현재가
            "change": data.get("change", ""),  # 전일 대비 (RISE/FALL/EVEN)
            "change_price": data.get("change_price", 0),  # 전일 대비 가격
            "change_rate": data.get("change_rate", 0),  # 전일 대비 변화율
            "signed_change_price": data.get("signed_change_price", 0),  # 부호가 있는 변화가격
            "signed_change_rate": data.get("signed_change_rate", 0),  # 부호가 있는 변화율
            "trade_volume": data.get("trade_volume", 0),  # 거래량
            "acc_trade_volume_24h": data.get("acc_trade_volume_24h", 0),  # 24시간 누적 거래량
            "acc_trade_price_24h": data.get("acc_trade_price_24h", 0),  # 24시간 누적 거래대금
            "high_price": data.get("high_price", 0),  # 고가
            "low_price": data.get("low_price", 0),  # 저가
            "prev_closing_price": data.get("prev_closing_price", 0),  # 전일 종가
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        }

    def print_ticker_console(self, ticker_data: Dict[str, Any]):
        """콘솔에 현재가 정보 출력"""
        symbol = ticker_data["symbol"]
        price = ticker_data["trade_price"]
        change = ticker_data["change"]
        change_rate = ticker_data["signed_change_rate"] * 100  # 퍼센트로 변환
        volume_24h = ticker_data["acc_trade_volume_24h"]
        high = ticker_data["high_price"]
        low = ticker_data["low_price"]
        timestamp = ticker_data["timestamp"]

        # 변화 상태에 따른 이모지
        if change == "RISE":
            change_emoji = "🔴"
            change_color = "+"
        elif change == "FALL":
            change_emoji = "🔵"
            change_color = ""
        else:
            change_emoji = "⚪"
            change_color = ""

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ {change_emoji} {symbol} 실시간 현재가                           │
├─────────────────────────────────────────────────────────────────┤
│ 현재가: {price:,}원                                             │
│ 변화율: {change_color}{change_rate:+.2f}%                      │
│ 고가: {high:,}원 | 저가: {low:,}원                            │
│ 24h 거래량: {volume_24h:.2f} BTC                               │
│ 시간: {timestamp}                                               │
└─────────────────────────────────────────────────────────────────┘
        """
        )

    async def listen_ticker(self):
        """실시간 현재가 데이터 수신 및 처리"""
        if not self.websocket:
            logger.error("WebSocket이 연결되지 않았습니다")
            return

        self.is_running = True
        logger.info("🎧 실시간 현재가 수신 시작...")

        try:
            async for message in self.websocket:
                if not self.is_running:
                    break

                try:
                    # 업비트는 바이너리 데이터로 전송하므로 decode 필요
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    data = json.loads(message)

                    # 현재가 데이터 처리
                    if data.get("type") == "ticker":
                        ticker_data = self.format_ticker_data(data)
                        self.print_ticker_console(ticker_data)

                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ JSON 파싱 실패: {e}")
                except Exception as e:
                    logger.error(f"❌ 메시지 처리 실패: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("⚠️ WebSocket 연결이 종료되었습니다")
        except Exception as e:
            logger.error(f"❌ 데이터 수신 중 오류: {e}")
        finally:
            self.is_running = False

    async def disconnect(self):
        """WebSocket 연결 종료"""
        self.is_running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("🔌 WebSocket 연결 종료")

    async def run_forever(self, symbols: list = ["KRW-BTC"]):
        """무한 실행 (재연결 포함)"""
        while True:
            try:
                logger.info("🔄 업비트 WebSocket 연결 시도...")

                if await self.connect():
                    await self.subscribe_ticker(symbols)
                    await self.listen_ticker()

                logger.warning("⚠️ 연결이 끊어졌습니다. 5초 후 재연결 시도...")
                await asyncio.sleep(5)

            except KeyboardInterrupt:
                logger.info("🛑 사용자가 프로그램을 종료했습니다")
                break
            except Exception as e:
                logger.error(f"❌ 예상치 못한 오류: {e}")
                await asyncio.sleep(5)
            finally:
                await self.disconnect()


async def main():
    """메인 실행 함수"""
    print("🚀 업비트 BTC 실시간 현재가 수집기 시작!")
    print("   종료하려면 Ctrl+C를 누르세요")
    print("=" * 65)

    collector = UpbitWebSocketCollector()
    await collector.run_forever(["KRW-BTC"])


if __name__ == "__main__":
    asyncio.run(main())
