#!/usr/bin/env python3
"""
업비트 오더북 WebSocket 실시간 수집기
Step 3: 오더북 데이터 수집 → 콘솔 출력
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


class UpbitOrderbookCollector:
    """업비트 오더북 WebSocket 실시간 수집기"""

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

    async def subscribe_orderbook(self, symbols: list = ["KRW-BTC"]):
        """오더북 정보 구독"""
        if not self.websocket:
            logger.error("WebSocket이 연결되지 않았습니다")
            return

        # 업비트 WebSocket 오더북 구독 메시지
        subscribe_message = [
            {
                "ticket": str(uuid.uuid4()),  # 고유 식별자
            },
            {
                "type": "orderbook",  # 오더북 정보
                "codes": symbols,  # 구독할 마켓 코드
            },
        ]

        try:
            await self.websocket.send(json.dumps(subscribe_message))
            logger.info(f"📊 {symbols} 오더북 구독 시작")
        except Exception as e:
            logger.error(f"❌ 구독 메시지 전송 실패: {e}")

    def format_orderbook_data(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        """오더북 데이터 포맷팅"""
        orderbook_units = data.get("orderbook_units", [])

        # 매수/매도 호가 분리
        ask_orders = []  # 매도 호가 (판매자)
        bid_orders = []  # 매수 호가 (구매자)

        for unit in orderbook_units:
            # 매도 호가 (ask)
            ask_orders.append(
                {"price": unit.get("ask_price", 0), "size": unit.get("ask_size", 0)}
            )

            # 매수 호가 (bid)
            bid_orders.append(
                {"price": unit.get("bid_price", 0), "size": unit.get("bid_size", 0)}
            )

        # 매도는 낮은 가격부터 (오름차순), 매수는 높은 가격부터 (내림차순)
        ask_orders.sort(key=lambda x: x["price"])  # 매도: 가격 오름차순
        bid_orders.sort(key=lambda x: x["price"], reverse=True)  # 매수: 가격 내림차순

        # 스프레드 계산 (최저 매도가 - 최고 매수가)
        best_ask = ask_orders[0]["price"] if ask_orders else 0
        best_bid = bid_orders[0]["price"] if bid_orders else 0
        spread = best_ask - best_bid if best_ask and best_bid else 0
        spread_percentage = (spread / best_ask * 100) if best_ask else 0

        # 총 매수/매도 물량 계산
        total_ask_size = sum(order["size"] for order in ask_orders)
        total_bid_size = sum(order["size"] for order in bid_orders)

        return {
            "symbol": data.get("code", ""),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "ask_orders": ask_orders[:10],  # 상위 10개만
            "bid_orders": bid_orders[:10],  # 상위 10개만
            "best_ask": best_ask,
            "best_bid": best_bid,
            "spread": spread,
            "spread_percentage": spread_percentage,
            "total_ask_size": total_ask_size,
            "total_bid_size": total_bid_size,
            "market_pressure": (
                "매수우세"
                if total_bid_size > total_ask_size
                else "매도우세"
                if total_ask_size > total_bid_size
                else "균형"
            ),
        }

    def print_orderbook_console(self, orderbook_data: Dict[str, Any]):
        """콘솔에 오더북 정보 출력"""
        symbol = orderbook_data["symbol"]
        timestamp = orderbook_data["timestamp"]
        spread = orderbook_data["spread"]
        spread_pct = orderbook_data["spread_percentage"]
        market_pressure = orderbook_data["market_pressure"]

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ 📊 {symbol} 실시간 오더북                                        │
├─────────────────────────────────────────────────────────────────┤
│ 스프레드: {spread:,.0f}원 ({spread_pct:.3f}%)                     │
│ 시장압력: {market_pressure}                                      │
│ 시간: {timestamp}                                        │
├─────────────────────────────────────────────────────────────────┤
│          매도 호가 (Ask Orders)          │        수량           │
├─────────────────────────────────────────────────────────────────┤"""
        )

        # 매도 호가 출력 (높은 가격부터 - 역순으로 표시)
        for i, ask in enumerate(reversed(orderbook_data["ask_orders"][:5])):  # 상위 5개만
            price = ask["price"]
            size = ask["size"]
            print(f"│ 🔴 {price:>12,}원                    │ {size:>8.4f} BTC      │")

        print("├─────────────────────────────────────────────────────────────────┤")
        print(f"│                    현재 스프레드: {spread:,}원                     │")
        print("├─────────────────────────────────────────────────────────────────┤")
        print("│          매수 호가 (Bid Orders)          │        수량           │")
        print("├─────────────────────────────────────────────────────────────────┤")

        # 매수 호가 출력 (높은 가격부터)
        for i, bid in enumerate(orderbook_data["bid_orders"][:5]):  # 상위 5개만
            price = bid["price"]
            size = bid["size"]
            print(f"│ 🔵 {price:>12,}원                    │ {size:>8.4f} BTC      │")

        print("├─────────────────────────────────────────────────────────────────┤")

        # 시장 요약 정보
        total_ask = orderbook_data["total_ask_size"]
        total_bid = orderbook_data["total_bid_size"]

        # 물량 비율 계산
        buy_ratio = total_bid / (total_ask + total_bid) * 100
        sell_ratio = total_ask / (total_ask + total_bid) * 100

        print(f"│ 총 매도물량: {total_ask:>8.4f} BTC                              │")
        print(f"│ 총 매수물량: {total_bid:>8.4f} BTC                              │")
        print(f"│ 물량 비율: 매수 {buy_ratio:.1f}% | 매도 {sell_ratio:.1f}%       │")
        print("└─────────────────────────────────────────────────────────────────┘")

    async def listen_orderbook(self):
        """실시간 오더북 데이터 수신 및 처리"""
        if not self.websocket:
            logger.error("WebSocket이 연결되지 않았습니다")
            return

        self.is_running = True
        logger.info("📊 실시간 오더북 수신 시작...")

        try:
            async for message in self.websocket:
                if not self.is_running:
                    break

                try:
                    # 업비트는 바이너리 데이터로 전송하므로 decode 필요
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    data = json.loads(message)

                    # 오더북 데이터 처리
                    if data.get("type") == "orderbook":
                        orderbook_data = self.format_orderbook_data(data)
                        self.print_orderbook_console(orderbook_data)

                        # 콘솔 화면 갱신을 위한 잠깐의 딜레이
                        await asyncio.sleep(1)

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
                logger.info("🔄 업비트 오더북 WebSocket 연결 시도...")

                if await self.connect():
                    await self.subscribe_orderbook(symbols)
                    await self.listen_orderbook()

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


class UpbitOrderbookAnalyzer:
    """오더북 분석 도구"""

    @staticmethod
    def analyze_market_sentiment(orderbook_data: Dict[str, Any]) -> Dict[str, Any]:
        """시장 심리 분석"""
        ask_orders = orderbook_data["ask_orders"]
        bid_orders = orderbook_data["bid_orders"]

        # 상위 5개 호가의 평균 규모 비교
        top5_ask_avg = sum(order["size"] for order in ask_orders[:5]) / 5
        top5_bid_avg = sum(order["size"] for order in bid_orders[:5]) / 5

        # 가격 압력 분석
        spread_pct = orderbook_data["spread_percentage"]

        # 유동성 분석
        liquidity_score = (
            "높음" if spread_pct < 0.1 else "보통" if spread_pct < 0.5 else "낮음"
        )

        # 시장 강도 분석
        if top5_bid_avg > top5_ask_avg * 1.2:
            market_strength = "강한 매수세"
        elif top5_ask_avg > top5_bid_avg * 1.2:
            market_strength = "강한 매도세"
        else:
            market_strength = "균형잡힌 시장"

        return {
            "liquidity": liquidity_score,
            "market_strength": market_strength,
            "spread_analysis": "좁음" if spread_pct < 0.1 else "넓음",
            "top5_bid_avg": top5_bid_avg,
            "top5_ask_avg": top5_ask_avg,
        }


async def main():
    """메인 실행 함수"""
    print("🚀 업비트 BTC 오더북 실시간 수집기 시작!")
    print("   실시간 매수/매도 호가를 모니터링합니다")
    print("   종료하려면 Ctrl+C를 누르세요")
    print("=" * 65)

    collector = UpbitOrderbookCollector()
    await collector.run_forever(["KRW-BTC"])


if __name__ == "__main__":
    asyncio.run(main())
