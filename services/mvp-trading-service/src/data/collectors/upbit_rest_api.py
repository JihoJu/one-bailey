#!/usr/bin/env python3
"""
업비트 REST API 투자상태 수집기
Step 2: 보유자산, 주문내역 조회 → 콘솔 출력
"""

import hashlib
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import jwt
import requests

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UpbitRestAPICollector:
    """업비트 REST API 투자상태 수집기"""

    def __init__(self, access_key: str = None, secret_key: str = None):
        self.server_url = "https://api.upbit.com"
        self.access_key = access_key or os.getenv("UPBIT_ACCESS_KEY")
        self.secret_key = secret_key or os.getenv("UPBIT_SECRET_KEY")

        if not self.access_key or not self.secret_key:
            logger.warning("⚠️ 업비트 API 키가 설정되지 않았습니다. 공개 API만 사용 가능합니다.")

    def _get_headers(self, query_params: str = None) -> Dict[str, str]:
        """JWT 토큰 생성 및 헤더 설정"""
        if not self.access_key or not self.secret_key:
            return {}

        payload = {
            "access_key": self.access_key,
            "nonce": str(uuid.uuid4()),
        }

        if query_params:
            # 쿼리 파라미터가 있는 경우 해시 생성
            m = hashlib.sha512()
            m.update(query_params.encode())
            query_hash = m.hexdigest()
            payload["query_hash"] = query_hash
            payload["query_hash_alg"] = "SHA512"

        # JWT 토큰 생성
        jwt_token = jwt.encode(payload, self.secret_key, algorithm="HS256")

        return {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
        }

    async def get_accounts(self) -> Optional[List[Dict[str, Any]]]:
        """보유 자산 조회"""
        if not self.access_key:
            logger.error("❌ API 키가 필요합니다")
            return None

        try:
            headers = self._get_headers()
            response = requests.get(
                f"{self.server_url}/v1/accounts", headers=headers, timeout=10
            )

            if response.status_code == 200:
                accounts = response.json()
                logger.info("✅ 보유 자산 조회 성공")
                return accounts
            else:
                logger.error(f"❌ 보유 자산 조회 실패: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"❌ 보유 자산 조회 중 오류: {e}")
            return None

    async def get_orders(self, state: str = "wait") -> Optional[List[Dict[str, Any]]]:
        """주문 내역 조회

        Args:
            state: 주문 상태 (wait: 미체결, done: 체결완료, cancel: 취소)
        """
        if not self.access_key:
            logger.error("❌ API 키가 필요합니다")
            return None

        try:
            query_params = urlencode({"state": state})
            headers = self._get_headers(query_params)

            response = requests.get(
                f"{self.server_url}/v1/orders?{query_params}",
                headers=headers,
                timeout=10,
            )

            if response.status_code == 200:
                orders = response.json()
                logger.info(f"✅ {state} 주문 내역 조회 성공")
                return orders
            else:
                logger.error(f"❌ 주문 내역 조회 실패: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"❌ 주문 내역 조회 중 오류: {e}")
            return None

    async def get_current_price(self, markets: str = "KRW-BTC") -> Optional[float]:
        """현재가 조회 (공개 API)"""
        try:
            response = requests.get(
                f"{self.server_url}/v1/ticker", params={"markets": markets}, timeout=10
            )

            if response.status_code == 200:
                ticker_data = response.json()
                if ticker_data:
                    current_price = ticker_data[0].get("trade_price", 0)
                    logger.info(f"✅ {markets} 현재가 조회 성공: {current_price:,}원")
                    return current_price
            else:
                logger.error(f"❌ 현재가 조회 실패: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ 현재가 조회 중 오류: {e}")
            return None

    def format_accounts_data(self, accounts: List[Dict[str, Any]]) -> Dict[str, Any]:
        """보유 자산 데이터 포맷팅"""
        formatted_data = {
            "total_krw": 0,
            "total_btc": 0,
            "assets": [],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        for account in accounts:
            currency = account.get("currency", "")
            balance = float(account.get("balance", 0))
            locked = float(account.get("locked", 0))
            avg_buy_price = float(account.get("avg_buy_price", 0))

            asset_info = {
                "currency": currency,
                "balance": balance,
                "locked": locked,
                "total": balance + locked,
                "avg_buy_price": avg_buy_price,
            }

            if currency == "KRW":
                formatted_data["total_krw"] = balance + locked
            elif currency == "BTC":
                formatted_data["total_btc"] = balance + locked

            if balance + locked > 0:  # 보유량이 있는 자산만 표시
                formatted_data["assets"].append(asset_info)

        return formatted_data

    def format_orders_data(self, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """주문 내역 데이터 포맷팅"""
        formatted_orders = []

        for order in orders:
            formatted_order = {
                "uuid": order.get("uuid", ""),
                "market": order.get("market", ""),
                "side": order.get("side", ""),  # bid: 매수, ask: 매도
                "ord_type": order.get("ord_type", ""),  # limit: 지정가, market: 시장가
                "price": float(order.get("price", 0)),
                "volume": float(order.get("volume", 0)),
                "remaining_volume": float(order.get("remaining_volume", 0)),
                "executed_volume": float(order.get("executed_volume", 0)),
                "state": order.get("state", ""),
                "created_at": order.get("created_at", ""),
                "trades_count": order.get("trades_count", 0),
            }
            formatted_orders.append(formatted_order)

        return formatted_orders

    def print_investment_status_console(
        self,
        accounts_data: Dict[str, Any],
        orders_data: List[Dict[str, Any]],
        btc_price: float = 0,
    ):
        """콘솔에 투자상태 출력"""
        # 총 자산가치 계산
        total_value = accounts_data["total_krw"] + (
            accounts_data["total_btc"] * btc_price
        )
        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ 💰 투자 상태 현황                                               │
├─────────────────────────────────────────────────────────────────┤
│ 보유 현금: {accounts_data['total_krw']:,.0f} KRW                │
│ 보유 비트코인: {accounts_data['total_btc']:.8f} BTC             │
│ BTC 평가금액: {accounts_data['total_btc'] * btc_price:,.0f} KRW │
│ 총 자산가치: {total_value:,.0f} KRW │
├─────────────────────────────────────────────────────────────────┤
│ 📊 보유 자산 상세                                               │
├─────────────────────────────────────────────────────────────────┤"""
        )

        for asset in accounts_data["assets"]:
            currency = asset["currency"]
            balance = asset["balance"]
            locked = asset["locked"]
            total = asset["total"]
            avg_price = asset["avg_buy_price"]

            if currency == "KRW":
                print(
                    f"│ {currency}: {total:,.0f} (사용가능: "
                    f"{balance:,.0f}, 주문중: {locked:,.0f})     │"
                )
            else:
                print(
                    f"│ {currency}: {total:.8f} (평균단가: "
                    f"{avg_price:,.0f}원)               │"
                )

        print("├─────────────────────────────────────────────────────────────────┤")
        print("│ 📋 미체결 주문 내역                                             │")
        print("├─────────────────────────────────────────────────────────────────┤")

        if orders_data:
            for order in orders_data[:5]:  # 최근 5개만 표시
                market = order["market"]
                side = "매수" if order["side"] == "bid" else "매도"
                price = order["price"]
                # volume = order["volume"]
                remaining = order["remaining_volume"]
                created_at = order["created_at"][:16]  # 날짜만 표시

                print(
                    f"│ {side} {market} {remaining:.8f} @ {price:,.0f}원 ("
                    f"{created_at}) │"
                )
        else:
            print("│ 미체결 주문이 없습니다.                                         │")

        print(f"│ 업데이트: {accounts_data['timestamp']}                    │")
        print("└─────────────────────────────────────────────────────────────────┘")

    async def collect_investment_status(self):
        """투자상태 전체 수집 및 출력"""
        logger.info("🔄 투자상태 수집 시작...")

        # 1. 보유 자산 조회
        accounts = await self.get_accounts()
        if not accounts:
            logger.error("❌ 보유 자산 조회 실패")
            return

        # 2. 미체결 주문 조회
        pending_orders = await self.get_orders("wait")
        if pending_orders is None:
            pending_orders = []

        # 3. 현재 BTC 가격 조회
        btc_price = await self.get_current_price("KRW-BTC")
        if not btc_price:
            btc_price = 0

        # 4. 데이터 포맷팅
        accounts_data = self.format_accounts_data(accounts)
        orders_data = self.format_orders_data(pending_orders)

        # 5. 콘솔 출력
        self.print_investment_status_console(accounts_data, orders_data, btc_price)

        return {
            "accounts": accounts_data,
            "orders": orders_data,
            "btc_price": btc_price,
        }


async def main():
    """메인 실행 함수"""
    print("🚀 업비트 투자상태 수집기 시작!")
    print("   .env 파일에 API 키가 설정되어 있어야 합니다.")
    print("=" * 65)

    # 환경변수 로드
    from dotenv import load_dotenv

    load_dotenv()

    collector = UpbitRestAPICollector()

    try:
        # 투자상태 수집 및 출력
        result = await collector.collect_investment_status()

        if result:
            print("\n✅ 투자상태 수집 완료!")
        else:
            print("\n❌ 투자상태 수집 실패!")

    except KeyboardInterrupt:
        print("\n🛑 사용자가 프로그램을 종료했습니다")
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
