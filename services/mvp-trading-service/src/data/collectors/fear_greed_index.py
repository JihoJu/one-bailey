#!/usr/bin/env python3
"""
공포탐욕지수 (Fear & Greed Index) 수집기
Step 5: Alternative.me API를 통한 시장 심리 분석 → 콘솔 출력
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FearGreedIndexCollector:
    """공포탐욕지수 수집기"""

    def __init__(self):
        self.api_url = "https://api.alternative.me/fng/"

    def get_current_index(self) -> Optional[Dict[str, Any]]:
        """현재 공포탐욕지수 조회"""
        try:
            response = requests.get(f"{self.api_url}?limit=1", timeout=10)

            if response.status_code == 200:
                data = response.json()

                if data.get("data") and len(data["data"]) > 0:
                    current_data = data["data"][0]
                    logger.info("✅ 현재 공포탐욕지수 조회 성공")
                    return self.format_index_data(current_data)
                else:
                    logger.error("❌ 공포탐욕지수 데이터가 비어있습니다")
                    return None
            else:
                logger.error(f"❌ API 호출 실패: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"❌ 공포탐욕지수 조회 중 오류: {e}")
            return None

    def get_historical_index(self, days: int = 7) -> Optional[List[Dict[str, Any]]]:
        """과거 공포탐욕지수 조회 (최근 N일)"""
        try:
            response = requests.get(f"{self.api_url}?limit={days}", timeout=10)

            if response.status_code == 200:
                data = response.json()

                if data.get("data"):
                    historical_data = []
                    for item in data["data"]:
                        formatted_data = self.format_index_data(item)
                        if formatted_data:
                            historical_data.append(formatted_data)

                    logger.info(f"✅ 과거 {len(historical_data)}일 공포탐욕지수 조회 성공")
                    return historical_data
                else:
                    logger.error("❌ 과거 공포탐욕지수 데이터가 비어있습니다")
                    return None
            else:
                logger.error(f"❌ 과거 데이터 API 호출 실패: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ 과거 공포탐욕지수 조회 중 오류: {e}")
            return None

    def format_index_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """공포탐욕지수 데이터 포맷팅"""
        try:
            value = int(raw_data.get("value", 0))
            value_classification = raw_data.get("value_classification", "Unknown")
            timestamp = int(raw_data.get("timestamp", 0))

            # timestamp를 datetime으로 변환
            date_time = datetime.fromtimestamp(timestamp)

            return {
                "value": value,
                "classification": value_classification,
                "timestamp": timestamp,
                "date": date_time.strftime("%Y-%m-%d"),
                "datetime": date_time.strftime("%Y-%m-%d %H:%M:%S"),
                "analysis": self.analyze_index_value(value),
                "emoji": self.get_index_emoji(value),
                "color": self.get_index_color(value),
            }
        except Exception as e:
            logger.error(f"❌ 데이터 포맷팅 실패: {e}")
            return None

    def analyze_index_value(self, value: int) -> Dict[str, str]:
        """공포탐욕지수 값 분석"""
        if value <= 20:
            return {
                "level": "극도의 공포",
                "signal": "강력한 매수 신호",
                "description": "시장이 극도로 공포에 빠진 상태. 역발상 투자 기회",
                "action": "적극적 매수 검토",
            }
        elif value <= 40:
            return {
                "level": "공포",
                "signal": "매수 신호",
                "description": "시장 심리가 부정적. 저점 매수 기회 가능성",
                "action": "점진적 매수 고려",
            }
        elif value <= 60:
            return {
                "level": "중립",
                "signal": "관망",
                "description": "시장 심리가 균형잡힌 상태",
                "action": "추세 관찰 후 판단",
            }
        elif value <= 80:
            return {
                "level": "탐욕",
                "signal": "매도 검토",
                "description": "시장이 과열되기 시작. 주의 필요",
                "action": "일부 매도 고려",
            }
        else:
            return {
                "level": "극도의 탐욕",
                "signal": "강력한 매도 신호",
                "description": "시장이 극도로 과열된 상태. 고점 가능성",
                "action": "적극적 매도 검토",
            }

    def get_index_emoji(self, value: int) -> str:
        """지수에 따른 이모지 반환"""
        if value <= 20:
            return "😱"  # 극도의 공포
        elif value <= 40:
            return "😰"  # 공포
        elif value <= 60:
            return "😐"  # 중립
        elif value <= 80:
            return "😍"  # 탐욕
        else:
            return "🤑"  # 극도의 탐욕

    def get_index_color(self, value: int) -> str:
        """지수에 따른 색상 반환"""
        if value <= 20:
            return "🔴"  # 빨간색 (극도의 공포)
        elif value <= 40:
            return "🟠"  # 주황색 (공포)
        elif value <= 60:
            return "🟡"  # 노란색 (중립)
        elif value <= 80:
            return "🟢"  # 초록색 (탐욕)
        else:
            return "🟣"  # 보라색 (극도의 탐욕)

    def calculate_trend_analysis(
        self, historical_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """트렌드 분석 (최근 7일)"""
        if not historical_data or len(historical_data) < 2:
            return {"trend": "데이터 부족", "change": 0}

        # 최신순으로 정렬 (timestamp 기준)
        sorted_data = sorted(
            historical_data, key=lambda x: x["timestamp"], reverse=True
        )

        current_value = sorted_data[0]["value"]
        previous_value = (
            sorted_data[1]["value"] if len(sorted_data) > 1 else current_value
        )
        week_ago_value = (
            sorted_data[-1]["value"] if len(sorted_data) >= 7 else previous_value
        )

        # 변화량 계산
        daily_change = current_value - previous_value
        weekly_change = current_value - week_ago_value

        # 트렌드 분석
        if weekly_change > 10:
            trend = "급속한 탐욕 증가"
        elif weekly_change > 5:
            trend = "탐욕 증가"
        elif weekly_change > -5:
            trend = "횡보"
        elif weekly_change > -10:
            trend = "공포 증가"
        else:
            trend = "급속한 공포 증가"

        # 변동성 계산
        values = [item["value"] for item in sorted_data]
        volatility = max(values) - min(values)

        return {
            "trend": trend,
            "daily_change": daily_change,
            "weekly_change": weekly_change,
            "volatility": volatility,
            "stability": "안정적" if volatility < 20 else "불안정",
        }

    def print_fear_greed_console(
        self, current_data: Dict[str, Any], historical_data: List[Dict[str, Any]] = None
    ):
        """콘솔에 공포탐욕지수 출력"""
        if not current_data:
            print("❌ 공포탐욕지수 데이터를 표시할 수 없습니다")
            return

        value = current_data["value"]
        analysis = current_data["analysis"]
        emoji = current_data["emoji"]
        color = current_data["color"]
        datetime_str = current_data["datetime"]

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ {emoji} 암호화폐 공포탐욕지수 (Fear & Greed Index)                   │
├─────────────────────────────────────────────────────────────────┤
│ 현재 지수: {color} {value}/100 - {analysis['level']}                    │
│ 투자 신호: {analysis['signal']}                                  │
│ 권장 행동: {analysis['action']}                                  │
│ 업데이트: {datetime_str}                                  │
├─────────────────────────────────────────────────────────────────┤
│ 📊 지수 해석                                                    │
├─────────────────────────────────────────────────────────────────┤
│ {analysis['description']}                                        │"""
        )

        # 히스토리 데이터가 있으면 트렌드 분석 표시
        if historical_data and len(historical_data) > 1:
            trend_analysis = self.calculate_trend_analysis(historical_data)
            # 변동성
            volatility = trend_analysis["volatility"]
            stability = trend_analysis["stability"]
            print("├─────────────────────────────────────────────────────────────────┤")
            print("│ 📈 트렌드 분석 (최근 7일)                                       │")
            print("├─────────────────────────────────────────────────────────────────┤")
            print(f"│ 추세: {trend_analysis['trend']}                                 │")
            print(f"│ 일간 변화: {trend_analysis['daily_change']:+d}                   │")
            print(f"│ 주간 변화: {trend_analysis['weekly_change']:+d}                  │")
            print(f"│ 변동성: {volatility} ({stability})│")

            print("├─────────────────────────────────────────────────────────────────┤")
            print("│ 📅 최근 7일간 변화                                             │")
            print("├─────────────────────────────────────────────────────────────────┤")

            # 최근 7일 데이터 표시 (최신순)
            for i, data in enumerate(historical_data[:7]):
                date = data["date"]
                val = data["value"]
                classification = data["classification"]
                emoji_hist = data["emoji"]

                if i == 0:
                    print(f"│ {date}: {val:2d} {emoji_hist} {classification} (오늘)   │")
                else:
                    print(
                        f"│ {date}: {val:2d} {emoji_hist} {classification}          │"
                    )

        print("├─────────────────────────────────────────────────────────────────┤")
        print("│ 🎯 투자 전략 가이드                                             │")
        print("├─────────────────────────────────────────────────────────────────┤")

        # 투자 전략 제안
        if value <= 20:
            print("│ • 극도의 공포 구간: 역발상 투자 최적 타이밍                     │")
            print("│ • DCA(분할매수) 전략으로 적극적 포지션 확대                     │")
            print("│ • 장기 관점에서 우량 자산 매수 기회                             │")
        elif value <= 40:
            print("│ • 공포 구간: 점진적 매수 전략 고려                              │")
            print("│ • 시장 저점 근처일 가능성, 단계별 진입                          │")
            print("│ • 리스크 관리하며 포지션 확대                                   │")
        elif value <= 60:
            print("│ • 중립 구간: 신중한 관망 및 추세 관찰                           │")
            print("│ • 기술적 분석과 함께 종합적 판단                                │")
            print("│ • 급격한 변화 시 대응 준비                                      │")
        elif value <= 80:
            print("│ • 탐욕 구간: 일부 수익 실현 고려                                │")
            print("│ • 과열 신호 감지 시 포지션 축소                                 │")
            print("│ • 추가 상승보다는 안전성 우선                                   │")
        else:
            print("│ • 극도의 탐욕: 적극적 수익 실현 타이밍                          │")
            print("│ • 고점 가능성 높음, 단계적 매도                                 │")
            print("│ • 다음 매수 기회 대비 현금 확보                                 │")

        print("└─────────────────────────────────────────────────────────────────┘")

    def collect_and_display(self):
        """공포탐욕지수 수집 및 표시"""
        logger.info("🔄 공포탐욕지수 수집 시작...")

        # 현재 지수 조회
        current_data = self.get_current_index()
        if not current_data:
            logger.error("❌ 현재 공포탐욕지수 조회 실패")
            return None

        # 과거 7일간 데이터 조회
        historical_data = self.get_historical_index(7)

        # 콘솔 출력
        self.print_fear_greed_console(current_data, historical_data)

        return {
            "current": current_data,
            "historical": historical_data,
            "collection_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


class MarketSentimentAnalyzer:
    """시장 심리 종합 분석기"""

    def __init__(self):
        self.fng_collector = FearGreedIndexCollector()

    def analyze_market_sentiment(self) -> Dict[str, Any]:
        """시장 심리 종합 분석"""
        # 공포탐욕지수 데이터 수집
        fng_data = self.fng_collector.collect_and_display()

        if not fng_data:
            return None

        current_index = fng_data["current"]["value"]

        # 시장 심리 종합 평가
        if current_index <= 25:
            market_sentiment = {
                "overall": "극도로 부정적",
                "opportunity": "매우 높음",
                "risk": "낮음",
                "strategy": "공격적 매수",
            }
        elif current_index <= 45:
            market_sentiment = {
                "overall": "부정적",
                "opportunity": "높음",
                "risk": "보통",
                "strategy": "단계적 매수",
            }
        elif current_index <= 55:
            market_sentiment = {
                "overall": "중립",
                "opportunity": "보통",
                "risk": "보통",
                "strategy": "관망",
            }
        elif current_index <= 75:
            market_sentiment = {
                "overall": "긍정적",
                "opportunity": "낮음",
                "risk": "높음",
                "strategy": "부분 매도",
            }
        else:
            market_sentiment = {
                "overall": "극도로 긍정적",
                "opportunity": "매우 낮음",
                "risk": "매우 높음",
                "strategy": "적극적 매도",
            }

        return {
            "fear_greed_data": fng_data,
            "market_sentiment": market_sentiment,
            "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


async def main():
    """메인 실행 함수"""
    print("🚀 암호화폐 공포탐욕지수 수집기 시작!")
    print("   Alternative.me API에서 시장 심리를 분석합니다")
    print("=" * 65)

    analyzer = MarketSentimentAnalyzer()

    try:
        # 시장 심리 종합 분석
        result = analyzer.analyze_market_sentiment()

        if result:
            print("\n✅ 공포탐욕지수 분석 완료!")

            # 추가 분석 결과 출력
            sentiment = result["market_sentiment"]
            print(
                f"""
┌─────────────────────────────────────────────────────────────────┐
│ 🎯 시장 심리 종합 평가                                           │
├─────────────────────────────────────────────────────────────────┤
│ 전반적 심리: {sentiment['overall']}                              │
│ 기회 수준: {sentiment['opportunity']}                            │
│ 리스크 수준: {sentiment['risk']}                                 │
│ 권장 전략: {sentiment['strategy']}                               │
│ 분석 시간: {result['analysis_time']}                     │
└─────────────────────────────────────────────────────────────────┘"""
            )
        else:
            print("\n❌ 공포탐욕지수 분석 실패!")

    except KeyboardInterrupt:
        print("\n🛑 사용자가 프로그램을 종료했습니다")
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
