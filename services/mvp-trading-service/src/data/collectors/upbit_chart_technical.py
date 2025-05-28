#!/usr/bin/env python3
"""
업비트 차트 데이터 및 보조지표 수집기
Step 4: 차트 데이터 수집 + TA-Lib 보조지표 계산 → 콘솔 출력
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests

# TA-Lib 임포트 (설치 필요: poetry add TA-Lib)
try:
    import talib

    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("⚠️ TA-Lib이 설치되지 않았습니다. 보조지표 계산이 제한됩니다.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UpbitChartCollector:
    """업비트 차트 데이터 수집기"""

    def __init__(self):
        self.server_url = "https://api.upbit.com/v1"

    def get_candles(
        self, market: str = "KRW-BTC", interval: str = "days", count: int = 30
    ) -> Optional[pd.DataFrame]:
        """캔들 데이터 조회

        Args:
            market: 마켓 코드 (KRW-BTC)
            interval: 캔들 타입 (days: 일봉, minutes/1: 1분봉, minutes/60: 1시간봉)
            count: 조회할 캔들 개수 (최대 200)
        """
        try:
            # API 엔드포인트 결정
            if interval == "days":
                endpoint = f"{self.server_url}/candles/days"
            elif interval.startswith("minutes"):
                unit = interval.split("/")[1] if "/" in interval else "1"
                endpoint = f"{self.server_url}/candles/minutes/{unit}"
            else:
                logger.error(f"❌ 지원하지 않는 interval: {interval}")
                return None

            # API 호출
            params = {"market": market, "count": min(count, 200)}  # API 제한

            response = requests.get(endpoint, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # DataFrame으로 변환
                df = pd.DataFrame(data)

                if df.empty:
                    logger.warning("⚠️ 빈 데이터가 반환되었습니다")
                    return None

                # 컬럼명 정리 및 타입 변환
                df = df.rename(
                    columns={
                        "candle_date_time_utc": "datetime",
                        "opening_price": "open",
                        "high_price": "high",
                        "low_price": "low",
                        "trade_price": "close",
                        "candle_acc_trade_volume": "volume",
                    }
                )

                # 필요한 컬럼만 선택
                df = df[["datetime", "open", "high", "low", "close", "volume"]]

                # 데이터 타입 변환
                df["datetime"] = pd.to_datetime(df["datetime"])
                for col in ["open", "high", "low", "close", "volume"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                # 시간순 정렬 (과거 → 현재)
                df = df.sort_values("datetime").reset_index(drop=True)

                logger.info(f"✅ {market} {interval} 캔들 {len(df)}개 조회 성공")
                return df

            else:
                logger.error(
                    f"❌ 캔들 데이터 조회 실패: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"❌ 캔들 데이터 조회 중 오류: {e}")
            return None


class TechnicalIndicatorCalculator:
    """TA-Lib 기반 보조지표 계산기"""

    def __init__(self):
        self.talib_available = TALIB_AVAILABLE

    def calculate_sma(
        self, df: pd.DataFrame, periods: List[int] = [5, 20, 60, 120]
    ) -> pd.DataFrame:
        """단순이동평균선 계산"""
        result_df = df.copy()

        if self.talib_available:
            for period in periods:
                if len(df) >= period:
                    result_df[f"sma_{period}"] = talib.SMA(
                        df["close"].values, timeperiod=period
                    )
                else:
                    result_df[f"sma_{period}"] = np.nan
        else:
            # TA-Lib 없이 pandas로 계산
            for period in periods:
                result_df[f"sma_{period}"] = df["close"].rolling(window=period).mean()

        return result_df

    def calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """RSI (상대강도지수) 계산"""
        result_df = df.copy()

        if self.talib_available and len(df) >= period:
            result_df["rsi"] = talib.RSI(df["close"].values, timeperiod=period)
        else:
            # TA-Lib 없이 직접 계산
            delta = df["close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            result_df["rsi"] = 100 - (100 / (1 + rs))

        return result_df

    def calculate_macd(
        self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9
    ) -> pd.DataFrame:
        """MACD 계산"""
        result_df = df.copy()

        if self.talib_available and len(df) >= slow:
            macd_line, macd_signal, macd_histogram = talib.MACD(
                df["close"].values,
                fastperiod=fast,
                slowperiod=slow,
                signalperiod=signal,
            )
            result_df["macd"] = macd_line
            result_df["macd_signal"] = macd_signal
            result_df["macd_histogram"] = macd_histogram
        else:
            # TA-Lib 없이 직접 계산
            ema_fast = df["close"].ewm(span=fast).mean()
            ema_slow = df["close"].ewm(span=slow).mean()
            result_df["macd"] = ema_fast - ema_slow
            result_df["macd_signal"] = result_df["macd"].ewm(span=signal).mean()
            result_df["macd_histogram"] = result_df["macd"] - result_df["macd_signal"]

        return result_df

    def calculate_bollinger_bands(
        self, df: pd.DataFrame, period: int = 20, std_dev: int = 2
    ) -> pd.DataFrame:
        """볼린저 밴드 계산"""
        result_df = df.copy()

        if self.talib_available and len(df) >= period:
            upper, middle, lower = talib.BBANDS(
                df["close"].values, timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev
            )
            result_df["bb_upper"] = upper
            result_df["bb_middle"] = middle
            result_df["bb_lower"] = lower
        else:
            # TA-Lib 없이 직접 계산
            sma = df["close"].rolling(window=period).mean()
            std = df["close"].rolling(window=period).std()
            result_df["bb_upper"] = sma + (std * std_dev)
            result_df["bb_middle"] = sma
            result_df["bb_lower"] = sma - (std * std_dev)

        return result_df

    def calculate_atr(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """ATR (Average True Range) 계산"""
        result_df = df.copy()

        if self.talib_available and len(df) >= period:
            result_df["atr"] = talib.ATR(
                df["high"].values,
                df["low"].values,
                df["close"].values,
                timeperiod=period,
            )
        else:
            # TA-Lib 없이 직접 계산
            high_low = df["high"] - df["low"]
            high_close = np.abs(df["high"] - df["close"].shift())
            low_close = np.abs(df["low"] - df["close"].shift())
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(
                axis=1
            )
            result_df["atr"] = true_range.rolling(window=period).mean()

        return result_df

    def calculate_all_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """모든 보조지표 계산"""
        if df is None or df.empty:
            return df

        logger.info("🔄 보조지표 계산 시작...")

        # 각 지표 계산
        df = self.calculate_sma(df, [5, 20, 60, 120])
        df = self.calculate_rsi(df)
        df = self.calculate_macd(df)
        df = self.calculate_bollinger_bands(df)
        df = self.calculate_atr(df)

        logger.info("✅ 모든 보조지표 계산 완료")
        return df


class ChartDataAnalyzer:
    """차트 데이터 분석기"""

    def __init__(self):
        self.chart_collector = UpbitChartCollector()
        self.indicator_calculator = TechnicalIndicatorCalculator()

    def collect_and_analyze(self, market: str = "KRW-BTC") -> Dict[str, Any]:
        """차트 데이터 수집 및 보조지표 분석"""
        logger.info(f"🔄 {market} 차트 데이터 분석 시작...")

        # 1. 30일 일봉 데이터 수집
        daily_data = self.chart_collector.get_candles(market, "days", 30)
        if daily_data is None:
            logger.error("❌ 일봉 데이터 수집 실패")
            return None

        # 2. 24시간 시간봉 데이터 수집
        hourly_data = self.chart_collector.get_candles(market, "minutes/60", 24)
        if hourly_data is None:
            logger.error("❌ 시간봉 데이터 수집 실패")
            return None

        # 3. 보조지표 계산
        daily_with_indicators = self.indicator_calculator.calculate_all_indicators(
            daily_data
        )
        hourly_with_indicators = self.indicator_calculator.calculate_all_indicators(
            hourly_data
        )

        # 4. 최신 데이터 추출
        latest_daily = (
            daily_with_indicators.iloc[-1] if not daily_with_indicators.empty else None
        )
        latest_hourly = (
            hourly_with_indicators.iloc[-1]
            if not hourly_with_indicators.empty
            else None
        )

        return {
            "market": market,
            "daily_data": daily_with_indicators,
            "hourly_data": hourly_with_indicators,
            "latest_daily": latest_daily,
            "latest_hourly": latest_hourly,
            "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def print_analysis_console(self, analysis_result: Dict[str, Any]):
        """콘솔에 분석 결과 출력"""
        if not analysis_result:
            return

        market = analysis_result["market"]
        latest_daily = analysis_result["latest_daily"]
        latest_hourly = analysis_result["latest_hourly"]
        analysis_time = analysis_result["analysis_time"]

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ 📈 {market} 차트 분석 및 보조지표                                │
├─────────────────────────────────────────────────────────────────┤
│ 분석시간: {analysis_time}                               │
├─────────────────────────────────────────────────────────────────┤
│ 📊 일봉 기준 (30일 데이터)                                      │
├─────────────────────────────────────────────────────────────────┤"""
        )

        if latest_daily is not None:
            print(
                f"│ 현재가: {latest_daily['close']:,.0f}원                              │"
            )
            print(
                f"│ 고가: {latest_daily['high']:,.0f}원 | 저가: {latest_daily['low']:,.0f}원│"
            )
            print(
                f"│ 거래량: {latest_daily['volume']:.2f} BTC                            │"
            )
            print("├─────────────────────────────────────────────────────────────────┤")
            print("│ 🔄 이동평균선 (SMA)                                             │")
            print("├─────────────────────────────────────────────────────────────────┤")

            current_price = latest_daily["close"]
            sma_5 = latest_daily.get("sma_5", np.nan)
            sma_20 = latest_daily.get("sma_20", np.nan)
            sma_60 = latest_daily.get("sma_60", np.nan)
            sma_120 = latest_daily.get("sma_120", np.nan)

            if not pd.isna(sma_5):
                trend_5 = "↗️" if current_price > sma_5 else "↘️"
                print(f"│ SMA 5일: {sma_5:,.0f}원 {trend_5}                           │")
            if not pd.isna(sma_20):
                trend_20 = "↗️" if current_price > sma_20 else "↘️"
                print(f"│ SMA 20일: {sma_20:,.0f}원 {trend_20}                        │")
            if not pd.isna(sma_60):
                trend_60 = "↗️" if current_price > sma_60 else "↘️"
                print(f"│ SMA 60일: {sma_60:,.0f}원 {trend_60}                        │")
            if not pd.isna(sma_120):
                trend_120 = "↗️" if current_price > sma_120 else "↘️"
                print(f"│ SMA 120일: {sma_120:,.0f}원 {trend_120}                     │")

            print("├─────────────────────────────────────────────────────────────────┤")
            print("│ 📊 보조지표                                                     │")
            print("├─────────────────────────────────────────────────────────────────┤")

            # RSI
            rsi = latest_daily.get("rsi", np.nan)
            if not pd.isna(rsi):
                if rsi >= 70:
                    rsi_signal = "과매수 🔴"
                elif rsi <= 30:
                    rsi_signal = "과매도 🔵"
                else:
                    rsi_signal = "중립 ⚪"
                print(
                    f"│ RSI (14): {rsi:.1f} - {rsi_signal}                           │"
                )

            # MACD
            macd = latest_daily.get("macd", np.nan)
            macd_signal = latest_daily.get("macd_signal", np.nan)
            # macd_histogram = latest_daily.get("macd_histogram", np.nan)

            if not pd.isna(macd) and not pd.isna(macd_signal):
                macd_trend = "상승 ↗️" if macd > macd_signal else "하락 ↘️"
                print(
                    f"│ MACD: {macd:.0f} ({macd_trend})                             │"
                )

            # 볼린저 밴드
            bb_upper = latest_daily.get("bb_upper", np.nan)
            bb_lower = latest_daily.get("bb_lower", np.nan)

            if not pd.isna(bb_upper) and not pd.isna(bb_lower):
                if current_price > bb_upper:
                    bb_position = "상단 돌파 🔴"
                elif current_price < bb_lower:
                    bb_position = "하단 이탈 🔵"
                else:
                    bb_position = "밴드 내부 ⚪"
                print(f"│ 볼린저밴드: {bb_position}                                       │")
                print(f"│   상단: {bb_upper:,.0f}원 | 하단: {bb_lower:,.0f}원             │")

            # ATR
            atr = latest_daily.get("atr", np.nan)
            if not pd.isna(atr):
                volatility = (
                    "높음"
                    if atr > current_price * 0.03
                    else "보통"
                    if atr > current_price * 0.01
                    else "낮음"
                )
                print(
                    f"│ ATR (14): {atr:,.0f}원 - 변동성 {volatility}                      │"
                )

        print("├─────────────────────────────────────────────────────────────────┤")
        print("│ ⏰ 시간봉 기준 (24시간 데이터)                                   │")
        print("├─────────────────────────────────────────────────────────────────┤")

        if latest_hourly is not None:
            hourly_rsi = latest_hourly.get("rsi", np.nan)
            if not pd.isna(hourly_rsi):
                if hourly_rsi >= 70:
                    hourly_rsi_signal = "과매수 🔴"
                elif hourly_rsi <= 30:
                    hourly_rsi_signal = "과매도 🔵"
                else:
                    hourly_rsi_signal = "중립 ⚪"
                print(
                    f"│ 단기 RSI (1H): {hourly_rsi:.1f} - {hourly_rsi_signal}           │"
                )

            hourly_sma_5 = latest_hourly.get("sma_5", np.nan)
            if not pd.isna(hourly_sma_5):
                hourly_trend = "↗️" if latest_hourly["close"] > hourly_sma_5 else "↘️"
                print(
                    f"│ 단기 SMA (5H): {hourly_sma_5:,.0f}원 {hourly_trend}             │"
                )

        print("└─────────────────────────────────────────────────────────────────┘")


async def main():
    """메인 실행 함수"""
    print("🚀 업비트 BTC 차트 분석 및 보조지표 계산기 시작!")
    print("   30일 일봉 + 24시간 시간봉 데이터를 분석합니다")
    if not TALIB_AVAILABLE:
        print("   ⚠️ TA-Lib 미설치: 기본 계산 방식을 사용합니다")
    print("=" * 65)

    analyzer = ChartDataAnalyzer()

    try:
        # 차트 데이터 분석
        result = analyzer.collect_and_analyze("KRW-BTC")

        if result:
            analyzer.print_analysis_console(result)
            print("\n✅ 차트 분석 완료!")
        else:
            print("\n❌ 차트 분석 실패!")

    except KeyboardInterrupt:
        print("\n🛑 사용자가 프로그램을 종료했습니다")
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
