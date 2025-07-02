#!/usr/bin/env python3
"""
업비트 기술분석기
- 업비트 OHLCV 데이터 수집
- 보조지표 계산 (SMA, RSI, MACD, 볼린저밴드, ATR)
- InfluxDB 저장 (ohlcv_data, technical_indicators)
- 스케줄링 실행 (기본 5분 간격)
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# TA-Lib 임포트 (선택사항)
try:
    import talib

    TALIB_AVAILABLE = True
    print("✅ TA-Lib 사용 가능")
except ImportError:
    TALIB_AVAILABLE = False
    print("⚠️ TA-Lib 없음 - 기본 계산 사용")

# 환경 변수 로드
load_dotenv()


def setup_logging():
    """로깅 설정"""
    search_paths = [
        "logs",
        "../logs",
        "../../logs",
        "../../../logs",
        "../../../../logs",
        "../../../../../logs",
    ]

    log_dir = None
    for path in search_paths:
        full_path = os.path.abspath(path)
        parent_dir = os.path.dirname(full_path)
        if os.path.exists(parent_dir):
            log_dir = full_path
            break

    if log_dir is None:
        log_dir = "logs"

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "technical_analysis.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    return log_file


logger = logging.getLogger(__name__)


class TechnicalInfluxConfig:
    """기술분석용 InfluxDB 설정"""

    def __init__(self):
        self.url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.token = os.getenv(
            "INFLUXDB_TOKEN", "one-bailey-admin-token-12345678901234567890"
        )
        self.org = os.getenv("INFLUXDB_ORG", "one-bailey")
        self.bucket = os.getenv("INFLUXDB_BUCKET", "trading_data")


class UpbitTechnicalAnalyzer:
    """
    업비트 기술분석기
    - 업비트 API 데이터 수집
    - 보조지표 계산
    - InfluxDB 저장
    """

    def __init__(self):
        self.upbit_url = "https://api.upbit.com/v1"
        self.influx_config = TechnicalInfluxConfig()
        self.influx_client = None
        self.write_api = None
        self._initialize_influxdb()

        # 통계
        self.analysis_count = 0
        self.success_count = 0
        self.ohlcv_write_count = 0
        self.indicators_write_count = 0
        self.start_time = time.time()

    def _initialize_influxdb(self):
        """InfluxDB 클라이언트 초기화"""
        try:
            self.influx_client = InfluxDBClient(
                url=self.influx_config.url,
                token=self.influx_config.token,
                org=self.influx_config.org,
                timeout=30000,
            )

            # 동기 방식 사용 (에러 감지 위해)
            self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

            # 연결 테스트
            health = self.influx_client.health()
            if health.status == "pass":
                logger.info("✅ 기술분석용 InfluxDB 연결 성공")
            else:
                raise Exception(f"InfluxDB 상태 이상: {health.status}")

        except Exception as e:
            logger.error(f"❌ 기술분석용 InfluxDB 초기화 실패: {e}")
            raise

    # ===========================================
    # 데이터 수집 관련 메서드
    # ===========================================

    def fetch_ohlcv_data(
        self, market: str, interval: str, count: int
    ) -> Optional[pd.DataFrame]:
        """업비트 OHLCV 데이터 수집"""
        try:
            # API 엔드포인트 결정
            if interval == "days":
                endpoint = f"{self.upbit_url}/candles/days"
            elif interval.startswith("minutes"):
                unit = interval.split("/")[1] if "/" in interval else "60"
                endpoint = f"{self.upbit_url}/candles/minutes/{unit}"
            else:
                logger.error(f"❌ 지원하지 않는 interval: {interval}")
                return None

            # API 호출
            params = {"market": market, "count": min(count, 200)}
            response = requests.get(endpoint, params=params, timeout=15)

            if response.status_code != 200:
                logger.error(f"❌ API 호출 실패: {response.status_code}")
                return None

            data = response.json()
            if not data:
                logger.warning("⚠️ 빈 데이터 응답")
                return None

            # DataFrame 변환
            df = pd.DataFrame(data)

            # 컬럼명 정리
            df = df.rename(
                columns={
                    "candle_date_time_utc": "datetime",
                    "opening_price": "open",
                    "high_price": "high",
                    "low_price": "low",
                    "trade_price": "close",
                    "candle_acc_trade_volume": "volume",
                    "candle_acc_trade_price": "volume_krw",
                }
            )

            # 필요한 컬럼만 선택
            required_columns = ["datetime", "open", "high", "low", "close", "volume"]
            available_columns = [col for col in required_columns if col in df.columns]
            df = df[available_columns].copy()

            # volume_krw 계산
            if "volume_krw" not in df.columns:
                df["volume_krw"] = df["volume"] * df["close"]

            # 타입 변환
            df["datetime"] = pd.to_datetime(df["datetime"])
            for col in ["open", "high", "low", "close", "volume", "volume_krw"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # 시간순 정렬
            df = df.sort_values("datetime").reset_index(drop=True)

            logger.info(f"✅ {market} {interval} 데이터 {len(df)}개 수집 완료")
            return df

        except Exception as e:
            logger.error(f"❌ OHLCV 데이터 수집 실패: {e}")
            return None

    # ===========================================
    # 보조지표 계산 관련 메서드
    # ===========================================

    def calculate_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """보조지표 계산"""
        if df is None or df.empty:
            return {}

        indicators = {}
        data_length = len(df)
        logger.info(f"📊 데이터 길이: {data_length}개")

        try:
            # 1. 이동평균선 (SMA)
            indicators["moving_averages"] = self._calculate_moving_averages(
                df, data_length
            )

            # 2. 모멘텀 지표 (RSI, MACD)
            indicators["momentum_indicators"] = self._calculate_momentum_indicators(
                df, data_length
            )

            # 3. 변동성 지표 (볼린저밴드, ATR)
            indicators["volatility_indicators"] = self._calculate_volatility_indicators(
                df, data_length
            )

            # 계산된 지표 로깅
            total_indicators = sum(len(category) for category in indicators.values())
            logger.info(f"📊 계산된 지표: {total_indicators}개")

            return indicators

        except Exception as e:
            logger.error(f"❌ 보조지표 계산 실패: {e}")
            return {}

    def _calculate_moving_averages(
        self, df: pd.DataFrame, data_length: int
    ) -> Dict[str, Any]:
        """이동평균선 계산"""
        moving_averages = {}
        for period in [5, 20, 60, 120]:
            if data_length >= period:
                try:
                    if TALIB_AVAILABLE:
                        sma_values = talib.SMA(df["close"].values, timeperiod=period)
                        current_sma = (
                            sma_values[-1]
                            if len(sma_values) > 0 and not np.isnan(sma_values[-1])
                            else None
                        )
                    else:
                        current_sma = df["close"].rolling(window=period).mean().iloc[-1]

                    if current_sma is not None and not pd.isna(current_sma):
                        current_price = df["close"].iloc[-1]
                        trend = "up" if current_price > current_sma else "down"
                        signal = "bullish" if current_price > current_sma else "bearish"

                        moving_averages[f"sma_{period}"] = {
                            "value": float(current_sma),
                            "trend": trend,
                            "signal": signal,
                        }

                except Exception as e:
                    logger.warning(f"⚠️ SMA {period} 계산 실패: {e}")

        return moving_averages

    def _calculate_momentum_indicators(
        self, df: pd.DataFrame, data_length: int
    ) -> Dict[str, Any]:
        """모멘텀 지표 계산 (RSI, MACD)"""
        momentum_indicators = {}

        # RSI 계산
        if data_length >= 14:
            try:
                if TALIB_AVAILABLE:
                    rsi_values = talib.RSI(df["close"].values, timeperiod=14)
                    current_rsi = (
                        rsi_values[-1]
                        if len(rsi_values) > 0 and not np.isnan(rsi_values[-1])
                        else None
                    )
                else:
                    delta = df["close"].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None

                if current_rsi is not None and not pd.isna(current_rsi):
                    if current_rsi >= 70:
                        rsi_signal = "overbought"
                    elif current_rsi <= 30:
                        rsi_signal = "oversold"
                    else:
                        rsi_signal = "neutral"

                    momentum_indicators["rsi"] = {
                        "value": float(current_rsi),
                        "signal": rsi_signal,
                        "strength": float(abs(current_rsi - 50) / 50),
                    }

            except Exception as e:
                logger.warning(f"⚠️ RSI 계산 실패: {e}")

        # MACD 계산
        if data_length >= 26:
            try:
                if TALIB_AVAILABLE:
                    macd_line, macd_signal, macd_histogram = talib.MACD(
                        df["close"].values, fastperiod=12, slowperiod=26, signalperiod=9
                    )
                    current_macd = (
                        macd_line[-1]
                        if len(macd_line) > 0 and not np.isnan(macd_line[-1])
                        else None
                    )
                    current_signal = (
                        macd_signal[-1]
                        if len(macd_signal) > 0 and not np.isnan(macd_signal[-1])
                        else None
                    )
                    current_histogram = (
                        macd_histogram[-1]
                        if len(macd_histogram) > 0 and not np.isnan(macd_histogram[-1])
                        else None
                    )
                else:
                    ema_fast = df["close"].ewm(span=12).mean()
                    ema_slow = df["close"].ewm(span=26).mean()
                    macd = ema_fast - ema_slow
                    signal = macd.ewm(span=9).mean()
                    histogram = macd - signal

                    current_macd = macd.iloc[-1]
                    current_signal = signal.iloc[-1]
                    current_histogram = histogram.iloc[-1]

                if all(
                    x is not None and not pd.isna(x)
                    for x in [current_macd, current_signal, current_histogram]
                ):
                    macd_signal_direction = (
                        "bullish" if current_macd > current_signal else "bearish"
                    )
                    crossover = (
                        1
                        if abs(current_macd - current_signal)
                        < abs(current_histogram) * 0.1
                        else 0
                    )

                    momentum_indicators["macd"] = {
                        "macd_line": float(current_macd),
                        "signal_line": float(current_signal),
                        "histogram": float(current_histogram),
                        "signal": macd_signal_direction,
                        "crossover": crossover,
                    }

            except Exception as e:
                logger.warning(f"⚠️ MACD 계산 실패: {e}")

        return momentum_indicators

    def _calculate_volatility_indicators(
        self, df: pd.DataFrame, data_length: int
    ) -> Dict[str, Any]:
        """변동성 지표 계산 (볼린저밴드, ATR)"""
        volatility_indicators = {}

        # 볼린저 밴드
        if data_length >= 20:
            try:
                if TALIB_AVAILABLE:
                    upper, middle, lower = talib.BBANDS(
                        df["close"].values, timeperiod=20, nbdevup=2, nbdevdn=2
                    )
                    current_upper = (
                        upper[-1]
                        if len(upper) > 0 and not np.isnan(upper[-1])
                        else None
                    )
                    current_middle = (
                        middle[-1]
                        if len(middle) > 0 and not np.isnan(middle[-1])
                        else None
                    )
                    current_lower = (
                        lower[-1]
                        if len(lower) > 0 and not np.isnan(lower[-1])
                        else None
                    )
                else:
                    sma = df["close"].rolling(window=20).mean()
                    std = df["close"].rolling(window=20).std()
                    current_upper = (sma + (std * 2)).iloc[-1]
                    current_middle = sma.iloc[-1]
                    current_lower = (sma - (std * 2)).iloc[-1]

                if all(
                    x is not None and not pd.isna(x)
                    for x in [current_upper, current_middle, current_lower]
                ):
                    current_price = df["close"].iloc[-1]
                    width = current_upper - current_lower

                    if current_price > current_upper:
                        position = "upper"
                    elif current_price < current_lower:
                        position = "lower"
                    else:
                        position = "middle"

                    squeeze = 1 if width < current_middle * 0.1 else 0

                    volatility_indicators["bollinger_bands"] = {
                        "upper": float(current_upper),
                        "middle": float(current_middle),
                        "lower": float(current_lower),
                        "width": float(width),
                        "position": position,
                        "squeeze": squeeze,
                    }

            except Exception as e:
                logger.warning(f"⚠️ 볼린저밴드 계산 실패: {e}")

        # ATR
        if data_length >= 14:
            try:
                if TALIB_AVAILABLE:
                    atr_values = talib.ATR(
                        df["high"].values,
                        df["low"].values,
                        df["close"].values,
                        timeperiod=14,
                    )
                    current_atr = (
                        atr_values[-1]
                        if len(atr_values) > 0 and not np.isnan(atr_values[-1])
                        else None
                    )
                else:
                    high_low = df["high"] - df["low"]
                    high_close = np.abs(df["high"] - df["close"].shift())
                    low_close = np.abs(df["low"] - df["close"].shift())
                    true_range = pd.concat(
                        [high_low, high_close, low_close], axis=1
                    ).max(axis=1)
                    current_atr = true_range.rolling(window=14).mean().iloc[-1]

                if (
                    current_atr is not None
                    and not pd.isna(current_atr)
                    and current_atr > 0
                ):
                    current_price = df["close"].iloc[-1]
                    volatility_level = (
                        "high"
                        if current_atr > current_price * 0.03
                        else "medium" if current_atr > current_price * 0.01 else "low"
                    )

                    volatility_indicators["atr"] = {
                        "value": float(current_atr),
                        "volatility_level": volatility_level,
                    }

            except Exception as e:
                logger.warning(f"⚠️ ATR 계산 실패: {e}")

        return volatility_indicators

    # ===========================================
    # InfluxDB 저장 관련 메서드
    # ===========================================

    def create_ohlcv_points(
        self, df: pd.DataFrame, symbol: str, timeframe: str
    ) -> List[Point]:
        """OHLCV 데이터를 InfluxDB Points로 변환"""
        points = []

        try:
            records = df.to_dict("records")

            for record in records:
                point = (
                    Point("ohlcv_data")
                    .tag("symbol", symbol)
                    .tag("timeframe", timeframe)
                    .field("open", float(record["open"]))
                    .field("high", float(record["high"]))
                    .field("low", float(record["low"]))
                    .field("close", float(record["close"]))
                    .field("volume", float(record["volume"]))
                    .field("volume_krw", float(record["volume_krw"]))
                    .time(record["datetime"])
                )

                points.append(point)

        except Exception as e:
            logger.error(f"❌ OHLCV Point 생성 실패: {e}")

        return points

    def create_indicator_points(
        self, indicators: Dict[str, Any], symbol: str
    ) -> List[Point]:
        """보조지표를 InfluxDB Points로 변환"""
        points = []
        timestamp = datetime.now(timezone.utc)

        try:
            # SMA 지표
            for sma_name, sma_data in indicators.get("moving_averages", {}).items():
                point = (
                    Point("technical_indicators")
                    .tag("symbol", symbol)
                    .tag("timeframe", "1d")
                    .tag("indicator_type", "sma")
                    .tag("indicator_name", sma_name)
                    .field("value", sma_data["value"])
                    .field("trend", sma_data["trend"])
                    .field("signal", sma_data["signal"])
                    .time(timestamp)
                )
                points.append(point)

            # RSI
            rsi_data = indicators.get("momentum_indicators", {}).get("rsi", {})
            if rsi_data:
                point = (
                    Point("technical_indicators")
                    .tag("symbol", symbol)
                    .tag("timeframe", "1d")
                    .tag("indicator_type", "rsi")
                    .tag("indicator_name", "rsi")
                    .field("value", rsi_data["value"])
                    .field("signal", rsi_data["signal"])
                    .field("strength", rsi_data["strength"])
                    .time(timestamp)
                )
                points.append(point)

            # MACD
            macd_data = indicators.get("momentum_indicators", {}).get("macd", {})
            if macd_data:
                point = (
                    Point("technical_indicators")
                    .tag("symbol", symbol)
                    .tag("timeframe", "1d")
                    .tag("indicator_type", "macd")
                    .tag("indicator_name", "macd")
                    .field("value", macd_data["macd_line"])
                    .field("value_secondary", macd_data["signal_line"])
                    .field("value_tertiary", macd_data["histogram"])
                    .field("signal", macd_data["signal"])
                    .field("crossover", macd_data["crossover"])
                    .time(timestamp)
                )
                points.append(point)

            # 볼린저 밴드
            bb_data = indicators.get("volatility_indicators", {}).get(
                "bollinger_bands", {}
            )
            if bb_data:
                point = (
                    Point("technical_indicators")
                    .tag("symbol", symbol)
                    .tag("timeframe", "1d")
                    .tag("indicator_type", "bollinger_bands")
                    .tag("indicator_name", "bb")
                    .field("value", bb_data["middle"])
                    .field("value_secondary", bb_data["upper"])
                    .field("value_tertiary", bb_data["lower"])
                    .field("width", bb_data["width"])
                    .field("position", bb_data["position"])
                    .field("squeeze", bb_data["squeeze"])
                    .time(timestamp)
                )
                points.append(point)

            # ATR
            atr_data = indicators.get("volatility_indicators", {}).get("atr", {})
            if atr_data:
                point = (
                    Point("technical_indicators")
                    .tag("symbol", symbol)
                    .tag("timeframe", "1d")
                    .tag("indicator_type", "atr")
                    .tag("indicator_name", "atr")
                    .field("value", atr_data["value"])
                    .field("volatility_level", atr_data["volatility_level"])
                    .time(timestamp)
                )
                points.append(point)

        except Exception as e:
            logger.error(f"❌ 보조지표 Point 생성 실패: {e}")

        return points

    def save_to_influxdb(self, points: List[Point]) -> bool:
        """InfluxDB에 Points 저장"""
        if not points:
            return True

        try:
            self.write_api.write(
                bucket=self.influx_config.bucket,
                org=self.influx_config.org,
                record=points,
            )
            return True

        except Exception as e:
            logger.error(f"❌ InfluxDB 저장 실패: {e}")
            return False

    # ===========================================
    # 통합 분석 실행 관련 메서드
    # ===========================================

    async def analyze_symbol(self, symbol: str = "KRW-BTC") -> bool:
        """심볼 분석 및 저장"""
        try:
            logger.info(f"🔄 {symbol} 기술분석 시작...")
            self.analysis_count += 1

            # 1. 시장 데이터 수집
            daily_data = self.fetch_ohlcv_data(symbol, "days", 30)
            hourly_data = self.fetch_ohlcv_data(symbol, "minutes/60", 24)

            if daily_data is None:
                logger.error(f"❌ {symbol} 일봉 데이터 수집 실패")
                return False

            # 2. 보조지표 계산
            indicators = self.calculate_indicators(daily_data)

            # 3. InfluxDB Points 생성
            all_points = []

            # OHLCV 데이터
            daily_points = self.create_ohlcv_points(daily_data, symbol, "1d")
            all_points.extend(daily_points)
            self.ohlcv_write_count += len(daily_points)

            if hourly_data is not None:
                hourly_points = self.create_ohlcv_points(hourly_data, symbol, "1h")
                all_points.extend(hourly_points)
                self.ohlcv_write_count += len(hourly_points)

            # 보조지표 데이터
            if indicators:
                indicator_points = self.create_indicator_points(indicators, symbol)
                all_points.extend(indicator_points)
                self.indicators_write_count += len(indicator_points)

            # 4. InfluxDB 저장
            if all_points:
                success = self.save_to_influxdb(all_points)
                if success:
                    logger.info(f"✅ {symbol} 분석 완료: {len(all_points)}건 저장")
                    self.success_count += 1
                    return True
                else:
                    logger.error(f"❌ {symbol} 저장 실패")
                    return False
            else:
                logger.warning(f"⚠️ {symbol} 저장할 데이터 없음")
                return False

        except Exception as e:
            logger.error(f"❌ {symbol} 기술분석 실패: {e}")
            return False

    def print_analysis_statistics(self):
        """분석 통계 출력"""
        uptime = time.time() - self.start_time
        success_rate = (self.success_count / max(self.analysis_count, 1)) * 100

        print(
            f"""
┌─────────────────────────────────────────────────────────────────┐
│ 📈 업비트 기술분석 통계                                         │
├─────────────────────────────────────────────────────────────────┤
│ 총 분석: {self.analysis_count}회                                │
│ 성공: {self.success_count}회 ({success_rate:.1f}%)              │
│ 가동시간: {uptime/60:.1f}분                                     │
├─────────────────────────────────────────────────────────────────┤
│ 💾 InfluxDB 저장:                                              │
│   OHLCV: {self.ohlcv_write_count}건                            │
│   보조지표: {self.indicators_write_count}건                     │
└─────────────────────────────────────────────────────────────────┘
        """
        )

    # ===========================================
    # 스케줄러 관련 메서드
    # ===========================================

    async def start_scheduler(
        self, symbols: List[str] = ["KRW-BTC"], interval_minutes: int = 5
    ):
        """기술분석 스케줄러 시작"""
        logger.info(f"📈 기술분석 스케줄러 시작: {interval_minutes}분 간격")

        while True:
            try:
                for symbol in symbols:
                    await self.analyze_symbol(symbol)

                # 통계 출력
                self.print_analysis_statistics()

                # 대기
                logger.info(f"⏰ {interval_minutes}분 대기 중...")
                await asyncio.sleep(interval_minutes * 60)

            except KeyboardInterrupt:
                logger.info("🛑 기술분석 스케줄러 중지")
                break
            except Exception as e:
                logger.error(f"❌ 스케줄러 오류: {e}")
                await asyncio.sleep(30)

    def close(self):
        """리소스 정리"""
        if self.write_api:
            self.write_api.close()
        if self.influx_client:
            self.influx_client.close()
        logger.info("🔌 기술분석기 종료")


async def test_technical_connection():
    """기술분석 연결 테스트"""
    print("🧪 업비트 기술분석 연결 테스트...")

    try:
        analyzer = UpbitTechnicalAnalyzer()

        # 간단한 분석 테스트
        test_success = await analyzer.analyze_symbol("KRW-BTC")
        analyzer.close()

        if test_success:
            print("✅ 기술분석 연결 테스트 성공!")
            return True
        else:
            print("❌ 기술분석 연결 테스트 실패!")
            return False

    except Exception as e:
        print(f"❌ 테스트 중 오류: {e}")
        return False


async def main():
    """업비트 기술분석기 메인 함수"""
    print("🚀 업비트 기술분석기 시작!")
    print("=" * 50)

    # 로깅 초기화
    log_file_path = setup_logging()
    print(f"📝 로그 파일: {log_file_path}")

    analyzer = UpbitTechnicalAnalyzer()

    try:
        # 초기 테스트
        test_success = await analyzer.analyze_symbol("KRW-BTC")
        if not test_success:
            print("❌ 초기 테스트 실패. 설정을 확인하세요.")
            return

        print("✅ 초기 테스트 성공!")
        print("📊 InfluxDB UI: http://localhost:8086")
        print("📈 5분마다 기술분석이 실행됩니다...")
        print("🛑 종료하려면 Ctrl+C를 누르세요\n")

        # 스케줄러 실행 (5분 간격)
        await analyzer.start_scheduler(["KRW-BTC"], interval_minutes=5)

    except KeyboardInterrupt:
        print("\n🛑 기술분석기 종료")
    except Exception as e:
        logger.error(f"❌ 실행 중 오류: {e}")
    finally:
        analyzer.close()


if __name__ == "__main__":
    asyncio.run(main())
