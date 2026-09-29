"""
=====================================================================
الوحدة الثالثة (Module 3): خوارزمية الزناد (Trigger Logic Engine)
=====================================================================
المصدر: Binance USDT-M Futures WebSocket - Streams:
    - @aggTrade    (للبصمة، CVD، الامتصاص، الاختلال/Z-Score)
    - @kline_5m    (لتأكيد الكسر وبناء الـ FVG)
    - @kline_1h / @kline_1d / @kline_1w  (لإعادة بناء VWAP وPDH/PDL وH4/Daily Bias
      محلياً، لأن هذه الوحدة تعمل كخدمة مستقلة عن الوحدة الثانية على Railway)

الهدف: دمج الشروط الستة من وثيقة الاستراتيجية (1_strategy_blueprint.md) في محرك
واحد، وعند تحقق الشروط الستة معاً (AND) يطبع "إشارة جاهزة" فقط - بلا إرسال
تنبيه فعلي (هذا عمل الوحدة الرابعة القادمة) وبلا أي تنفيذ حقيقي للصفقات، وفق
القواعد الصارمة للمشروع (2_strict_rules.md).

الشروط الستة المطبقة هنا:
    1. Killzone: لندن (07:00-10:00 EST) أو نيويورك (13:00-16:00 EST).
    2. علاقة VWAP: السعر تحت الـ VWAP الأسبوعي (مرتبط بفلتر H4/Daily Bias
       وليس شرطاً مطلقاً وحيداً - كما اعتمدنا في قرارات المشروع).
    3. Sweep: كسر خط الإغراء (Inducement) وPDL معاً.
    4. Absorption: بيع ماركت ضخم (m==True) والسعر يتجمد (لا ينخفض أكثر من
       عتبة Ticks محددة).
    5. Imbalance: بدل نسبة 300% الثابتة، نستخدم Delta Z-Score - إذا تجاوزت
       الدلتا اللحظية عدداً معيناً من الانحرافات المعيارية عن متوسط الدلتا
       الأخيرة، نعتبره اختلالاً حقيقياً يتكيف مع تقلب السوق.
    6. تأكيد الكسر: إغلاق شمعة 5 دقائق فوق PDL، مع رصد FVG (فجوة قيمة عادلة)
       بين ذيل الشمعة الأولى والثالثة في نمط الاندفاع.
=====================================================================
"""

import asyncio
import json
import logging
import statistics
from collections import deque
from datetime import datetime, timezone
from enum import Enum

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("TriggerLogicEngine")


class MarketBias(Enum):
    """اتجاه الهيكل الأعلى - نفس منطق الوحدة الثانية، مُعاد بناؤه هنا محلياً."""
    BULLISH = "صاعد"
    BEARISH = "هابط"
    NEUTRAL = "محايد"


class TriggerLogicEngine:
    """
    محرك الزناد: يراقب كل الشروط الستة لحظياً، ويطبع فقط عند اكتمالها معاً.
    مبني بطريقة كائنية التوجه (OOP)، وكل شرط دالة منفصلة لتسهيل الاختبار
    والفصل لاحقاً عند مرحلة الـ Backtest (الوحدة الخامسة).
    """

    # إعدادات قابلة للتعديل لاحقاً بعد الاختبار (Backtest) بدل ثوابت جامدة في المنطق
    ABSORPTION_TICK_THRESHOLD = 10       # أقصى انخفاض بالـ Ticks أثناء الامتصاص
    TICK_SIZE = 0.10                     # حجم الـ Tick التقريبي لعقد BTCUSDT Futures
    ZSCORE_WINDOW = 50                   # عدد عينات الدلتا الأخيرة لحساب المتوسط والانحراف
    ZSCORE_THRESHOLD = 2.5               # عدد الانحرافات المعيارية لاعتبار الاختلال حقيقياً

    def __init__(self, symbol: str = "btcusdt"):
        self.symbol = symbol.lower()
        self.ws_url = (
            "wss://fstream.binance.com/stream?streams="
            f"{self.symbol}@aggTrade/{self.symbol}@kline_5m/"
            f"{self.symbol}@kline_1h/{self.symbol}@kline_1d/{self.symbol}@kline_1w"
        )

        # --- حالة البصمة والـ CVD (من الوحدة الأولى) ---
        self.cumulative_delta: float = 0.0
        self._delta_samples: deque = deque(maxlen=self.ZSCORE_WINDOW)  # عينات دلتا كل صفقة

        # --- حالة الخريطة (من الوحدة الثانية) ---
        self._daily_pv_sum: float = 0.0
        self._daily_vol_sum: float = 0.0
        self._weekly_pv_sum: float = 0.0
        self._weekly_vol_sum: float = 0.0
        self.daily_vwap = None
        self.weekly_vwap = None
        self._current_day_start = None
        self._current_week_start = None
        self.pdh = None
        self.pdl = None
        self._last_swing_low = None
        self.inducement_level = None
        self._h1_candles_buffer: list[dict] = []
        self.h4_bias = MarketBias.NEUTRAL
        self.daily_bias = MarketBias.NEUTRAL

        # --- حالة الزناد (خاصة بهذه الوحدة) ---
        self._recent_lows: deque = deque(maxlen=20)     # آخر قيعان لحظية لرصد الامتصاص
        self._sweep_triggered: bool = False              # هل حدث Sweep منذ آخر إعادة ضبط
        self._sweep_low_price: float | None = None        # أدنى سعر أثناء الـ Sweep (لحساب SL لاحقاً)
        self._absorption_confirmed: bool = False
        self._last_3_candles: deque = deque(maxlen=3)     # لبناء FVG من شمعات 5 دقائق

        self._reconnect_delay = 2
        self._max_reconnect_delay = 30

    # ------------------------------------------------------------------
    # الشرط 1: Killzone (لندن أو نيويورك بتوقيت EST)
    # ------------------------------------------------------------------
    @staticmethod
    def _is_in_killzone() -> bool:
        """
        يحول الوقت الحالي (UTC) لتوقيت EST تقريبي (UTC-5، بلا مراعاة التوقيت
        الصيفي حالياً - قابل للتحسين لاحقاً) ويتحقق من جلستي لندن ونيويورك.
        """
        now_utc = datetime.now(timezone.utc)
        est_hour = (now_utc.hour - 5) % 24
        london_session = 7 <= est_hour < 10
        newyork_session = 13 <= est_hour < 16
        return london_session or newyork_session

    # ------------------------------------------------------------------
    # الشرط 2: علاقة VWAP الأسبوعي (مرتبط بـ H4/Daily Bias)
    # ------------------------------------------------------------------
    def _is_discount_zone(self, current_price: float) -> bool:
        """السعر تحت الـ VWAP الأسبوعي، والهيكل الأعلى لا يمنع الشراء (ليس Bearish)."""
        if self.weekly_vwap is None:
            return False
        under_vwap = current_price < self.weekly_vwap
        bias_allows = self.h4_bias != MarketBias.BEARISH and self.daily_bias != MarketBias.BEARISH
        return under_vwap and bias_allows

    # ------------------------------------------------------------------
    # الشرط 3: Sweep (كسر الإغراء وPDL معاً)
    # ------------------------------------------------------------------
    def _check_sweep(self, current_price: float) -> None:
        if self.pdl is None or self.inducement_level is None:
            return
        if current_price < self.inducement_level and current_price < self.pdl:
            if not self._sweep_triggered:
                logger.info(f"⚡ Sweep مرصود: السعر {current_price:.2f} كسر الإغراء وPDL معاً.")
            self._sweep_triggered = True
            if self._sweep_low_price is None or current_price < self._sweep_low_price:
                self._sweep_low_price = current_price

    # ------------------------------------------------------------------
    # الشرط 4: Absorption (تجمّد السعر رغم بيع ضخم)
    # ------------------------------------------------------------------
    def _check_absorption(self, current_price: float, is_buyer_maker: bool) -> None:
        if not self._sweep_triggered or self._sweep_low_price is None:
            return
        self._recent_lows.append(current_price)
        if not is_buyer_maker:  # نبحث تحديداً عن ضغط بيع (m == True)
            return
        drop_in_ticks = (self._sweep_low_price - min(self._recent_lows)) / self.TICK_SIZE
        if abs(drop_in_ticks) <= self.ABSORPTION_TICK_THRESHOLD:
            if not self._absorption_confirmed:
                logger.info("🧊 Absorption مؤكد: السعر يتجمد رغم ضغط البيع.")
            self._absorption_confirmed = True

    # ------------------------------------------------------------------
    # الشرط 5: Imbalance عبر Delta Z-Score (بدل نسبة 300% الثابتة)
    # ------------------------------------------------------------------
    def _check_imbalance_zscore(self) -> bool:
        if len(self._delta_samples) < self.ZSCORE_WINDOW:
            return False
        mean = statistics.mean(self._delta_samples)
        stdev = statistics.pstdev(self._delta_samples)
        if stdev == 0:
            return False
        current = self._delta_samples[-1]
        z_score = (current - mean) / stdev
        return z_score >= self.ZSCORE_THRESHOLD

    # ------------------------------------------------------------------
    # الشرط 6: تأكيد الكسر + FVG (من شمعات 5 دقائق)
    # ------------------------------------------------------------------
    def _check_breakout_confirmation_and_fvg(self) -> bool:
        if len(self._last_3_candles) < 3 or self.pdl is None:
            return False
        first, _, third = self._last_3_candles
        # فجوة القيمة العادلة: ذيل الشمعة الأولى (Low) أعلى من ذيل الشمعة الثالثة؟
        # في نمط اندفاع صاعد: قمة الشمعة الأولى أقل من قاع الشمعة الثالثة = فجوة صاعدة
        has_fvg = first["high"] < third["low"]
        last_close = self._last_3_candles[-1]["close"]
        closed_above_pdl = last_close > self.pdl
        return has_fvg and closed_above_pdl

    def _reset_trigger_state(self) -> None:
        """إعادة ضبط حالة الزناد بعد كل محاولة (نجحت أو فشلت) لبدء دورة جديدة."""
        self._sweep_triggered = False
        self._sweep_low_price = None
        self._absorption_confirmed = False
        self._recent_lows.clear()

    # ------------------------------------------------------------------
    # معالجة صفقات aggTrade (تحديث CVD + فحص الشروط 3، 4، 5)
    # ------------------------------------------------------------------
    def _process_agg_trade(self, data: dict) -> None:
        price = float(data["p"])
        quantity = float(data["q"])
        is_buyer_maker = data["m"]

        delta = -quantity if is_buyer_maker else quantity
        self.cumulative_delta += delta
        self._delta_samples.append(delta)

        self._check_sweep(price)
        self._check_absorption(price, is_buyer_maker)
        self._evaluate_all_conditions(price)

    # ------------------------------------------------------------------
    # معالجة شمعات 5 دقائق (بناء FVG وتأكيد الكسر)
    # ------------------------------------------------------------------
    def _process_5m_kline(self, kline: dict) -> None:
        if not kline["x"]:  # فقط عند إغلاق الشمعة رسمياً
            return
        candle = {
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
        }
        self._last_3_candles.append(candle)

    # ------------------------------------------------------------------
    # إعادة بناء الخريطة محلياً (نفس منطق الوحدة الثانية)
    # ------------------------------------------------------------------
    def _process_1h_kline(self, kline: dict) -> None:
        if not kline["x"]:
            return
        self._h1_candles_buffer.append({"open": float(kline["o"]), "close": float(kline["c"])})
        if len(self._h1_candles_buffer) > 4:
            self._h1_candles_buffer.pop(0)
        if len(self._h1_candles_buffer) == 4:
            self.h4_bias = self._determine_bias(
                self._h1_candles_buffer[0]["open"], self._h1_candles_buffer[-1]["close"]
            )

    def _process_1d_kline(self, kline: dict) -> None:
        open_time = kline["t"]
        if self._current_day_start != open_time:
            self._current_day_start = open_time
            self._daily_pv_sum = 0.0
            self._daily_vol_sum = 0.0
        typical_price = (float(kline["h"]) + float(kline["l"]) + float(kline["c"])) / 3
        volume = float(kline["v"])
        self._daily_pv_sum += typical_price * volume
        self._daily_vol_sum += volume
        if self._daily_vol_sum > 0:
            self.daily_vwap = self._daily_pv_sum / self._daily_vol_sum
        if kline["x"]:
            self.pdh = float(kline["h"])
            self.pdl = float(kline["l"])
            self.daily_bias = self._determine_bias(float(kline["o"]), float(kline["c"]))
            self._last_swing_low = None
            self.inducement_level = None
            self._reset_trigger_state()

    def _process_1w_kline(self, kline: dict) -> None:
        open_time = kline["t"]
        if self._current_week_start != open_time:
            self._current_week_start = open_time
            self._weekly_pv_sum = 0.0
            self._weekly_vol_sum = 0.0
        typical_price = (float(kline["h"]) + float(kline["l"]) + float(kline["c"])) / 3
        volume = float(kline["v"])
        self._weekly_pv_sum += typical_price * volume
        self._weekly_vol_sum += volume
        if self._weekly_vol_sum > 0:
            self.weekly_vwap = self._weekly_pv_sum / self._weekly_vol_sum

    @staticmethod
    def _determine_bias(open_price: float, close_price: float) -> MarketBias:
        change_pct = (close_price - open_price) / open_price * 100
        if change_pct > 0.15:
            return MarketBias.BULLISH
        elif change_pct < -0.15:
            return MarketBias.BEARISH
        return MarketBias.NEUTRAL

    def update_inducement_watch(self, current_low: float) -> None:
        if self.pdl is None:
            return
        if current_low > self.pdl and (self._last_swing_low is None or current_low < self._last_swing_low):
            self._last_swing_low = current_low
            self.inducement_level = current_low

    # ------------------------------------------------------------------
    # التقييم النهائي: هل تحققت الشروط الستة معاً؟
    # ------------------------------------------------------------------
    def _evaluate_all_conditions(self, current_price: float) -> None:
        conditions = {
            "Killzone": self._is_in_killzone(),
            "الخصم (VWAP)": self._is_discount_zone(current_price),
            "Sweep": self._sweep_triggered,
            "Absorption": self._absorption_confirmed,
            "Imbalance (Z-Score)": self._check_imbalance_zscore(),
            "تأكيد الكسر + FVG": self._check_breakout_confirmation_and_fvg(),
        }

        if all(conditions.values()):
            logger.info("=" * 60)
            logger.info(f"🟢 إشارة جاهزة (Long) - {self.symbol.upper()} عند السعر {current_price:.2f}")
            logger.info(f"   أدنى سعر Sweep (لحساب SL لاحقاً): {self._sweep_low_price}")
            logger.info("   [هذه الوحدة لا ترسل تنبيهاً ولا تنفذ صفقة - الوحدة الرابعة القادمة]")
            logger.info("=" * 60)
            self._reset_trigger_state()  # منع تكرار نفس الإشارة فوراً

    # ------------------------------------------------------------------
    def _route_message(self, message: dict) -> None:
        stream = message.get("stream", "")
        data = message.get("data", {})

        if "@aggTrade" in stream:
            self._process_agg_trade(data)
        elif "@kline_5m" in stream:
            self._process_5m_kline(data["k"])
        elif "@kline_1h" in stream:
            self._process_1h_kline(data["k"])
        elif "@kline_1d" in stream:
            self._process_1d_kline(data["k"])
        elif "@kline_1w" in stream:
            self._process_1w_kline(data["k"])

    async def _listen(self) -> None:
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            logger.info(f"تم الاتصال بنجاح بمحرك الزناد: {self.ws_url}")
            self._reconnect_delay = 2
            async for raw_message in ws:
                try:
                    message = json.loads(raw_message)
                    self._route_message(message)
                except (KeyError, ValueError) as parse_error:
                    logger.warning(f"تجاهل رسالة غير صالحة: {parse_error}")

    async def run(self) -> None:
        while True:
            try:
                await self._listen()
            except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as error:
                logger.error(f"انقطع الاتصال: {error} - إعادة المحاولة خلال {self._reconnect_delay} ثانية...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)
            except Exception as unexpected_error:
                logger.error(f"خطأ غير متوقع: {unexpected_error} - إعادة المحاولة خلال {self._reconnect_delay} ثانية...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, self._max_reconnect_delay)


async def main():
    engine = TriggerLogicEngine(symbol="btcusdt")
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
