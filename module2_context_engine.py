"""
=====================================================================
الوحدة الثانية (Module 2): محرك الخريطة (Context Engine)
=====================================================================
المصدر: Binance USDT-M Futures WebSocket - Streams:
    - @kline_1h  (لحساب فلتر الاتجاه الأعلى H4 عبر تجميع 4 شمعات ساعة)
    - @kline_1d  (لحساب PDH/PDL وVWAP اليومي وفلتر Daily Bias)
    - @kline_1w  (لحساب VWAP الأسبوعي)

الهدف:
    1. حساب VWAP اليومي والأسبوعي تراكمياً من بيانات الشموع الرسمية (Kline).
       (VWAP = مجموع (السعر النموذجي * الحجم) / مجموع الحجم، منذ بداية الفترة)
    2. تحديد PDH (Previous Day High) وPDL (Previous Day Low) من إغلاق اليوم السابق.
    3. رصد "قاع الإغراء" (Inducement): قاع فرعي صغير يتشكل قبل كسر PDL الرئيسي،
       يليه ارتفاع طفيف - يشير لاحتمال فخ سيولة (راجع 1_strategy_blueprint.md).
    4. فلتر الاتجاه الأعلى (H4/Daily Bias): يحدد هل الهيكل الأكبر يسمح بالشراء
       فقط (Bullish) أو لا (Bearish/Neutral)، بدل الاعتماد فقط على "تحت VWAP"
       كشرط مطلق - كما اعتمدنا في قرارات المشروع.

ملاحظة: هذه الوحدة لا ترسل أي تنبيه ولا تنفذ أي صفقة - فقط تجهّز "الخريطة"
(Context) التي ستستهلكها الوحدة الثالثة (Trigger Logic).
=====================================================================
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ContextEngine")


class MarketBias(Enum):
    """اتجاه الهيكل الأعلى (H4/Daily) - يُستخدم كفلتر قبل السماح بالبحث عن صفقات شراء."""
    BULLISH = "صاعد (Long مسموح)"
    BEARISH = "هابط (Long ممنوع)"
    NEUTRAL = "محايد (انتظار)"


class ContextEngine:
    """
    محرك الخريطة: يبني الصورة الأكبر للسوق (VWAP، PDH/PDL، الإغراء، الاتجاه الأعلى)
    اعتماداً على بيانات الشموع الرسمية من Binance - بلا أي مؤشر كلاسيكي.
    مبني بطريقة كائنية التوجه (OOP) ليُستهلك لاحقاً من الوحدة الثالثة (Trigger Logic).
    """

    def __init__(self, symbol: str = "btcusdt"):
        self.symbol = symbol.lower()

        # ثلاثة ستريمات مدمجة في اتصال واحد (Combined Stream) لتقليل عدد الاتصالات
        self.ws_url = (
            "wss://fstream.binance.com/stream?streams="
            f"{self.symbol}@kline_1h/{self.symbol}@kline_1d/{self.symbol}@kline_1w"
        )

        # --- حالة الـ VWAP التراكمي (يُعاد تصفيره عند بداية كل يوم/أسبوع جديد) ---
        self._daily_pv_sum: float = 0.0    # مجموع (السعر النموذجي * الحجم) لليوم الحالي
        self._daily_vol_sum: float = 0.0   # مجموع الحجم لليوم الحالي
        self._weekly_pv_sum: float = 0.0
        self._weekly_vol_sum: float = 0.0
        self.daily_vwap = None
        self.weekly_vwap = None

        self._current_day_start = None   # طابع زمني لبداية اليوم الحالي (ms)
        self._current_week_start = None

        # --- PDH / PDL (من آخر شمعة يومية مغلقة) ---
        self.pdh = None   # Previous Day High
        self.pdl = None   # Previous Day Low

        # --- رصد الإغراء (Inducement) ---
        self._last_swing_low = None       # آخر قاع فرعي مرصود قبل PDL
        self.inducement_level = None       # مستوى الإغراء المعتمد حالياً

        # --- فلتر الاتجاه الأعلى (H4 عبر تجميع 4 شمعات ساعة + Daily) ---
        self._h1_candles_buffer = []   # نافذة متحركة من شمعات الساعة لبناء H4
        self.h4_bias = MarketBias.NEUTRAL
        self.daily_bias = MarketBias.NEUTRAL

        # إعادة الاتصال (Reconnection Backoff)
        self._reconnect_delay = 2
        self._max_reconnect_delay = 30

    # ------------------------------------------------------------------
    # 1) معالجة شمعات الساعة (1h) -> بناء H4 Bias
    # ------------------------------------------------------------------
    def _process_1h_kline(self, kline: dict) -> None:
        """كل شمعة ساعة مغلقة تُضاف لنافذة من 4 شمعات لبناء تحيّز H4 يدوياً (بلا EMA/SMA)."""
        if not kline["x"]:  # x = True فقط عند إغلاق الشمعة رسمياً
            return

        close_price = float(kline["c"])
        open_price = float(kline["o"])
        self._h1_candles_buffer.append({"open": open_price, "close": close_price})

        # نحتفظ فقط بآخر 4 شمعات ساعة (= إطار H4)
        if len(self._h1_candles_buffer) > 4:
            self._h1_candles_buffer.pop(0)

        if len(self._h1_candles_buffer) == 4:
            h4_open = self._h1_candles_buffer[0]["open"]
            h4_close = self._h1_candles_buffer[-1]["close"]
            self.h4_bias = self._determine_bias(h4_open, h4_close)
            logger.info(f"تحديث H4 Bias: {self.h4_bias.value} (فتح: {h4_open:.2f} | إغلاق حالي: {h4_close:.2f})")

    @staticmethod
    def _determine_bias(open_price: float, close_price: float) -> MarketBias:
        """
        منطق هيكلي بسيط بدل المؤشرات الكلاسيكية: مقارنة سعر الفتح بالإغلاق للإطار.
        (سيُستبدل لاحقاً بمنطق هيكل أعلى/أدنى (HH/HL أو LH/LL) بعد اختبار هذا الأساس).
        """
        change_pct = (close_price - open_price) / open_price * 100
        if change_pct > 0.15:
            return MarketBias.BULLISH
        elif change_pct < -0.15:
            return MarketBias.BEARISH
        return MarketBias.NEUTRAL

    # ------------------------------------------------------------------
    # 2) معالجة الشمعة اليومية (1d) -> PDH/PDL + Daily VWAP + Daily Bias
    # ------------------------------------------------------------------
    def _process_1d_kline(self, kline: dict) -> None:
        open_time = kline["t"]

        # عند بداية يوم جديد: تصفير عدّادات الـ VWAP اليومي
        if self._current_day_start != open_time:
            self._current_day_start = open_time
            self._daily_pv_sum = 0.0
            self._daily_vol_sum = 0.0
            logger.info("بداية يوم تداول جديد - تصفير VWAP اليومي.")

        # تحديث الـ VWAP اليومي التراكمي بالسعر النموذجي (High+Low+Close)/3
        typical_price = (float(kline["h"]) + float(kline["l"]) + float(kline["c"])) / 3
        volume = float(kline["v"])
        self._daily_pv_sum += typical_price * volume
        self._daily_vol_sum += volume
        if self._daily_vol_sum > 0:
            self.daily_vwap = self._daily_pv_sum / self._daily_vol_sum

        # عند إغلاق الشمعة اليومية رسمياً: اعتماد PDH/PDL والـ Daily Bias لليوم التالي
        if kline["x"]:
            self.pdh = float(kline["h"])
            self.pdl = float(kline["l"])
            self.daily_bias = self._determine_bias(float(kline["o"]), float(kline["c"]))
            logger.info(
                f"إغلاق يومي: PDH={self.pdh:.2f} | PDL={self.pdl:.2f} | "
                f"Daily Bias: {self.daily_bias.value}"
            )
            # إعادة ضبط رصد الإغراء لليوم الجديد
            self._last_swing_low = None
            self.inducement_level = None

    # ------------------------------------------------------------------
    # 3) معالجة الشمعة الأسبوعية (1w) -> Weekly VWAP
    # ------------------------------------------------------------------
    def _process_1w_kline(self, kline: dict) -> None:
        open_time = kline["t"]

        if self._current_week_start != open_time:
            self._current_week_start = open_time
            self._weekly_pv_sum = 0.0
            self._weekly_vol_sum = 0.0
            logger.info("بداية أسبوع تداول جديد - تصفير VWAP الأسبوعي.")

        typical_price = (float(kline["h"]) + float(kline["l"]) + float(kline["c"])) / 3
        volume = float(kline["v"])
        self._weekly_pv_sum += typical_price * volume
        self._weekly_vol_sum += volume
        if self._weekly_vol_sum > 0:
            self.weekly_vwap = self._weekly_pv_sum / self._weekly_vol_sum

    # ------------------------------------------------------------------
    # 4) رصد قاع الإغراء (Inducement) - يُستدعى من الوحدة الثالثة لاحقاً بسعر لحظي
    # ------------------------------------------------------------------
    def update_inducement_watch(self, current_low: float) -> None:
        """
        منطق أولي لرصد الإغراء: إذا تشكّل قاع أعلى من PDL لكن أقل من آخر قاع مسجل،
        نعتبره "قاع فرعي" مرشّح ليكون خط الإغراء. سيُطوَّر أكثر في الوحدة الثالثة
        بربطه مع حركة السعر اللحظية الكاملة بدل قيمة واحدة.
        """
        if self.pdl is None:
            return
        if current_low > self.pdl and (self._last_swing_low is None or current_low < self._last_swing_low):
            self._last_swing_low = current_low
            self.inducement_level = current_low
            logger.info(f"رصد قاع إغراء محتمل عند: {current_low:.2f} (فوق PDL: {self.pdl:.2f})")

    def get_context_snapshot(self) -> dict:
        """لقطة كاملة من حالة الخريطة الحالية - ستستهلكها الوحدة الثالثة (Trigger Logic)."""
        return {
            "daily_vwap": self.daily_vwap,
            "weekly_vwap": self.weekly_vwap,
            "pdh": self.pdh,
            "pdl": self.pdl,
            "inducement_level": self.inducement_level,
            "h4_bias": self.h4_bias,
            "daily_bias": self.daily_bias,
        }

    def _print_context(self) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        logger.info(
            f"[{timestamp}] الخريطة الحالية | Daily VWAP: {self._fmt(self.daily_vwap)} | "
            f"Weekly VWAP: {self._fmt(self.weekly_vwap)} | PDH: {self._fmt(self.pdh)} | "
            f"PDL: {self._fmt(self.pdl)} | H4 Bias: {self.h4_bias.value} | "
            f"Daily Bias: {self.daily_bias.value}"
        )

    @staticmethod
    def _fmt(value) -> str:
        return f"{value:.2f}" if value is not None else "غير متوفر بعد"

    # ------------------------------------------------------------------
    # المعالجة الرئيسية لكل رسالة واردة من الـ Combined Stream
    # ------------------------------------------------------------------
    def _route_message(self, message: dict) -> None:
        stream = message.get("stream", "")
        payload = message.get("data", {})
        kline = payload.get("k")
        if kline is None:
            return

        if "@kline_1h" in stream:
            self._process_1h_kline(kline)
        elif "@kline_1d" in stream:
            self._process_1d_kline(kline)
        elif "@kline_1w" in stream:
            self._process_1w_kline(kline)

        self._print_context()

    async def _listen(self) -> None:
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            logger.info(f"تم الاتصال بنجاح بمحرك الخريطة: {self.ws_url}")
            self._reconnect_delay = 2
            async for raw_message in ws:
                try:
                    message = json.loads(raw_message)
                    self._route_message(message)
                except (KeyError, ValueError) as parse_error:
                    logger.warning(f"تجاهل رسالة غير صالحة: {parse_error}")

    async def run(self) -> None:
        """حلقة رئيسية محمية بـ try/except وإعادة اتصال أسّية، وفق القواعد الصارمة للمشروع."""
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
    engine = ContextEngine(symbol="btcusdt")
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
