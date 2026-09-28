"""
=====================================================================
الوحدة الأولى (Module 1): محرك البصمة والدلتا التراكمية (Footprint & CVD Engine)
=====================================================================
المصدر: Binance USDT-M Futures WebSocket - Stream: @aggTrade (بيانات عامة، لا تحتاج حساب أو API Key)
الهدف:
    1. الاتصال المستمر بالسوق عبر asyncio + websockets (بلا مؤشرات كلاسيكية).
    2. قراءة متغير m (is_buyer_maker) لتحديد الطرف المُنفِّذ (Aggressor):
         m == False -> أمر شراء ماركت ضغط فعلي من المشتري (Buy Aggressive)
         m == True  -> أمر بيع ماركت ضغط فعلي من البائع (Sell Aggressive)
    3. حساب الدلتا التراكمية (CVD) لحظياً.
    4. عرض عدد وحجم المشترين/البائعين اللحظي (Live Buyers/Sellers Counter).
    5. إعادة الاتصال التلقائي عند انقطاع الشبكة (Reconnection) وفق القواعد الصارمة للمشروع.

ملاحظة: هذه الوحدة لا ترسل أي تنبيه ولا تنفذ أي صفقة - فقط تجهّز البيانات الخام
للوحدات القادمة (Context Engine / Trigger Logic / Alerting Engine).
=====================================================================
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

# إعداد سجلّ الأحداث (Logging) بدل استخدام print فقط - ممارسة احترافية
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("FootprintCVDEngine")


class FootprintCVDEngine:
    """
    محرك البصمة (Order Flow Footprint) والدلتا التراكمية (CVD) لرمز تداول واحد.
    مبني بطريقة كائنية التوجه (OOP) ليسهل ربطه لاحقاً بالوحدة الثانية (Context Engine).
    """

    def __init__(self, symbol: str = "btcusdt"):
        self.symbol = symbol.lower()
        # رابط Binance Futures العام لبث الصفقات المجمّعة (aggTrade) - مجاني بالكامل
        self.ws_url = f"wss://fstream.binance.com/ws/{self.symbol}@aggTrade"

        # --- متغيرات حالة البصمة (Footprint State) ---
        self.cumulative_delta: float = 0.0   # الدلتا التراكمية (CVD)
        self.buy_volume: float = 0.0          # حجم الشراء العدواني اللحظي (نافذة حالية)
        self.sell_volume: float = 0.0         # حجم البيع العدواني اللحظي (نافذة حالية)
        self.buy_count: int = 0               # عدد صفقات الشراء اللحظية
        self.sell_count: int = 0              # عدد صفقات البيع اللحظية

        # إعادة الاتصال (Reconnection Backoff)
        self._reconnect_delay: int = 2
        self._max_reconnect_delay: int = 30

    def _process_trade(self, data: dict) -> None:
        """
        معالجة كل صفقة واردة من aggTrade وتحديث حالة البصمة والـ CVD.
        هذه هي نقطة الحقيقة الوحيدة لمعرفة من يضغط زر الماركت (m).
        """
        price = float(data["p"])
        quantity = float(data["q"])
        is_buyer_maker = data["m"]  # المتغير m كما ورد في وثيقة الاستراتيجية

        if is_buyer_maker:
            # m == True -> المشتري هو صانع السيولة => البائع هو من نفّذ بالماركت (بيع عدواني)
            self.sell_volume += quantity
            self.sell_count += 1
            self.cumulative_delta -= quantity
            side = "SELL (عدواني)"
        else:
            # m == False -> البائع هو صانع السيولة => المشتري هو من نفّذ بالماركت (شراء عدواني)
            self.buy_volume += quantity
            self.buy_count += 1
            self.cumulative_delta += quantity
            side = "BUY  (عدواني)"

        self._print_live_footprint(price, quantity, side)

    def _print_live_footprint(self, price: float, quantity: float, side: str) -> None:
        """عرض لحظي لعدّاد المشترين/البائعين، الحجم، والـ CVD التراكمي."""
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        logger.info(
            f"[{timestamp}] {self.symbol.upper()} | {side} | السعر: {price:.2f} | "
            f"الكمية: {quantity:.4f} || "
            f"عدد المشترين: {self.buy_count} (حجم {self.buy_volume:.2f}) | "
            f"عدد البائعين: {self.sell_count} (حجم {self.sell_volume:.2f}) || "
            f"CVD التراكمي: {self.cumulative_delta:.4f}"
        )

    async def _listen(self) -> None:
        """الاتصال بالـ WebSocket والاستماع المستمر للصفقات."""
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            logger.info(f"تم الاتصال بنجاح بـ {self.ws_url}")
            self._reconnect_delay = 2  # إعادة ضبط زمن الانتظار بعد اتصال ناجح
            async for message in ws:
                try:
                    data = json.loads(message)
                    self._process_trade(data)
                except (KeyError, ValueError) as parse_error:
                    logger.warning(f"تجاهل رسالة غير صالحة: {parse_error}")

    async def run(self) -> None:
        """
        الحلقة الرئيسية مع حماية try/except وإعادة اتصال تلقائي أسّية (Exponential Backoff)
        كما تنص القواعد الصارمة للمشروع (البند 5).
        """
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
    engine = FootprintCVDEngine(symbol="btcusdt")
    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
