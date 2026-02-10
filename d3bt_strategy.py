#!/usr/bin/env python3
"""
STRAT-034: D3 Bull Trap Short
Based on quant research: 85.11% WR, +0.65R EV, PF 19.45 (47 Polygon-validated trades)

Strategy Overview:
- Multi-day SHORT (3-day chain):
  D1: gap >= 30% + closes GREEN
  D2: closes RED (reversal day)
  D3: gaps up above VWAP -> bull trap -> SHORT
- Entry SHORT at 9:31 on D3 (OPEN-KNOWABLE)
- Stop: max(PM_High * 1.05, entry * 1.15) -- 15% max risk
- Take profit: entry * 0.80 (20% drop)
- EOD exit at 15:55 ET
- Sub-signal sizing:
  - D3 gap 10-20%: FULL (100% WR -- sweet spot, 15 trades)
  - D3 gap 5-10%: REDUCED (71% WR)
  - D1 gap 100%+: FULL (90% WR)
- Complementary to S50: S50 shorts D2 GREEN, D3BT shorts after D2 RED
"""
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger('s034-strategy')


class SignalType(Enum):
    WATCH = "WATCH"
    ENTERED = "ENTERED"


class TradeStatus(Enum):
    ACTIVE = "ACTIVE"
    WIN = "WIN"
    LOSS = "LOSS"
    EXPIRED = "EXPIRED"


class SignalStrength(Enum):
    FULL = "FULL"
    REDUCED = "REDUCED"


@dataclass
class D3BTScreeningCriteria:
    """Screening criteria for D3 Bull Trap"""
    min_d1_gap_pct: float = 30.0
    min_price: float = 2.00
    max_price: float = 30.0
    min_pm_volume: int = 50_000


@dataclass
class D1Runner:
    """A stock that gapped >= 30% on D1 and closed GREEN"""
    ticker: str
    d1_date: str
    d1_gap_pct: float
    d1_open: float
    d1_close: float
    d1_high: float
    d1_low: float
    d1_volume: int = 0


@dataclass
class D2RedDay:
    """D2 confirmation: D1 runner that closed RED on D2"""
    ticker: str
    d1_runner: D1Runner
    d2_date: str
    d2_open: float
    d2_close: float
    d2_high: float
    d2_low: float
    d2_volume: int = 0


@dataclass
class D3BTCandidate:
    """D3 Bull Trap candidate: D1 GREEN -> D2 RED -> D3 gaps up above VWAP"""
    ticker: str
    current_price: float
    prev_close: float
    d3_gap_pct: float
    vwap: float
    vwap_premium_pct: float
    pm_high: float
    pm_low: float
    pm_volume: int
    d1_runner: D1Runner = None
    d2_red: D2RedDay = None

    signal_type: SignalType = field(default=SignalType.WATCH)
    signal_strength: SignalStrength = field(default=SignalStrength.FULL)
    score: float = 0.0
    last_update: Optional[datetime] = None

    entry_triggered: bool = False
    entry_price: float = 0.0
    entry_time: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            'ticker': self.ticker,
            'price': round(self.current_price, 4),
            'd3_gap_pct': round(self.d3_gap_pct, 1),
            'vwap': round(self.vwap, 4),
            'vwap_premium_pct': round(self.vwap_premium_pct, 1),
            'pm_high': round(self.pm_high, 4),
            'pm_volume': self.pm_volume,
            'd1_gap_pct': round(self.d1_runner.d1_gap_pct, 1) if self.d1_runner else 0,
            'd1_date': self.d1_runner.d1_date if self.d1_runner else '',
            'd2_date': self.d2_red.d2_date if self.d2_red else '',
            'signal_type': self.signal_type.value,
            'signal_strength': self.signal_strength.value,
            'score': round(self.score, 1),
        }


@dataclass
class ShortTrade:
    """Trade tracking for D3 Bull Trap SHORT"""
    ticker: str
    entry_price: float = 0.0
    stop_loss: float = 0.0
    target_price: float = 0.0
    pm_high: float = 0.0
    pm_low: float = 0.0
    d1_gap_pct: float = 0.0
    d3_gap_pct: float = 0.0
    vwap: float = 0.0
    vwap_premium_pct: float = 0.0
    signal_strength: str = "FULL"

    status: TradeStatus = field(default=TradeStatus.ACTIVE)
    high_price: float = 0.0
    low_price: float = 0.0
    exit_price: float = 0.0
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    pnl_pct: float = 0.0
    exit_reason: str = ""

    def set_entry(self, price: float) -> None:
        self.entry_price = price
        pm_stop = round(self.pm_high * 1.05, 4) if self.pm_high > 0 else price * 1.15
        entry_stop = round(price * 1.15, 4)
        self.stop_loss = max(pm_stop, entry_stop)
        self.target_price = round(price * 0.80, 4)  # 20% drop
        self.high_price = price
        self.low_price = price
        self.entry_time = datetime.now(timezone.utc)

    def update_price(self, current_price: float) -> Optional[TradeStatus]:
        if current_price > self.high_price:
            self.high_price = current_price
        if self.low_price == 0 or current_price < self.low_price:
            self.low_price = current_price

        if current_price >= self.stop_loss and self.stop_loss > 0:
            self._exit(current_price, TradeStatus.LOSS, "STOP_LOSS")
            return TradeStatus.LOSS

        if current_price <= self.target_price and self.target_price > 0:
            self._exit(current_price, TradeStatus.WIN, "TARGET_20PCT_DROP")
            return TradeStatus.WIN

        return None

    def _exit(self, price: float, status: TradeStatus, reason: str) -> None:
        self.status = status
        self.exit_price = price
        self.exit_time = datetime.now(timezone.utc)
        self.pnl_pct = ((self.entry_price - price) / self.entry_price) * 100 if self.entry_price > 0 else 0
        self.exit_reason = reason

    def expire(self, current_price: float = 0.0) -> None:
        if self.status == TradeStatus.ACTIVE:
            self.status = TradeStatus.EXPIRED
            self.exit_time = datetime.now(timezone.utc)
            self.exit_reason = "EOD_1555"
            if current_price > 0 and self.entry_price > 0:
                self.exit_price = current_price
                self.pnl_pct = ((self.entry_price - current_price) / self.entry_price) * 100
            elif self.low_price > 0 and self.entry_price > 0:
                self.exit_price = self.low_price
                self.pnl_pct = ((self.entry_price - self.low_price) / self.entry_price) * 100

    def r_multiple(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        risk = self.stop_loss - self.entry_price
        if risk <= 0:
            return 0.0
        pnl = self.entry_price - self.exit_price if self.exit_price > 0 else 0
        return round(pnl / risk, 2)

    def to_dict(self) -> dict:
        return {
            'ticker': self.ticker,
            'entry_price': round(self.entry_price, 4),
            'stop_loss': round(self.stop_loss, 4),
            'target_price': round(self.target_price, 4),
            'vwap': round(self.vwap, 4),
            'vwap_premium_pct': round(self.vwap_premium_pct, 1),
            'pm_high': round(self.pm_high, 4),
            'pm_low': round(self.pm_low, 4),
            'd1_gap_pct': round(self.d1_gap_pct, 2),
            'd3_gap_pct': round(self.d3_gap_pct, 2),
            'signal_strength': self.signal_strength,
            'status': self.status.value,
            'high_price': round(self.high_price, 4),
            'low_price': round(self.low_price, 4),
            'exit_price': round(self.exit_price, 4),
            'pnl_pct': round(self.pnl_pct, 2),
            'exit_reason': self.exit_reason,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ShortTrade':
        trade = cls(ticker=data['ticker'])
        trade.entry_price = data.get('entry_price', 0)
        trade.stop_loss = data.get('stop_loss', 0)
        trade.target_price = data.get('target_price', 0)
        trade.vwap = data.get('vwap', 0)
        trade.vwap_premium_pct = data.get('vwap_premium_pct', 0)
        trade.pm_high = data.get('pm_high', 0)
        trade.pm_low = data.get('pm_low', 0)
        trade.d1_gap_pct = data.get('d1_gap_pct', 0)
        trade.d3_gap_pct = data.get('d3_gap_pct', 0)
        trade.signal_strength = data.get('signal_strength', 'FULL')
        trade.status = TradeStatus(data.get('status', 'ACTIVE'))
        trade.high_price = data.get('high_price', 0)
        trade.low_price = data.get('low_price', 0)
        trade.exit_price = data.get('exit_price', 0)
        trade.pnl_pct = data.get('pnl_pct', 0)
        trade.exit_reason = data.get('exit_reason', '')
        if data.get('entry_time'):
            trade.entry_time = datetime.fromisoformat(data['entry_time'])
        if data.get('exit_time'):
            trade.exit_time = datetime.fromisoformat(data['exit_time'])
        return trade


class D3BTScreener:
    """Screens for D3 Bull Trap SHORT candidates"""

    def __init__(self, criteria: D3BTScreeningCriteria = None):
        self.criteria = criteria or D3BTScreeningCriteria()

    def find_d1_runners_query(self) -> str:
        """SQL query to find D1 GREEN runners from recent days"""
        return """
            SELECT ticker, date, gap_value, poly_open, poly_close,
                   poly_high, poly_low, volume
            FROM historical_gappers
            WHERE date >= (CURRENT_DATE - INTERVAL '10 days')
              AND gap_value >= 30
              AND close_red NOT IN ('true', 'red')
              AND poly_open > 0 AND poly_close > 0
              AND poly_close > poly_open
            ORDER BY date DESC, gap_value DESC
        """

    def find_d2_red_days_query(self, tickers: List[str], d1_dates: dict) -> str:
        """SQL query to find D2 RED days for D1 runners.
        d1_dates: {ticker: d1_date_str} mapping"""
        if not tickers:
            return ""
        ticker_list = ", ".join(f"'{t}'" for t in tickers)
        return f"""
            SELECT ticker, date, poly_open, poly_close, poly_high, poly_low, volume
            FROM historical_gappers
            WHERE ticker IN ({ticker_list})
              AND date >= (CURRENT_DATE - INTERVAL '10 days')
              AND close_red IN ('true', 'red')
              AND poly_open > 0 AND poly_close > 0
              AND poly_close < poly_open
            ORDER BY date DESC
        """

    def check_d3_signal(
        self, d2_red: D2RedDay, price: float, prev_close: float,
        vwap: float, pm_high: float, pm_low: float, pm_volume: int
    ) -> Tuple[bool, List[str], Optional[D3BTCandidate]]:
        """Check if D2 RED day qualifies for D3 Bull Trap SHORT entry"""
        reasons = []
        c = self.criteria

        if price < c.min_price or price > c.max_price:
            reasons.append(f"Price ${price:.2f} out of range")
            return (False, reasons, None)

        if pm_volume < c.min_pm_volume:
            reasons.append(f"PM Vol {pm_volume:,} below min {c.min_pm_volume:,}")
            return (False, reasons, None)

        if vwap <= 0:
            reasons.append("No VWAP data")
            return (False, reasons, None)

        vwap_premium_pct = ((price - vwap) / vwap) * 100
        if vwap_premium_pct <= 0:
            reasons.append(f"Price below VWAP ({vwap_premium_pct:.1f}%)")
            return (False, reasons, None)

        d3_gap_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        strength = self._determine_strength(d2_red.d1_runner.d1_gap_pct, d3_gap_pct)
        score = self._calculate_score(d2_red.d1_runner.d1_gap_pct, d3_gap_pct, vwap_premium_pct, pm_volume)

        candidate = D3BTCandidate(
            ticker=d2_red.ticker,
            current_price=price,
            prev_close=prev_close,
            d3_gap_pct=d3_gap_pct,
            vwap=vwap,
            vwap_premium_pct=vwap_premium_pct,
            pm_high=pm_high,
            pm_low=pm_low,
            pm_volume=pm_volume,
            d1_runner=d2_red.d1_runner,
            d2_red=d2_red,
            signal_strength=strength,
            score=score,
            last_update=datetime.now(timezone.utc)
        )

        logger.info(
            f"[SCREENER] {d2_red.ticker} D3 Bull Trap PASSED | "
            f"D1 gap:{d2_red.d1_runner.d1_gap_pct:.0f}% D3 gap:{d3_gap_pct:.1f}% "
            f"VWAP+{vwap_premium_pct:.1f}% Strength:{strength.value} Score:{score:.0f}"
        )
        return (True, [], candidate)

    def _determine_strength(self, d1_gap: float, d3_gap: float) -> SignalStrength:
        """Sub-signal sizing from Polygon validation"""
        # D3 gap 10-20%: FULL (100% WR -- sweet spot, 15 trades)
        # D3 gap 5-10%: REDUCED (71% WR)
        # D1 gap 100%+: bonus toward FULL (90% WR)
        if 10.0 <= d3_gap <= 20.0:
            return SignalStrength.FULL  # Sweet spot -- 100% WR
        elif d3_gap > 20.0 and d1_gap >= 100:
            return SignalStrength.FULL  # High conviction big runners
        else:
            return SignalStrength.REDUCED

    def _calculate_score(
        self, d1_gap: float, d3_gap: float, vwap_premium: float, pm_volume: int
    ) -> float:
        score = 0.0

        # D1 gap score (25pts max)
        if d1_gap >= 100:
            score += 25
        elif d1_gap >= 50:
            score += 20
        elif d1_gap >= 30:
            score += 15

        # D3 gap score (30pts max) -- most important for D3BT
        if 10.0 <= d3_gap <= 20.0:
            score += 30  # Sweet spot -- 100% WR
        elif d3_gap > 20.0:
            score += 25
        elif d3_gap >= 5.0:
            score += 15
        else:
            score += 10

        # VWAP premium score (25pts max)
        if vwap_premium > 10:
            score += 25
        elif vwap_premium > 5:
            score += 20
        elif vwap_premium > 0:
            score += 15

        # PM volume score (20pts max)
        if pm_volume >= 500_000:
            score += 20
        elif pm_volume >= 200_000:
            score += 15
        elif pm_volume >= 100_000:
            score += 10
        else:
            score += 5

        return score
