"""
Minimal L1 order book reconstruction for Eurex DI events.

We only track best bid/ask (price_level==0) to keep the first step simple and
transparent. Actions handled: New(0), Change(1), Delete(2), Overlay(5).

This module is intentionally lightweight to support the first 1s aggregation
prototype; later steps can extend it to full L1-L5 depth and DS reconciliation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


UPDATE_ACTIONS = {0, 1, 5}
DELETE_ACTION = 2


@dataclass
class L1State:
    best_bid: Optional[float] = None
    bid_size: Optional[int] = None
    best_ask: Optional[float] = None
    ask_size: Optional[int] = None
    ts_ns: Optional[int] = None


class L1Book:
    def __init__(self) -> None:
        self.state = L1State()

    def apply_event(self, e: Dict[str, object]) -> bool:
        """Apply a DI event. Return True if L1 changed, False otherwise.

        Expected keys in e: entry_type (0/1), price_level (int), price (float),
        size (int), md_update_action (int), ts_ns (int).
        """
        et = e.get("entry_type")
        lvl = e.get("price_level")
        act = e.get("md_update_action")
        price = e.get("price")
        size = e.get("size")
        ts_ns = e.get("ts_ns")

        changed = False

        if et not in (0, 1):
            # Unknown side: ignore for L1 tracking
            return False
        if lvl is None or lvl > 5:
            # Too deep or invalid level: ignore
            return False

        if act in UPDATE_ACTIONS:
            if et == 0:  # bid
                old_bid = self.state.best_bid
                old_bid_size = self.state.bid_size
                if price is not None:
                    self.state.best_bid = float(price)
                if size is not None:
                    self.state.bid_size = int(size)
                if self.state.best_bid != old_bid or self.state.bid_size != old_bid_size:
                    changed = True
            else:  # ask
                old_ask = self.state.best_ask
                old_ask_size = self.state.ask_size
                if price is not None:
                    self.state.best_ask = float(price)
                if size is not None:
                    self.state.ask_size = int(size)
                if self.state.best_ask != old_ask or self.state.ask_size != old_ask_size:
                    changed = True

        elif act == DELETE_ACTION:
            if et == 0:  # bid delete
                if self.state.bid_size not in (None, 0):
                    self.state.bid_size = 0
                    changed = True
            else:  # ask delete
                if self.state.ask_size not in (None, 0):
                    self.state.ask_size = 0
                    changed = True

        # Update last timestamp regardless
        if ts_ns is not None:
            self.state.ts_ns = int(ts_ns)

        return changed

    def snapshot(self, action: Optional[int] = None) -> Dict[str, object]:
        return {
            "ts_ns": self.state.ts_ns,
            "best_bid": self.state.best_bid,
            "bid_size": self.state.bid_size,
            "best_ask": self.state.best_ask,
            "ask_size": self.state.ask_size,
            "action": action,
        }

