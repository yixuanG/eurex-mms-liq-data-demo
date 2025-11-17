"""
Multi-level order book reconstruction for Eurex DI events.

Supports L1, L3, L5, L10 or full depth tracking.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
import pandas as pd


UPDATE_ACTIONS = {0, 1, 5}
DELETE_ACTION = 2


@dataclass
class MultiLevelState:
    """State for multi-level order book."""
    bids: Dict[int, Tuple[float, int]] = field(default_factory=dict)  # level -> (price, size)
    asks: Dict[int, Tuple[float, int]] = field(default_factory=dict)  # level -> (price, size)
    ts_ns: Optional[int] = None
    
    def get_best_bid(self) -> Optional[Tuple[float, int]]:
        """Get best bid (highest price)."""
        if not self.bids:
            return None
        best_level = min(self.bids.keys())  # Level 0 should be best
        return self.bids[best_level]
    
    def get_best_ask(self) -> Optional[Tuple[float, int]]:
        """Get best ask (lowest price).""" 
        if not self.asks:
            return None
        best_level = min(self.asks.keys())  # Level 0 should be best
        return self.asks[best_level]
    
    def get_levels_snapshot(self, max_levels: int = 5) -> Dict[str, object]:
        """Get snapshot of top N levels."""
        snapshot = {
            "ts_ns": self.ts_ns,
            "bids": [],
            "asks": []
        }
        
        # Sort levels and take top N
        bid_levels = sorted(self.bids.keys())[:max_levels]
        ask_levels = sorted(self.asks.keys())[:max_levels]
        
        for level in bid_levels:
            price, size = self.bids[level]
            snapshot["bids"].append({"level": level, "price": price, "size": size})
            
        for level in ask_levels:
            price, size = self.asks[level]
            snapshot["asks"].append({"level": level, "price": price, "size": size})
            
        return snapshot


class MultiLevelBook:
    """Multi-level order book tracker."""
    
    def __init__(self, max_levels: int = 10):
        self.state = MultiLevelState()
        self.max_levels = max_levels
        
    def apply_event(self, e: Dict[str, object]) -> bool:
        """Apply a DI event. Return True if book changed."""
        et = e.get("entry_type")
        level = e.get("price_level") 
        act = e.get("md_update_action")
        price = e.get("price")
        size = e.get("size")
        ts_ns = e.get("ts_ns")
        
        if et not in (0, 1) or level is None or level > self.max_levels:
            return False
            
        changed = False
        
        if act in UPDATE_ACTIONS:
            if et == 0:  # bid
                old_entry = self.state.bids.get(level)
                if price is not None and size is not None:
                    new_entry = (float(price), int(size))
                    if old_entry != new_entry:
                        self.state.bids[level] = new_entry
                        changed = True
            else:  # ask
                old_entry = self.state.asks.get(level)
                if price is not None and size is not None:
                    new_entry = (float(price), int(size))
                    if old_entry != new_entry:
                        self.state.asks[level] = new_entry
                        changed = True
                        
        elif act == DELETE_ACTION:
            if et == 0 and level in self.state.bids:
                del self.state.bids[level]
                changed = True
            elif et == 1 and level in self.state.asks:
                del self.state.asks[level] 
                changed = True
                
        # Update timestamp
        if ts_ns is not None:
            self.state.ts_ns = int(ts_ns)
            
        return changed
        
    def snapshot_l1(self) -> Dict[str, object]:
        """Get L1 snapshot compatible with existing format."""
        best_bid = self.state.get_best_bid()
        best_ask = self.state.get_best_ask()
        
        return {
            "ts_ns": self.state.ts_ns,
            "best_bid": best_bid[0] if best_bid else None,
            "bid_size": best_bid[1] if best_bid else None,
            "best_ask": best_ask[0] if best_ask else None,
            "ask_size": best_ask[1] if best_ask else None,
        }
        
    def snapshot_l5(self) -> Dict[str, object]:
        """Get L5 snapshot with top 5 levels."""
        snapshot = self.state.get_levels_snapshot(max_levels=5)
        
        # Flatten for easy analysis
        result = {"ts_ns": snapshot["ts_ns"]}
        
        # Add L1-L5 bid/ask prices and sizes
        for i in range(5):
            bid_key_price = f"bid_price_{i+1}" 
            bid_key_size = f"bid_size_{i+1}"
            ask_key_price = f"ask_price_{i+1}"
            ask_key_size = f"ask_size_{i+1}"
            
            if i < len(snapshot["bids"]):
                result[bid_key_price] = snapshot["bids"][i]["price"]
                result[bid_key_size] = snapshot["bids"][i]["size"]
            else:
                result[bid_key_price] = None
                result[bid_key_size] = None
                
            if i < len(snapshot["asks"]):
                result[ask_key_price] = snapshot["asks"][i]["price"] 
                result[ask_key_size] = snapshot["asks"][i]["size"]
            else:
                result[ask_key_price] = None
                result[ask_key_size] = None
                
        return result
