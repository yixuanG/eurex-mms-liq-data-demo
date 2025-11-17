"""
Lightweight parsing utilities for Eurex high-frequency CSV-like lines.

This module focuses on DI (Depth Incremental) inspection first, providing
helpers to extract entry tokens from lines and to infer a minimal mapping for
key fields required for L1 order book reconstruction.

It intentionally keeps assumptions minimal and data-driven:
- We search for top-level {...} blocks and split by commas preserving empties.
- We infer likely indices for fields such as md_update_action, entry_type,
  price_level, security_id, price, size, ts_ns based on simple heuristics from
  a sample of lines.

Next steps (in later commits): add robust DS parsing and an event parser that
uses the inferred mapping to emit structured DI events.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple


# Regex to capture the content of top-level {...} blocks inside a DI message
_BRACE_RE = re.compile(r"\{([^{}]*)\}")


def extract_entry_tokens_from_di_line(line: str) -> List[List[str]]:
    """Extract token lists for each entry {...} from a DI line.

    Strategy: find all top-level {...} blocks using balanced brace parsing and 
    split their inner content by commas, preserving empties. Whitespace is stripped around tokens.
    """
    entries: List[List[str]] = []
    i = 0
    while i < len(line):
        if line[i] == '{':
            start = i + 1
            brace_count = 1
            i += 1
            while i < len(line) and brace_count > 0:
                if line[i] == '{':
                    brace_count += 1
                elif line[i] == '}':
                    brace_count -= 1
                i += 1
            if brace_count == 0:
                inner = line[start:i-1]
                # Split by comma preserving empties; strip whitespace
                raw_tokens = inner.split(",")
                tokens = [t.strip() for t in raw_tokens]
                entries.append(tokens)
        else:
            i += 1
    return entries


def _is_int_like(s: str) -> bool:
    if s == "":
        return False
    if s.startswith("-"):
        return s[1:].isdigit()
    return s.isdigit()


def _is_big_ns_int(s: str) -> bool:
    # 16–20 digit integers typically represent nanosecond timestamps
    return _is_int_like(s) and 16 <= len(s.lstrip("-")) <= 20


def _is_float_like(s: str) -> bool:
    if s in ("", "."):
        return False
    try:
        float(s)
        return True
    except Exception:
        return False


@dataclass
class DiMapping:
    md_update_action_idx: int
    entry_type_idx: int
    price_level_idx: int
    security_id_idx: int
    price_idx: int
    size_idx: int
    ts_ns_idx: int


def infer_di_mapping(lines: Iterable[str], sample_limit: int = 200) -> Optional[DiMapping]:
    """Infer a minimal DI mapping from a sample of lines.

    Optimized for Eurex DI format with pattern: {md_action,price_level,entry_type,security_id,M,price,size,...,ts_ns,...}
    
    Heuristics:
    - ts_ns_idx: index with most 16–20 digit integers across entries
    - price_idx: index with frequent floats; prefer the first float after a literal 'M' if present
    - size_idx: index with frequent small positive ints after price
    - md_update_action_idx: typically index 0 for Eurex format
    - entry_type_idx: typically index 2 for Eurex format (0=bid, 1=ask)
    - price_level_idx: typically index 1 for Eurex format (depth level)
    - security_id_idx: index with mid-sized integers (e.g., 5–10 digits) relatively stable
    """
    samples: List[List[str]] = []
    for i, line in enumerate(lines):
        if i >= sample_limit:
            break
        entries = extract_entry_tokens_from_di_line(line)
        for e in entries:
            samples.append(e)

    if not samples:
        return None

    # Determine max token length among entries to size our stats
    max_len = max(len(e) for e in samples)
    if max_len == 0:
        return None

    # Stats per index
    big_ns_counts = [0] * max_len
    float_counts = [0] * max_len
    int_counts = [0] * max_len
    small_int_counts = [0] * max_len  # 0..10
    literal_M_counts = [0] * max_len

    for e in samples:
        for idx in range(max_len):
            val = e[idx] if idx < len(e) else ""
            if val == "M":
                literal_M_counts[idx] += 1
            if _is_big_ns_int(val):
                big_ns_counts[idx] += 1
            if _is_float_like(val):
                float_counts[idx] += 1
            if _is_int_like(val):
                int_counts[idx] += 1
                try:
                    v = int(val)
                    if 0 <= v <= 10:
                        small_int_counts[idx] += 1
                except Exception:
                    pass

    # Timestamp index: max big_ns_counts
    ts_ns_idx = max(range(max_len), key=lambda i: big_ns_counts[i])

    # Find preferred 'M' position if any (venue/source literal)
    M_idx = max(range(max_len), key=lambda i: literal_M_counts[i]) if any(literal_M_counts) else None

    # Price: prefer first float index after M_idx if present, else global max
    def _first_float_after(start_idx: int) -> int:
        for j in range(start_idx + 1, max_len):
            if float_counts[j] > 0:
                return j
        return max(range(max_len), key=lambda i: float_counts[i])

    price_idx = _first_float_after(M_idx) if M_idx is not None else max(range(max_len), key=lambda i: float_counts[i])

    # Size: first int index after price_idx with many ints
    def _first_int_after(start_idx: int) -> int:
        for j in range(start_idx + 1, max_len):
            if int_counts[j] > 0:
                return j
        return max(range(max_len), key=lambda i: int_counts[i])

    size_idx = _first_int_after(price_idx)

    # For Eurex format, use known pattern positions as defaults, then verify
    md_update_action_idx = 0
    price_level_idx = 1  
    entry_type_idx = 2
    
    # Verify entry_type_idx: should have values alternating between 0 and 1
    if max_len > 2:
        entry_type_values = []
        for e in samples[:20]:  # Check first 20 samples
            if 2 < len(e):
                try:
                    val = int(e[2])
                    if val in (0, 1):
                        entry_type_values.append(val)
                except:
                    pass
        
        # If position 2 doesn't look like entry_type, fall back to heuristic
        if len(entry_type_values) < 5 or len(set(entry_type_values)) < 2:
            # Fall back to original heuristic
            ranking = sorted([(small_int_counts[i], i) for i in range(max_len)], reverse=True)
            candidate_small = [i for _, i in ranking if i not in (price_idx, size_idx, ts_ns_idx)]
            entry_type_idx = candidate_small[0] if candidate_small else 1
            md_update_action_idx = candidate_small[1] if len(candidate_small) > 1 else 0
            remaining = [i for i in candidate_small if i not in (entry_type_idx, md_update_action_idx)]
            price_level_idx = remaining[0] if remaining else 1

    # security_id: among int_counts, pick a column with many ints that is not small_int-dominated and not ts
    candidates_sec = []
    for i in range(max_len):
        if i in (price_idx, size_idx, ts_ns_idx, entry_type_idx, md_update_action_idx, price_level_idx):
            continue
        if int_counts[i] > 0 and big_ns_counts[i] == 0:
            candidates_sec.append((int_counts[i] - small_int_counts[i], i))
    security_id_idx = max(candidates_sec)[1] if candidates_sec else 3

    return DiMapping(
        md_update_action_idx=md_update_action_idx,
        entry_type_idx=entry_type_idx,
        price_level_idx=price_level_idx,
        security_id_idx=security_id_idx,
        price_idx=price_idx,
        size_idx=size_idx,
        ts_ns_idx=ts_ns_idx,
    )


def tokens_to_event(tokens: List[str], m: DiMapping) -> Dict[str, object]:
    """Convert a token list into a minimally structured DI event using mapping m.

    Note: values are kept as best-effort types (int/float) where applicable.
    """
    def _get(i: int) -> str:
        return tokens[i] if i < len(tokens) else ""

    def _to_int(s: str) -> Optional[int]:
        try:
            return int(s)
        except Exception:
            return None

    def _to_float(s: str) -> Optional[float]:
        try:
            return float(s)
        except Exception:
            return None

    return {
        "md_update_action": _to_int(_get(m.md_update_action_idx)),
        "entry_type": _to_int(_get(m.entry_type_idx)),  # 0=bid, 1=ask
        "price_level": _to_int(_get(m.price_level_idx)),
        "security_id": _to_int(_get(m.security_id_idx)),
        "price": _to_float(_get(m.price_idx)),
        "size": _to_int(_get(m.size_idx)),
        "ts_ns": _to_int(_get(m.ts_ns_idx)),
    }

