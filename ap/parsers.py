# ap/parsers.py
import re
from dataclasses import dataclass
from typing import Optional, List, Dict

@dataclass
class ParsedLeg:
    direction: str               # "CALL" or "PUT"
    entry: Optional[float]
    stop: Optional[float]
    pt1: Optional[float]
    pt2: Optional[float]
    pt3: Optional[float]
    strike: Optional[float]
    expiry_hint: Optional[str]   # "Weekly" or "0DTE" or None
    raw_strike_line: Optional[str]

@dataclass
class ParsedScannerMessage:
    symbol: str
    current: Optional[float]
    scanned_at: Optional[str]
    calls: Optional[ParsedLeg]
    puts: Optional[ParsedLeg]
    source: str = "discord_scanner"

_float = r"(\d+(?:\.\d+)?)"

def _to_float(x: Optional[str]) -> Optional[float]:
    if not x: return None
    try: return float(x)
    except: return None

def parse_scanner_text(text: str) -> List[ParsedScannerMessage]:
    """
    Parses messages like:
    📌 PEP (Current: $147.66)
       CALLS SETUP
       Entry: $147.66
       PT1: $148.98 | PT2: $149.78 | PT3: $150.37
       Stop Loss: $145.99
       Strike: PEP 148 calls Weekly
       PUTS SETUP ...
    Returns one or more ParsedScannerMessage (some messages contain 4 setups).
    """
    blocks = re.split(r"─{5,}", text)
    out: List[ParsedScannerMessage] = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # scanned timestamp
        m_ts = re.search(r"Scanned at\s+([0-9:\-\s]+)", block)
        scanned_at = m_ts.group(1).strip() if m_ts else None

        # find each "📌 SYMBOL (Current: $X)" group
        # If a single block includes multiple symbols, split them
        parts = re.split(r"(?=📌\s+[A-Z]{1,6}\s+\(Current:)", block)
        for part in parts:
            part = part.strip()
            if not part.startswith("📌"):
                continue

            m_head = re.search(r"📌\s+([A-Z]{1,6})\s+\(Current:\s*\$?"+_float+r"\)", part)
            if not m_head:
                continue
            symbol = m_head.group(1)
            current = _to_float(m_head.group(2))

            calls = _parse_leg(part, "CALLS")
            puts = _parse_leg(part, "PUTS")

            out.append(ParsedScannerMessage(
                symbol=symbol,
                current=current,
                scanned_at=scanned_at,
                calls=calls,
                puts=puts,
            ))
    return out

def _parse_leg(part: str, leg_label: str) -> Optional[ParsedLeg]:
    # isolate the section starting at "{CALLS|PUTS} SETUP" until next leg or end
    m_start = re.search(rf"{leg_label}\s+SETUP", part)
    if not m_start:
        return None
    start = m_start.start()

    # end at the other leg setup or end
    other = "PUTS" if leg_label == "CALLS" else "CALLS"
    m_end = re.search(rf"{other}\s+SETUP", part[start+1:])
    end = start + 1 + m_end.start() if m_end else len(part)

    seg = part[start:end]

    # entry
    m_entry = re.search(r"Entry:\s*\$?"+_float, seg)
    entry = _to_float(m_entry.group(1)) if m_entry else None

    # stop
    m_stop = re.search(r"Stop Loss:\s*\$?"+_float, seg)
    stop = _to_float(m_stop.group(1)) if m_stop else None

    # targets can be "PT1/PT2/PT3" or single "Target"
    pt1 = pt2 = pt3 = None
    m_pts = re.search(r"PT1:\s*\$?"+_float+r"\s*\|\s*PT2:\s*(?:\$?"+_float+r"|N/A)\s*\|\s*PT3:\s*(?:\$?"+_float+r"|N/A)", seg)
    if m_pts:
        pt1 = _to_float(m_pts.group(1))
        pt2 = _to_float(m_pts.group(2))
        pt3 = _to_float(m_pts.group(3))
    else:
        m_t = re.search(r"Target:\s*\$?"+_float, seg)
        if m_t:
            pt1 = _to_float(m_t.group(1))

    # strike line
    m_strike_line = re.search(r"Strike:\s*(.+)", seg)
    raw_strike_line = m_strike_line.group(1).strip() if m_strike_line else None

    strike = None
    expiry_hint = None
    if raw_strike_line:
        # Examples: "PEP 148 calls Weekly" or "NFLX 90 calls Weekly"
        m_strike = re.search(r"\b([A-Z]{1,6})\s+(\d+(?:\.\d+)?)\s+(calls|puts)\s+(Weekly|0DTE)\b", raw_strike_line, re.IGNORECASE)
        if m_strike:
            strike = _to_float(m_strike.group(2))
            expiry_hint = m_strike.group(4)

    return ParsedLeg(
        direction="CALL" if leg_label == "CALLS" else "PUT",
        entry=entry,
        stop=stop,
        pt1=pt1,
        pt2=pt2,
        pt3=pt3,
        strike=strike,
        expiry_hint=expiry_hint,
        raw_strike_line=raw_strike_line,
    )
