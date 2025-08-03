#!/usr/bin/env python3
"""
scada2protocol.py

Transforms Excel/CSV SCADA exports into JSON-serialized protocol messages
(T1.F01 telemetry, T1.F02 alarms, T2.F01 theoretical curves) based on a
vendor-specific mapping YAML.
"""

import pandas as pd
import yaml
import uuid
import json
from datetime import datetime
import pytz
from dateutil import parser
import argparse

# ─────── Helpers ───────

def load_mapping(path):
    """Load the YAML mapping file."""
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def parse_value(raw, cfg):
    """Parse a raw cell value according to the mapping config."""
    if pd.isna(raw):
        # blank → use default if provided
        return cfg.get('default')
    t = cfg['type']
    if t == 'string':
        return str(raw)
    if t == 'int':
        return int(raw)
    if t == 'float':
        return float(raw)
    if t == 'datetime':
        # parse flexibly, then localize/convert to UTC epoch ms
        dt = parser.parse(str(raw), dayfirst=True)
        tz = pytz.timezone(cfg.get('timezone', 'UTC'))
        if dt.tzinfo is None:
            dt = tz.localize(dt)
        return int(dt.astimezone(pytz.UTC).timestamp() * 1000)
    if t == 'duration':
        # format "H:MM:SS" or "H:MM:SS.SSS"
        parts = str(raw).split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = float(parts[2])
        total = hours*3600 + minutes*60 + seconds
        return total
    raise ValueError(f"Unsupported type '{t}' in mapping")

def make_header(stream, version):
    """Build the common header for each message."""
    s, f = stream.split('.')
    return {
        "stream": s,
        "function": f,
        "message_id": str(uuid.uuid4()),
        "protocol_version": version,
        "sent_time": int(datetime.utcnow().timestamp() * 1000)
    }

# ─────── Main Transform Function ───────

def transform(excel_path, mapping_path, output_path=None):
    """
    Read the mapping YAML, load the Excel/CSV, transform each row
    into one or more protocol messages, return list of messages.
    """
    m = load_mapping(mapping_path)
    df = pd.read_excel(excel_path, engine='openpyxl')
    messages = []
    stream = m['stream']
    version = m['version']

    for _, row in df.iterrows():
        # Telemetry: group by turbine_id, emit one message per turbine
        if stream == 'T1.F01':
            # collect values per turbine
            buckets = {}
            ts_epoch = None
            for col, cfg in m['fields'].items():
                raw = row.get(col)
                val = parse_value(raw, cfg)
                tag = cfg['tag']
                tid = cfg.get('turbine_id')
                # detect timestamp first
                if tag == 'timestamp':
                    ts_epoch = val
                # initialize bucket if needed
                if tid not in buckets:
                    buckets[tid] = {
                        "turbine_id": tid,
                        "timestamp": ts_epoch
                    }
                # assign parsed value
                buckets[tid][tag] = val
            # now emit one message per turbine
            for tid, payload in buckets.items():
                header = make_header(stream, version)
                messages.append({"header": header, "payload": payload})

        # Alarm/Event: one message per row
        elif stream == 'T1.F02':
            payload = {}
            for col, cfg in m['fields'].items():
                raw = row.get(col)
                val = parse_value(raw, cfg)
                payload[cfg['tag']] = val
            header = make_header(stream, version)
            messages.append({"header": header, "payload": payload})

        # Theoretical power curve: one message per row
        elif stream == 'T2.F01':
            payload = {}
            for col, cfg in m['fields'].items():
                raw = row.get(col)
                val = parse_value(raw, cfg)
                payload[cfg['tag']] = val
            header = make_header(stream, version)
            messages.append({"header": header, "payload": payload})

        else:
            raise ValueError(f"Unsupported stream '{stream}' in mapping")

    # optionally write to file
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)

    return messages

# ─────── CLI Entry Point ───────

def main():
    parser = argparse.ArgumentParser(
        description="Transform SCADA Excel/CSV → Protocol JSON messages"
    )
    parser.add_argument("mapping", help="Path to mapping YAML")
    parser.add_argument("excel",   help="Path to Excel (or CSV) SCADA export")
    parser.add_argument("-o", "--output", help="Write JSON array to this file")
    args = parser.parse_args()

    msgs = transform(args.excel, args.mapping, args.output)
    # print to stdout if no file specified
    if not args.output:
        for m in msgs:
            print(json.dumps(m, ensure_ascii=False))

if __name__ == "__main__":
    main()
