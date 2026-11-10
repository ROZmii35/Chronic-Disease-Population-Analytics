
import pandas as pd
import numpy as np
import os
import json
import time
from collections import Counter

# ==========================================
# 1. CONFIGURATION
# ==========================================

INPUT_FILE = (
    "data/synthetic/encounters/"
    "encounters_synthetic.csv"
)

OUTPUT_DIR = "data/synthetic/patient_events"

OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "patient_events.jsonl"
)

REPORT_DIR = "reports"

TARGET_GB = 1.0
TARGET_BYTES = int(TARGET_GB * 1_000_000_000)

CHUNK_SIZE = 100_000
EVENT_PROBABILITY = 0.40
RANDOM_SEED = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ==========================================
# 2. EVENT CONFIGURATION
# ==========================================

EVENT_TYPES = [
    "admission_registered",
    "laboratory_activity",
    "medication_review",
    "discharge_registered",
    "followup_scheduled"
]

EVENT_WEIGHTS = [
    0.25,
    0.25,
    0.20,
    0.20,
    0.10
]

# Simulated timestamp range
START_TIMESTAMP = pd.Timestamp(
    "2026-01-01T00:00:00Z"
).timestamp()

END_TIMESTAMP = pd.Timestamp(
    "2026-11-10T23:59:59Z"
).timestamp()

rng = np.random.default_rng(RANDOM_SEED)

# ==========================================
# 3. INITIALIZATION
# ==========================================

print("=" * 65)
print("PATIENT EVENTS JSON GENERATOR")
print("CHRONIC DISEASE POPULATION ANALYTICS")
print("=" * 65)

print("\nTarget size:", TARGET_GB, "GB")
print("Chunk size:", CHUNK_SIZE)
print("Event probability:", EVENT_PROBABILITY)

if not os.path.exists(INPUT_FILE):
    raise FileNotFoundError(INPUT_FILE)

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

start_time = time.time()

total_scanned = 0
total_events = 0
total_bytes = 0
batch_number = 0

event_counter = Counter()

# ==========================================
# 4. BATCH PROCESSING
# ==========================================

print("\nStarting batch processing...\n")

with open(
    OUTPUT_FILE,
    "w",
    encoding="utf-8",
    newline="\n"
) as output:

    reader = pd.read_csv(
        INPUT_FILE,
        usecols=[
            "patient_nbr",
            "encounter_id"
        ],
        dtype={
            "patient_nbr": "string",
            "encounter_id": "string"
        },
        chunksize=CHUNK_SIZE
    )

    for chunk in reader:

        batch_number += 1
        total_scanned += len(chunk)

        # Select a subset of encounters
        selected_mask = (
            rng.random(len(chunk))
            < EVENT_PROBABILITY
        )

        selected = chunk.loc[
            selected_mask
        ].reset_index(drop=True)

        if selected.empty:
            continue

        event_count = len(selected)

        # Generate event types
        event_types = rng.choice(
            EVENT_TYPES,
            size=event_count,
            p=EVENT_WEIGHTS
        )

        # Generate simulated timestamps
        timestamps = rng.integers(
            int(START_TIMESTAMP),
            int(END_TIMESTAMP),
            size=event_count
        )

        event_timestamps = (
            pd.to_datetime(
                timestamps,
                unit="s",
                utc=True
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        # Generate unique event IDs
        event_ids = [
            f"EVT{x:012d}"
            for x in range(
                total_events + 1,
                total_events + event_count + 1
            )
        ]

        # Build event DataFrame
        event_df = pd.DataFrame({
            "event_id": event_ids,
            "patient_nbr": selected["patient_nbr"],
            "encounter_id": selected["encounter_id"],
            "event_type": event_types,
            "event_timestamp": event_timestamps,
            "source": "synthetic_simulation",
            "synthetic_flag": True
        })

        # Serialize as JSON Lines
        json_lines = event_df.to_json(
            orient="records",
            lines=True,
            force_ascii=False
        )

        batch_bytes = len(
            json_lines.encode("utf-8")
        )

        remaining_bytes = (
            TARGET_BYTES - total_bytes
        )

        # Write complete batch if it fits
        if batch_bytes <= remaining_bytes:

            output.write(json_lines)

            total_bytes += batch_bytes
            total_events += event_count

            event_counter.update(
                event_df["event_type"].tolist()
            )

        else:

            accepted_lines = []
            accepted_bytes = 0

            for index, line in enumerate(
                json_lines.splitlines(keepends=True)
            ):

                line_bytes = len(
                    line.encode("utf-8")
                )

                if (
                    total_bytes
                    + accepted_bytes
                    + line_bytes
                    > TARGET_BYTES
                ):
                    break

                accepted_lines.append(line)
                accepted_bytes += line_bytes

                event_counter.update([
                    event_types[index]
                ])

            accepted_text = "".join(
                accepted_lines
            )

            output.write(accepted_text)

            written_events = len(
                accepted_lines
            )

            total_events += written_events
            total_bytes += accepted_bytes

            break

        if batch_number % 10 == 0:

            print(
                f"Batch {batch_number} | "
                f"Scanned: {total_scanned:,} | "
                f"Events: {total_events:,} | "
                f"Size: {total_bytes / 1_000_000:.2f} MB"
            )

# ==========================================
# 5. GENERATION REPORT
# ==========================================

elapsed_time = time.time() - start_time

actual_size_gb = (
    total_bytes / 1_000_000_000
)

report = {
    "dataset": "Chronic Disease Population Analytics",
    "source": "Synthetic Encounters CSV",
    "format": "JSON Lines (NDJSON)",
    "generation_method": "Batch Event Simulation",
    "random_seed": RANDOM_SEED,
    "event_probability": EVENT_PROBABILITY,
    "synthetic_records": int(total_events),
    "scanned_encounters": int(total_scanned),
    "target_size_gb": TARGET_GB,
    "actual_size_gb": round(actual_size_gb, 6),
    "file_size_bytes": int(total_bytes),
    "generation_time_seconds": round(
        elapsed_time, 2
    ),
    "event_distribution": dict(event_counter),
    "timestamp_type": "Simulated UTC",
    "synthetic_flag": True
}

REPORT_FILE = (
    f"{REPORT_DIR}/patient_events_generation_report.json"
)

with open(REPORT_FILE, "w") as f:
    json.dump(report, f, indent=4)

# ==========================================
# 6. FINAL OUTPUT
# ==========================================

print("\n" + "=" * 65)
print("PATIENT EVENTS GENERATION COMPLETED")
print("=" * 65)

print("Scanned encounters:", f"{total_scanned:,}")
print("Generated events:", f"{total_events:,}")
print("File size:", round(actual_size_gb, 4), "GB")
print("Generation time:", round(elapsed_time, 2), "seconds")

print("\nEvent distribution:")

for event, count in event_counter.items():
    print(f"{event}: {count:,}")

print("\nOutput:", OUTPUT_FILE)
print("Report:", REPORT_FILE)

if total_bytes < TARGET_BYTES:
    print("\nWARNING: Source exhausted before target size.")
