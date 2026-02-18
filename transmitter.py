import os
import time
import json
import threading
import grpc
import sounddevice as sd
import numpy as np
import soundfile as sf

from scipy.signal import resample_poly
from datetime import datetime, date

import audio_pb2
import audio_pb2_grpc

# =========================================================
# CONFIG
# =========================================================
DEVICE_ID = "raspi-01"
MACHINE_ID = "fan-unit-01"

CAPTURE_RATE = 48000
TARGET_RATE = 16000
WINDOW_SEC = 5
CHANNELS = 1

RECEIVER_ADDR = os.getenv("RECEIVER_ADDR", "192.168.1.5:50051")

MAX_RETRIES = 2
SEND_TIMEOUT = 5

BASE_DIR = os.path.expanduser("~/data")
BUFFER_AUDIO = f"{BASE_DIR}/buffer/audio"
BUFFER_META = f"{BASE_DIR}/buffer/meta"
ERROR_AUDIO = f"{BASE_DIR}/error_buffer/audio"
ERROR_META = f"{BASE_DIR}/error_buffer/meta"

for d in [BUFFER_AUDIO, BUFFER_META, ERROR_AUDIO, ERROR_META]:
    os.makedirs(d, exist_ok=True)

print("=== TRANSMITTER STARTED (DISK FIFO MODE) ===", flush=True)

# =========================================================
# TLS
# =========================================================
with open("certs/ca.pem", "rb") as f:
    ca = f.read()
with open("certs/device.pem", "rb") as f:
    cert = f.read()
with open("certs/device.key", "rb") as f:
    key = f.read()

creds = grpc.ssl_channel_credentials(
    root_certificates=ca,
    private_key=key,
    certificate_chain=cert
)

channel = grpc.secure_channel(RECEIVER_ADDR, creds)
stub = audio_pb2_grpc.AudioIngestorStub(channel)

# =========================================================
# SEQUENCE MANAGER (THREAD SAFE)
# =========================================================
class SeqManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.date = date.today().strftime("%Y%m%d")
        self.counter = 0

    def next(self):
        with self.lock:
            today = date.today().strftime("%Y%m%d")
            if today != self.date:
                self.date = today
                self.counter = 0
            self.counter += 1
            return f"{DEVICE_ID}-{today}-{self.counter:08d}"

seq_mgr = SeqManager()

# =========================================================
# ATOMIC JSON WRITE
# =========================================================
def atomic_json_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.rename(tmp, path)

# =========================================================
# RECORDER THREAD (PRODUCER)
# =========================================================
def recorder():
    while True:
        try:
            print("[REC] Recording 5s", flush=True)

            audio = sd.rec(
                int(CAPTURE_RATE * WINDOW_SEC),
                samplerate=CAPTURE_RATE,
                channels=CHANNELS,
                dtype="int16"
            )
            sd.wait()

            audio_f = audio.astype(np.float32) / 32768.0
            audio_16k = resample_poly(audio_f, TARGET_RATE, CAPTURE_RATE)

            seq_id = seq_mgr.next()
            created = datetime.utcnow().isoformat()

            wav_path = f"{BUFFER_AUDIO}/{seq_id}.wav"
            meta_path = f"{BUFFER_META}/{seq_id}.json"

            sf.write(wav_path, audio_16k.squeeze(), TARGET_RATE, subtype="PCM_16")

            meta = {
                "device_id": DEVICE_ID,
                "machine_id": MACHINE_ID,
                "seq_id": seq_id,
                "wav_file": wav_path,
                "created_utc": created,
                "status": "CREATED",
                "retries": 0
            }

            atomic_json_write(meta_path, meta)

            print(f"[REC] Stored {seq_id}", flush=True)

        except Exception as e:
            print(f"[REC][ERROR] {e}", flush=True)
            time.sleep(2)

# =========================================================
# SENDER THREAD (CONSUMER, FIFO)
# =========================================================
def sender():
    while True:
        metas = sorted(os.listdir(BUFFER_META))

        for meta_file in metas:
            meta_path = f"{BUFFER_META}/{meta_file}"

            try:
                with open(meta_path) as f:
                    meta = json.load(f)
            except json.JSONDecodeError:
                print(f"[SEND][SKIP] Partial meta {meta_file}", flush=True)
                continue

            if meta["status"] != "CREATED":
                continue

            seq_id = meta["seq_id"]
            wav_path = meta["wav_file"]

            for attempt in range(1, MAX_RETRIES + 2):
                try:
                    print(f"[SEND] {seq_id} attempt {attempt}", flush=True)

                    with open(wav_path, "rb") as f:
                        wav_bytes = f.read()

                    msg = audio_pb2.AudioChunk(
                        device_id=DEVICE_ID,
                        timestamp_ms=int(time.time() * 1000),
                        sample_rate=TARGET_RATE,
                        wav_data=wav_bytes
                    )

                    ack = stub.SendAudio(msg, timeout=SEND_TIMEOUT)

                    if ack.success:
                        meta["status"] = "SENT"
                        atomic_json_write(meta_path, meta)
                        print(f"[SEND] ACK {seq_id}", flush=True)
                        break

                except Exception as e:
                    meta["retries"] += 1
                    print(f"[SEND][FAIL] {seq_id} {e}", flush=True)

            if meta["retries"] > MAX_RETRIES:
                meta["status"] = "FAILED"
                os.rename(wav_path, f"{ERROR_AUDIO}/{seq_id}.wav")
                os.rename(meta_path, f"{ERROR_META}/{seq_id}.json")
                print(f"[ERROR] {seq_id} → error_buffer", flush=True)

        time.sleep(1)

# =========================================================
# START THREADS
# =========================================================
threading.Thread(target=recorder, daemon=True).start()
threading.Thread(target=sender, daemon=True).start()

while True:
    time.sleep(60)
