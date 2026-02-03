import os
import time
import threading
import grpc
import sounddevice as sd
import numpy as np
import soundfile as sf
from scipy.signal import resample_poly
from queue import Queue
from datetime import datetime

import audio_pb2
import audio_pb2_grpc

# ---------------- CONFIG ----------------
DEVICE_ID = "raspi-01"
CAPTURE_RATE = 48000
TARGET_RATE = 16000
WINDOW_SEC = 5
CHANNELS = 1

RECEIVER_ADDR = os.getenv("RECEIVER_ADDR", "192.168.1.50:50051")

BUFFER_DIR = "/data/buffer"
os.makedirs(BUFFER_DIR, exist_ok=True)

SEND_RETRY_DELAY = 3
# ---------------------------------------

print("=== TRANSMITTER STARTED (PRODUCTION MODE) ===")

send_queue = Queue()

# ---------- TLS ----------
with open("/certs/ca.pem", "rb") as f:
    ca = f.read()
with open("/certs/device.pem", "rb") as f:
    cert = f.read()
with open("/certs/device.key", "rb") as f:
    key = f.read()

creds = grpc.ssl_channel_credentials(
    root_certificates=ca,
    private_key=key,
    certificate_chain=cert
)

channel = grpc.secure_channel(RECEIVER_ADDR, creds)
stub = audio_pb2_grpc.AudioIngestorStub(channel)


# ---------- Recorder Thread ----------
def recorder():
    while True:
        print("[REC] Recording 5s audio...")
        audio = sd.rec(
            int(CAPTURE_RATE * WINDOW_SEC),
            samplerate=CAPTURE_RATE,
            channels=CHANNELS,
            dtype="int16"
        )
        sd.wait()

        audio_f = audio.astype(np.float32) / 32768.0
        audio_16k = resample_poly(audio_f, TARGET_RATE, CAPTURE_RATE)

        ts = int(time.time())
        filename = f"{ts}.wav"
        path = os.path.join(BUFFER_DIR, filename)

        sf.write(
            path,
            audio_16k.squeeze(),
            TARGET_RATE,
            format="WAV",
            subtype="PCM_16"
        )

        print(f"[REC] Saved {path}")
        send_queue.put(path)


# ---------- Sender Thread ----------
def sender():
    while True:
        path = send_queue.get()
        try:
            with open(path, "rb") as f:
                wav_bytes = f.read()

            msg = audio_pb2.AudioChunk(
                device_id=DEVICE_ID,
                seq=int(os.path.basename(path).split(".")[0]),
                timestamp_ms=int(time.time() * 1000),
                sample_rate=TARGET_RATE,
                wav_data=wav_bytes
            )

            ack = stub.SendAudio(msg, timeout=5)

            if ack.success:
                print(f"[SEND] ACK seq={ack.seq}")
            else:
                print(f"[SEND] NACK seq={ack.seq}")

        except Exception as e:
            print(f"[SEND] Failed, retry later: {e}")
            time.sleep(SEND_RETRY_DELAY)
            send_queue.put(path)


# ---------- START ----------
threading.Thread(target=recorder, daemon=True).start()
threading.Thread(target=sender, daemon=True).start()

while True:
    time.sleep(60)
