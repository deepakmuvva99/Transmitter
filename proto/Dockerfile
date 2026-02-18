FROM python:3.9-slim

# =========================================================
# System dependencies (Audio + Build tools)
# =========================================================
RUN apt-get update && apt-get install -y \
    alsa-utils \
    libasound2 \
    libasound2-dev \
    portaudio19-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# =========================================================
# App setup
# =========================================================
WORKDIR /app

# -------------------------------
# Python deps
# -------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir grpcio-tools

# -------------------------------
# Copy source (including proto)
# -------------------------------
COPY . .

# =========================================================
# Generate gRPC Python files from proto
# =========================================================
RUN python -m grpc_tools.protoc \
    -I proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/audio.proto

# =========================================================
# Runtime
# =========================================================
CMD ["python", "transmitter.py"]
