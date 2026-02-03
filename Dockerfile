FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    libsndfile1 \
    portaudio19-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto and generate gRPC code
COPY proto ./proto
RUN python -m grpc_tools.protoc \
    -I proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/audio.proto

# Copy transmitter code + buffer dir
COPY transmitter.py .
COPY buffer ./buffer

CMD ["python", "transmitter.py"]
