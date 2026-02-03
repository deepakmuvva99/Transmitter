# Sonicster Transmitter Module

This container captures 5-second audio/vibration samples,
buffers them locally, and transmits them securely to the
receiver module using gRPC over mTLS.

## Responsibilities
- Continuous audio capture
- Disk-backed buffering
- Reliable gRPC transmission with ACK
- Error handling and retries

## Security
- Certificates are injected at runtime
- Private keys are NOT stored in source control

## Runtime Directories
- buffer/ : main send buffer (mounted volume)
- error_buffer/ : failed transmissions
