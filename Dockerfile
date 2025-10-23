FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY audio_splitter.py video_merger.py handler.py ./

ENV PYTHONUNBUFFERED=1 \
    FFMPEG_PATH="ffmpeg" \
    FFPROBE_PATH="ffprobe"

# (opcional) usuario no root
RUN useradd -m appuser
USER appuser

# Healthcheck simple
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ffmpeg -version >/dev/null 2>&1 || exit 1

# ¡Listo para RunPod Serverless!
CMD ["python", "handler.py"]