# Dockerfile cho Railway
FROM python:3.13-slim-bookworm

# Cài FFmpeg và libopus từ apt (Debian base)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libopus0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy và install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code bot
COPY . .

# Start bot
CMD ["python", "bot.py"]