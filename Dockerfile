# ─────────────────────────────────────────────
#  Stage 1: Build TA-Lib from source
# ─────────────────────────────────────────────
FROM python:3.11-slim AS talib-builder

RUN apt-get update && apt-get install -y \
    wget build-essential gcc make \
    && rm -rf /var/lib/apt/lists/*

# Download and compile TA-Lib C library
RUN wget https://downloads.sourceforge.net/project/ta-lib/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr/local \
    && make \
    && make install \
    && ldconfig

# ─────────────────────────────────────────────
#  Stage 2: Final app image
# ─────────────────────────────────────────────
FROM python:3.11-slim

# Copy compiled TA-Lib from builder stage
COPY --from=talib-builder /usr/local/lib/libta_lib* /usr/local/lib/
COPY --from=talib-builder /usr/local/include/ta-lib /usr/local/include/ta-lib

RUN apt-get update && apt-get install -y \
    libgomp1 git \
    && ldconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Cloud Run uses PORT env variable (default 8080)
ENV PORT=8080

# Gunicorn serves Flask app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "main:app"]