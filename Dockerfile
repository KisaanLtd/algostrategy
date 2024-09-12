# Use the official Python image from Docker Hub
ARG PYTHON_VERSION=3.10.0
FROM python:${PYTHON_VERSION}-slim as base

# Install dependencies for building TA-Lib and cleaning up unnecessary files
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential wget \
    # curl \
    # libssl-dev \
    # libffi-dev \
    # python3-dev \
    && rm -rf /var/lib/apt/lists/*

### Download, extract, compile, and install TA-Lib C++ library
RUN wget https://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \

    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && rm ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib/ \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib/

### Install the Python wrapper for TA-Lib
RUN pip install --no-cache-dir ta-lib

### Set environment variables to avoid writing .pyc files and to ensure Python outputs everything to stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

### Set the working directory
WORKDIR /app

### Copy requirements.txt before copying the rest of the application code to leverage Docker's caching mechanism
COPY requirements.txt .

### Install Python dependencies with cache mount for faster builds
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r requirements.txt

### Copy the rest of the application code to the Docker image
COPY . .

### Create a script to run multiple Python scripts
RUN echo "#!/bin/bash\n\
python scripts/datasampling.py &\n\
python scripts/tvdata_update.py &\n\
python scripts/indicator_update.py &\n\
wait" > /app/start.sh \
    && chmod +x /app/start.sh

### Use the created script as the entry point
CMD ["bash", "start.sh"]