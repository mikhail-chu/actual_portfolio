FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-cache

COPY . .

ENV PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_ENABLECORS=false \
    STREAMLIT_SERVER_ENABLEXSRS=false \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

EXPOSE 8501

CMD ["uv", "run", "streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]