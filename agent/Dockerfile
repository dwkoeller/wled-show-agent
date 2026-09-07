FROM node:24-bookworm-slim AS codex
ARG CODEX_VERSION=0.153.4
RUN npm install -g @openai/codex@${CODEX_VERSION} \
 && mkdir /codex-runtime \
 && find /usr/local/lib/node_modules -type f -path '*/bin/codex*' -exec cp {} /codex-runtime/ \; \
 && test -x /codex-runtime/codex

FROM python:3.14-slim

COPY --from=codex /codex-runtime/ /usr/local/bin/

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Minimal deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8088
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8088"]
