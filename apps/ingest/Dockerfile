# 取り込みサービス（Cloud Run）。ビルド context はリポジトリ直下（schema/tokyo/ を同梱するため）:
#   docker build -f apps/ingest/Dockerfile -t ingest:local .
# context に入るものは .dockerignore（許可リスト方式）で限定する。auth.txt・functions/ 等は入らない
ARG PYTHON_IMAGE=python:3.13-slim@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f

FROM ${PYTHON_IMAGE} AS build
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install --no-cache-dir uv==0.12.19
WORKDIR /src/apps/ingest
# 依存だけ先に入れて層をキャッシュする（uv.lock のハッシュで検証）
COPY apps/ingest/pyproject.toml apps/ingest/uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY apps/ingest/src ./src
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /opt/venv /opt/venv
COPY schema/tokyo/ /app/schema/tokyo/
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080
WORKDIR /app
USER 10001
# 同時実行 1（Cloud Run 側も 1）。アプリの期限は 45 秒（settings.REQUEST_DEADLINE_SECONDS）、gunicorn の worker タイムアウト 50 秒は止まったときの最後の砦
CMD ["sh", "-c", "exec gunicorn --bind :${PORT} --workers 1 --threads 1 --timeout 50 --no-control-socket optionchan_ingest.app:app"]
