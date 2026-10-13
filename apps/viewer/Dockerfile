# ビューアー（Cloud Run、#13 / #52）。ビルド context は apps/viewer（auth.txt・functions/・appengine/ は context の外）:
#   docker build -t viewer:local apps/viewer
# context に入るものは apps/viewer/.dockerignore（許可リスト方式）で限定する
ARG NODE_IMAGE=node:24-slim@sha256:d6aa754f16b3197301076f047b5def2f02ea1dbbc2ca920407d46d7ec7f87b20
ARG PYTHON_IMAGE=python:3.13-slim@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f

# ブラウザ用ライブラリ: package-lock.json の integrity で検証して入れ、UMD のファイルだけを static/vendor/ にコピーする（CDN は使わない）
FROM ${NODE_IMAGE} AS vendor
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund && npm run vendor

FROM ${PYTHON_IMAGE} AS build
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN pip install --no-cache-dir uv==0.12.19
WORKDIR /src
# 依存だけ先に入れて層をキャッシュする（uv.lock のハッシュで検証）
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY --from=vendor /src/src/optionchan_viewer/static/vendor ./src/optionchan_viewer/static/vendor
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080
WORKDIR /app
USER 10001
# GCP_PROJECT（必須）・BQ_DATASET・BQ_LOCATION は実行時の環境変数で渡す。I/O 待ちが主なのでスレッドで並べる
CMD ["sh", "-c", "exec gunicorn --bind :${PORT} --workers 1 --threads 8 --timeout 60 --no-control-socket optionchan_viewer.app:app"]
