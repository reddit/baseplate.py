# syntax=docker/dockerfile:1
FROM ghcr.io/reddit/thrift-compiler:0.19.0 AS thrift

FROM python:3.12

COPY --from=thrift /usr/local/bin/thrift /usr/local/bin/thrift

WORKDIR /src

RUN --mount=type=cache,target=/root/.cache \
    python -m venv /tmp/poetry && \
    /tmp/poetry/bin/pip install poetry==1.8.2 && \
    ln -s /tmp/poetry/bin/poetry /usr/local/bin/poetry

COPY pyproject.toml poetry.lock ./
RUN poetry install --all-extras

CMD ["/bin/bash"]
