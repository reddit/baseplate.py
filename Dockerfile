FROM ghcr.io/reddit/thrift-compiler:0.19.0 AS thrift

FROM public.ecr.aws/docker/library/python:3.13

# This is needed for pendulum due to no wheel: https://github.com/sdispater/pendulum/issues/844
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

COPY --from=thrift /usr/local/bin/thrift /usr/local/bin/thrift

WORKDIR /src

RUN python -m venv /tmp/poetry && \
    /tmp/poetry/bin/pip install poetry==1.8.3 && \
    ln -s /tmp/poetry/bin/poetry /usr/local/bin/poetry

COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --all-extras

CMD ["/bin/bash"]
