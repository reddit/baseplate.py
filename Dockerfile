FROM ghcr.io/reddit/thrift-compiler:0.19.0 AS thrift

FROM public.ecr.aws/docker/library/python:3.13

# TODO(ckuehl|2024-09-26): Remove this once Python 3.13 wheels are available for our dependencies.
# https://github.com/confluentinc/confluent-kafka-python/blob/master/INSTALL.md#install-from-source-on-debian-or-ubuntu
COPY confluent-archive.key /usr/local/share/keyrings/
RUN echo 'deb [signed-by=/usr/local/share/keyrings/confluent-archive.key] https://packages.confluent.io/clients/deb bookworm main' > /etc/apt/sources.list.d/confluent.list

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install \
        build-essential \
        curl \
        krb5-user \
        libffi-dev \
        libpq-dev \
        librdkafka-dev \
        libsasl2-modules-gssapi-mit \
    && rm -rf /var/lib/apt/lists/*

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
