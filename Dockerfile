FROM docker.io/library/python:3.10

COPY --from=ghcr.io/reddit/thrift-compiler:0.14.1 /usr/local/bin/thrift /usr/local/bin/thrift

RUN apt-get update && \
    apt-get install -y librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# cassandra-driver doesn't seem to publish pre-compiled wheels for py39 so we
# have to wait for some hefty native extensions to build. skip this because we
# don't care for our tests.
ENV CASS_DRIVER_NO_EXTENSIONS theytaketoolongtobuild

COPY requirements*.txt ./
RUN pip install -r requirements.txt

RUN touch /baseplate-py-dev-docker-image

CMD ["/bin/bash"]
