FROM ghcr.io/reddit/thrift-compiler:0.19.0 AS thrift

FROM python:3.12

COPY --from=thrift /usr/local/bin/thrift /usr/local/bin/thrift

WORKDIR /src

COPY requirements*.txt ./
RUN apt-get -y update && apt-get -y install librdkafka-dev && rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements.txt

RUN touch /baseplate-py-dev-docker-image

CMD ["/bin/bash"]
