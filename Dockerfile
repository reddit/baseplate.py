FROM ghcr.io/reddit/thrift-compiler:0.14.1 AS thrift

FROM python:3.8

COPY --from=thrift /usr/local/bin/thrift /usr/local/bin/thrift

WORKDIR /src

COPY requirements*.txt ./
RUN pip install -r requirements.txt

RUN touch /baseplate-py-dev-docker-image

CMD ["/bin/bash"]
