FROM thrift:0.12 AS thrift

FROM python:3.8

COPY --from=thrift /usr/local/bin/thrift /usr/local/bin/thrift

WORKDIR /src

COPY requirements*.txt ./
RUN pip install -r requirements.txt

CMD ["/bin/bash"]
