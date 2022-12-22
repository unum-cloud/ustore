FROM ubuntu:focal as builder
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"

RUN ln -s /usr/bin/dpkg-split /usr/sbin/dpkg-split && \
    ln -s /usr/bin/dpkg-deb /usr/sbin/dpkg-deb && \
    ln -s /bin/rm /usr/sbin/rm && \
    ln -s /bin/tar /usr/sbin/tar && \
    ln -s /bin/as /usr/sbin/as

RUN apt-get update -y && \
    apt-get install -y apt-utils 2>&1 | grep -v "debconf: delaying package configuration, since apt-utils is not installed" && \
    apt-get install -y --no-install-recommends cmake git libssl-dev build-essential zlib1g zlib1g-dev

COPY . /usr/src/ukv
WORKDIR /usr/src/ukv

RUN git config --global http.sslVerify "false"
RUN cmake \
    -DUKV_BUILD_TESTS=0 \
    -DUKV_BUILD_BENCHMARKS=0 \
    -DUKV_BUILD_ENGINE_UMEM=1 \
    -DUKV_BUILD_ENGINE_LEVELDB=1 \
    -DUKV_BUILD_ENGINE_ROCKSDB=1 \
    -DUKV_BUILD_API_FLIGHT_CLIENT=0 \
    -DUKV_BUILD_API_FLIGHT_SERVER=1 . && \
    make -j32 \
        ukv_flight_server_umem \
        ukv_flight_server_leveldb \
        ukv_flight_server_rocksdb

## TODO: Optimize: https://github.com/docker-slim/docker-slim
FROM ubuntu:focal
WORKDIR /root/
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_umem ./umem_server
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_leveldb ./leveldb_server
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_rocksdb ./rocksdb_server
EXPOSE 38709
CMD ["./umem_server"]
