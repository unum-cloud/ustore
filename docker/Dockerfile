FROM ubuntu:focal as builder
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
# Few components have to be installed separately:
# 1. CMake: build-essential, cmake
# 2. Arrow: https://arrow.apache.org/install/
# 3. Boost, if you need the REST server, which we don't :)
RUN apt update && apt install -y sudo git

RUN apt update && apt install -y -V libssl-dev python3 python3-pip
RUN pip install cmake

COPY . /usr/src/ukv
WORKDIR /usr/src/ukv
RUN cmake \
    -DUKV_BUILD_SDK_PYTHON=0 \
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
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

RUN apt update && apt install -y -V libssl-dev 

WORKDIR /root/
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_umem ./umem_server
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_leveldb ./leveldb_server
COPY --from=builder /usr/src/ukv/build/bin/ukv_flight_server_rocksdb ./rocksdb_server
EXPOSE 38709
CMD ["./umem_server"]
