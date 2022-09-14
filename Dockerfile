FROM gcc:12 as builder

# Few components have to be installed separately:
# 1. CMake: build-essential, cmake
# 2. Arrow: https://arrow.apache.org/install/
# 3. Boost, if you need the REST server, which we don't :)
RUN apt-get update && \
    apt-get install -y -V ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y -V --fix-missing \
    build-essential \
    cmake \
    libarrow-dev \
    libarrow-flight-dev 

COPY . /usr/src/ukv
WORKDIR /usr/src/ukv
RUN cmake \
    -DUKV_BUILD_PYTHON=0 \
    -DUKV_BUILD_TESTS=0 \
    -DUKV_BUILD_BENCHMARKS=0 \
    -DUKV_BUILD_FLIGHT_RPC=1 . && \
    make -j16 \
    ukv_stl_flight_server \
    ukv_leveldb_flight_server \
    ukv_rocksdb_flight_server

## TODO: Optimize: https://github.com/docker-slim/docker-slim
FROM ubuntu:22.04
WORKDIR /root/
COPY --from=builder /usr/src/ukv/build/bin/ukv_stl_flight_server ./
COPY --from=builder /usr/src/ukv/build/bin/ukv_leveldb_flight_server ./
COPY --from=builder /usr/src/ukv/build/bin/ukv_rocksdb_flight_server ./
EXPOSE 38709
CMD ["./ukv_stl_flight_server"]
