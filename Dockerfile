FROM ubuntu:20.04 as builder
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
# Few components have to be installed separately:
# 1. CMake: build-essential, cmake
# 2. Arrow: https://arrow.apache.org/install/
# 3. Boost, if you need the REST server, which we don't :)
RUN apt update && apt install -y sudo git

COPY cmake/arrow.sh arrow.sh
RUN chmod +x arrow.sh && ./arrow.sh
RUN pip install cmake

COPY . /usr/src/ukv
WORKDIR /usr/src/ukv
RUN cmake \
    -DUKV_BUILD_PYTHON=0 \
    -DUKV_BUILD_TESTS=0 \
    -DUKV_BUILD_BENCHMARKS=0 \
    -DUKV_BUILD_FLIGHT_API=1 . && \
    make -j32 \
    ukv_umem_flight_server
    # ukv_leveldb_flight_server \
    # ukv_rocksdb_flight_server

## TODO: Optimize: https://github.com/docker-slim/docker-slim
FROM ubuntu:20.04
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"

WORKDIR /root/
COPY cmake/arrow.sh arrow.sh
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
RUN apt update && apt install -y sudo build-essential
RUN chmod +x arrow.sh && DEBIAN_FRONTEND=noninteractive ./arrow.sh
COPY --from=builder /usr/src/ukv/build/bin/ukv_umem_flight_server ./
# COPY --from=builder /usr/src/ukv/build/bin/ukv_leveldb_flight_server ./
# COPY --from=builder /usr/src/ukv/build/bin/ukv_rocksdb_flight_server ./
EXPOSE 38709
CMD ["./ukv_umem_flight_server"]
