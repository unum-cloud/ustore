FROM ubuntu:20.04 as builder
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
# Few components have to be installed separately:
# 1. CMake: build-essential, cmake
# 2. Arrow: https://arrow.apache.org/install/
# 3. Boost, if you need the REST server, which we don't :)
RUN apt update && apt install -y sudo git

ENV arrow_version=9.0.0-1
RUN apt update && apt install -y -V ca-certificates lsb-release wget && \
    cd /tmp && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt update && apt install -y -V libarrow-dev=${arrow_version} libarrow-dataset-dev=${arrow_version} libarrow-flight-dev=${arrow_version} \
    libarrow-python-dev=${arrow_version} libarrow-python-flight-dev=${arrow_version} libgandiva-dev=${arrow_version} libparquet-dev=${arrow_version} \
    python3 python3-pip
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
    -DUKV_BUILD_API_FLIGHT=1 . && \
    make -j32 \
    ukv_umem_flight_server \
    ukv_leveldb_flight_server \
    ukv_rocksdb_flight_server

## TODO: Optimize: https://github.com/docker-slim/docker-slim
FROM ubuntu:20.04
ENV DEBIAN_FRONTEND="noninteractive" TZ="Europe/London"
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

ENV arrow_version=9.0.0-1
RUN apt update && apt install -y -V ca-certificates lsb-release wget && \
    cd /tmp && wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt update && apt install -y -V libarrow-dev=${arrow_version} libarrow-flight-dev=${arrow_version}

WORKDIR /root/
COPY --from=builder /usr/src/ukv/build/bin/ukv_umem_flight_server ./
COPY --from=builder /usr/src/ukv/build/bin/ukv_leveldb_flight_server ./
COPY --from=builder /usr/src/ukv/build/bin/ukv_rocksdb_flight_server ./
EXPOSE 38709
CMD ["./ukv_umem_flight_server"]
