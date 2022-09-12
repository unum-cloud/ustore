FROM gcc:12

# Two components need to be installed separately:
# 1. Boost
# 2. Arrow: https://arrow.apache.org/install/
RUN apt-get update && \
    apt-get install -y -V ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y -V --fix-missing \
    libboost-all-dev \
    libarrow-dev \
    libarrow-dataset-dev \
    libarrow-flight-dev \
    libparquet-dev

COPY . /usr/src/ukv
WORKDIR /usr/src/ukv
RUN cmake . && make -j16

## TODO: Optimize: https://github.com/docker-slim/docker-slim
FROM ubuntu:22.04
WORKDIR /root/
COPY --from=0 /usr/src/ukv/build/bin/ukv_stl_flight_server ./
CMD ["./ukv_stl_flight_server"]
