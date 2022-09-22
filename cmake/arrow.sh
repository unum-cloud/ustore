# Suggestions: https://arrow.apache.org/install/
# https://anaconda.org/fastchan/arrow-cpp/files?sort=basename&version=9.0.0&sort_order=desc
# https://anaconda.org/fastchan/pyarrow/files?sort=basename&version=9.0.0&sort_order=desc
# conda install pyarrow=9.0.0=py39h42d110c_0_cpu arrow-cpp=9.0.0=py39h811ffd7_0_cpu -c fastchan
# conda install arrow-cpp=9.0.0=py39h811ffd7_0_cpu -c fastchan

sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev \
    libarrow-dataset-dev \
    libarrow-flight-dev \
    libarrow-python-dev \
    libarrow-python-flight-dev \
    libplasma-dev \
    libgandiva-dev \
    libparquet-dev
