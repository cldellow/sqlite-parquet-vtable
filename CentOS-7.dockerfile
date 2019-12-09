FROM centos:7
RUN yum install -y epel-release
RUN yum install -y sqlite-devel libicu-devel gcc gcc-c++ meson
COPY ci/arrow-centos.repo /etc/yum.repos.d/Apache-Arrow.repo
RUN yum install -y parquet-devel
WORKDIR /src
COPY . /src
RUN mkdir builddir && meson builddir && cd builddir && ninja-build
