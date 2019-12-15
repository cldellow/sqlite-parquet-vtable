FROM ubuntu:16.04
RUN apt-get update && apt-get install -y apt-transport-https curl gnupg lsb-release pkg-config sqlite3 libsqlite3-dev libicu-dev gcc g++ meson
COPY ci/arrow-ubuntu16.04.list /etc/apt/sources.list.d/apache-arrow.list
RUN curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | apt-key add - && apt-get update && apt-get install -y libparquet-dev
WORKDIR /src
COPY . /src
RUN mkdir builddir && meson builddir
WORKDIR builddir
RUN meson configure -Db_pgo=generate && ninja
RUN ninja test
RUN meson configure -Db_pgo=use && ninja
