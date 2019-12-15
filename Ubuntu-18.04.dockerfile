FROM ubuntu:18.04
RUN apt-get update && apt-get install -y apt-transport-https curl gnupg lsb-release pkg-config sqlite3 libsqlite3-dev libicu-dev gcc g++ meson
COPY ci/arrow-ubuntu18.04.list /etc/apt/sources.list.d/apache-arrow.list
RUN curl -L -o /usr/share/keyrings/apache-arrow-keyring.gpg https://dl.bintray.com/apache/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-keyring.gpg && apt-get update && apt-get install -y libparquet-dev
WORKDIR /src
COPY . /src
RUN mkdir builddir && meson builddir
WORKDIR builddir
RUN meson configure -Db_pgo=generate && ninja
RUN ninja test
RUN meson configure -Db_pgo=use && ninja
