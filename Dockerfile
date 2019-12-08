#FROM centos:7
#RUN yum install -y epel-release 
#RUN yum install -y sqlite-devel
#RUN yum install -y gcc gcc-c++ meson
#COPY ci/arrow-centos.repo /etc/yum.repos.d/Apache-Arrow.repo
#RUN yum install -y parquet-devel
FROM ubuntu:16.04
RUN apt-get update && apt-get install -y apt-transport-https curl gnupg lsb-release pkg-config libsqlite3-dev libicu-dev gcc g++ meson
COPY ci/arrow-ubuntu16.04.list /etc/apt/sources.list.d/apache-arrow.list
RUN curl https://dist.apache.org/repos/dist/dev/arrow/KEYS | apt-key add - && apt-get update && apt-get install -y libparquet-dev

#https://apache.bintray.com/arrow/centos/7/x86_64/Packages/parquet-devel-0.14.1-1.el7.x86_64.rpm https://apache.bintray.com/arrow/centos/7/x86_64/Packages/parquet-libs-0.14.1-1.el7.x86_64.rpm https://apache.bintray.com/arrow/centos/7/x86_64/Packages/arrow-libs-0.14.1-1.el7.x86_64.rpm https://apache.bintray.com/arrow/centos/7/x86_64/Packages/arrow-glib-libs-0.14.1-1.el7.x86_64.rpm https://apache.bintray.com/arrow/centos/7/x86_64/Packages/arrow-glib-devel-0.14.1-1.el7.x86_64.rpm https://apache.bintray.com/arrow/centos/7/x86_64/Packages/arrow-devel-0.14.1-1.el7.x86_64.rpm 
#COPY ci/arrow-centos.repo //yum.repos.d/Apache-Arrow.repo
#RUN yum install -y epel-release && yum install -y --enablerepo=epel parquet-devel
WORKDIR /src/
VOLUME /src
CMD mkdir builddir && meson builddir && cd builddir && ninja
