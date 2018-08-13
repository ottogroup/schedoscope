FROM cloudera/quickstart:5.7.0-0-beta
MAINTAINER "notExist <notExist@ottogroup.com>"

ADD Dockerfile /Dockerfile

RUN curl -L -b "oraclelicense=a" http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-linux-x64.tar.gz | tar xvz -C /usr/java \
    && echo "export JAVA_HOME=/usr/java/jdk1.8.0_181" >> /etc/default/cloudera-scm-server \
    && echo "export JAVA_HOME=/usr/java/jdk1.8.0_181" >> /etc/bashrc \
    && echo "export MAVEN_HOME=/usr/local/apache-maven/apache-maven-3.0.4" >> /etc/bashrc \
    && echo "export PATH=\$JAVA_HOME/bin:\$MAVEN_HOME/bin:\$PATH" >> /etc/bashrc

RUN yum remove -y git ; yum clean all \
    && yum install -y --nogpgcheck wget curl-devel expat-devel gettext-devel openssl-devel zlib-devel gcc perl-ExtUtils ; yum clean all \
    && yum update -y nss curl libcurl ; yum clean all \
    && wget --no-check-certificate -qO- https://www.kernel.org/pub/software/scm/git/git-2.9.5.tar.gz | tar xvz -C /usr/local/src \
    && cd /usr/local/src/git-2.9.5 \
    && make prefix=/usr/local/git-2.9.5 all \
    && make prefix=/usr/local/git-2.9.5 install \
    && ln -s /usr/local/git-2.9.5/bin/git /usr/bin/git

RUN sed -i 's/\/5/\/5.14.0/g' /etc/yum.repos.d/cloudera-manager.repo \
    && yum --nogpgcheck -y upgrade cloudera-manager-server cloudera-manager-daemons cloudera-manager-agent ; yum clean all

RUN wget -qO- http://archive.cloudera.com/spark2/csd/SPARK2_ON_YARN-2.2.0.cloudera2.jar > /opt/cloudera/csd/SPARK2_ON_YARN-2.2.0.cloudera2.jar \
    && chown cloudera-scm:cloudera-scm /opt/cloudera/csd/SPARK2_ON_YARN-2.2.0.cloudera2.jar \
    && chmod 644 /opt/cloudera/csd/SPARK2_ON_YARN-2.2.0.cloudera2.jar

RUN yum -y install python-pip ; yum clean all \
    && pip install cm-api

COPY scripts/parcel-installer.py /root/parcel-installer.py
COPY scripts/prepare-env.sh /root/prepare-env.sh

CMD /root/prepare-env.sh