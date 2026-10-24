FROM apache/hadoop:3.4.1
USER root
# CentOS 7 sudah EOL: mirror lama mati, ambil python3 dari vault.
RUN sed -i -e 's|^mirrorlist=|#mirrorlist=|' -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever|baseurl=http://vault.centos.org/7.9.2009|' /etc/yum.repos.d/CentOS-*.repo \
 && yum install -y -q python3 && yum clean all
USER hadoop
