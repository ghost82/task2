FROM  sequenceiq/spark:1.6.0

RUN yum install -y build-essential python34 python34-devel python34-setuptools sqlite3 libsqlite3-dev
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.4
ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN mkdir /etc/luigi
ADD luigi.cfg /etc/luigi/luigi.cfg

RUN mkdir /var/log/luigid
ADD logrotate.cfg /etc/logrotate.d/luigid
VOLUME /var/log/luigid

ADD bootstrap.sh /etc/bootstrap.sh
# update boot script
COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh

ADD luigid.sh /etc/luigi/
# update boot script
RUN chown root.root /etc/luigi/luigid.sh
RUN chmod 700 /etc/luigi/luigid.sh

RUN mkdir /data
COPY tech_lead /data 

RUN mkdir /task2
COPY task2 /task2

EXPOSE 8082
ENTRYPOINT ["/etc/bootstrap.sh"]
