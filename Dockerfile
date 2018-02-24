FROM python

ADD https://github.com/oanda/oandapy/archive/master.tar.gz /tmp/oandapy.tar.gz
ADD . /tmp/fract

RUN set -e \
      && ln -sf /bin/bash /bin/sh

RUN set -e \
      && apt-get -y update \
      && apt-get -y upgrade \
      && apt-get -y autoremove \
      && apt-get clean

RUN set -e \
      && pip install -U --no-cache-dir pip \
      && pip install -U --no-cache-dir /tmp/oandapy.tar.gz /tmp/fract \
      && rm -rf /tmp/*

ENTRYPOINT ["fract"]
