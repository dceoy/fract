FROM python

ADD https://github.com/oanda/oandapy/archive/master.tar.gz /tmp/oandapy.tar.gz
ADD . /tmp/fract

RUN set -e \
      && apt-get -y update \
      && apt-get -y upgrade \
      && apt-get clean

RUN set -e \
      && pip install -U pip \
      && pip install -U /tmp/oandapy.tar.gz /tmp/fract \
      && rm -rf /tmp/*

ENTRYPOINT ["fract"]
