FROM python

ADD . /tmp/fractus

RUN set -e \
      && pip install git+https://github.com/oanda/oandapy.git \
      && pip install -U /tmp/fractus

CMD ["fract"]
