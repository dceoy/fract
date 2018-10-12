FROM dceoy/oanda-cli:latest

ADD https://github.com/oanda/oandapy/archive/master.tar.gz /tmp/oandapy.tar.gz
ADD https://github.com/dceoy/oanda-cli/archive/master.tar.gz /tmp/oanda-cli.tar.gz
ADD . /tmp/fract

RUN set -e \
      && pip install -U --no-cache-dir /tmp/oandapy.tar.gz /tmp/oanda-cli.tar.gz /tmp/fract \
      && rm -rf /tmp/oandapy.tar.gz /tmp/oanda-cli.tar.gz /tmp/fract

ENTRYPOINT ["fract"]
