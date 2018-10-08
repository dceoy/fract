FROM dceoy/oanda:latest

ADD . /tmp/fract

RUN set -e \
      && pip install -U --no-cache-dir /tmp/fract \
      && rm -rf /tmp/fract

ENTRYPOINT ["fract"]
