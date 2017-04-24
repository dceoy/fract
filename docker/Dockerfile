FROM python

RUN set -e \
      && pip install git+https://github.com/oanda/oandapy.git \
      && pip install -U git+https://github.com/dceoy/fractus.git

CMD ["fract"]
