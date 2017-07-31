-- sql for rate tracking

CREATE TABLE candle (
  instrument VARCHAR(7),
  time VARCHAR(30),
  openBid DOUBLE PRECISION,
  openAsk DOUBLE PRECISION,
  highBid DOUBLE PRECISION,
  highAsk DOUBLE PRECISION,
  lowBid DOUBLE PRECISION,
  lowAsk DOUBLE PRECISION,
  closeBid DOUBLE PRECISION,
  closeAsk DOUBLE PRECISION,
  volume SMALLINT,
  PRIMARY KEY(instrument, time)
);

CREATE INDEX ix_inst ON candle (instrument);
CREATE INDEX ix_time ON candle (time);
