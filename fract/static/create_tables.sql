-- sql for rate tracking

CREATE TABLE tick (
  instrument VARCHAR(7),
  time VARCHAR(30),
  bid DOUBLE PRECISION,
  ask DOUBLE PRECISION
);

CREATE INDEX ix_tick_inst ON tick (instrument);
CREATE INDEX ix_tick_time ON tick (time);

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
  volume INTEGER,
  PRIMARY KEY(instrument, time)
);

CREATE INDEX ix_candle_inst ON candle (instrument);
CREATE INDEX ix_candle_time ON candle (time);

CREATE TABLE event (
  instrument VARCHAR(7),
  time VARCHAR(30),
  json TEXT
);

CREATE INDEX ix_event_inst ON event (instrument);
CREATE INDEX ix_event_time ON event (time);
