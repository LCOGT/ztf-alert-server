ALTER TABLE alert
ADD COLUMN drb double precision,
ADD COLUMN drbversion varchar(200) NOT NULL DEFAULT '';

CREATE INDEX ix_alert_drb ON alert USING btree(drb);