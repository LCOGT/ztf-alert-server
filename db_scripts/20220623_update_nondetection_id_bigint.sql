BEGIN;
ALTER TABLE non_detection
ADD COLUMN new_id bigint;
CREATE INDEX non_detection_new_pkey
ON non_detection USING btree(new_id);
UPDATE non_detection
SET new_id = id;
END;


BEGIN;
ALTER TABLE non_detection
DROP INDEX non_detection_pkey;
ALTER TABLE non_detection
DROP CONSTRAINT id;
ALTER TABLE non_detection
DROP COLUMN id;
ALTER TABLE non_detection
RENAME COLUMN new_id to id;
ALTER TABLE non_detection
RENAME INDEX non_detection_new_pkey to non_detection_pkey;
ALTER TABLE non_detection
ADD CONSTRAINT id
PRIMARY KEY USING INDEX non_detection_pkey;
END;
