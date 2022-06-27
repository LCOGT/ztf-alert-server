BEGIN;
-- before we create the new bigint column (new_id) and specify the PK sequence
-- we need to tell the sequence that it must be a bigint now..
ALTER SEQUENCE non_detection_id_seq AS bigint;
-- bigint default nextval() is equivalent to BIGSERIAL and we will be recycling the old sequence
ALTER TABLE non_detection ADD COLUMN new_id bigint default nextval('non_detection_id_seq');
-- change ownership of this sequence to the new_id (the new PK)
-- because we will not be able to delete the old primary key while it still owns the sequence
ALTER SEQUENCE non_detection_id_seq OWNED by non_detection.new_id;
CREATE UNIQUE INDEX non_detection_new_pkey ON non_detection USING btree(new_id);
UPDATE non_detection SET new_id = id;
END;


BEGIN;
ALTER TABLE non_detection DROP COLUMN id;
ALTER TABLE non_detection RENAME COLUMN new_id to id;
ALTER INDEX non_detection_new_pkey RENAME TO non_detection_pkey;
ALTER TABLE non_detection ADD PRIMARY KEY USING INDEX non_detection_pkey;
END;
