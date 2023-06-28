ALTER TABLE quotes DROP COLUMN id_tratta;
DROP TABLE tratta;

ALTER TABLE quotes ADD COLUMN origin_geom geometry(Point, 4326);
ALTER TABLE quotes ADD COLUMN destination_geom geometry(Point, 4326);

CREATE TABLE cache (
    id_commessa VARCHAR PRIMARY KEY,
    id_quotes VARCHAR,
    FOREIGN KEY (id_quotes) REFERENCES quotes(id_quotes)
);
