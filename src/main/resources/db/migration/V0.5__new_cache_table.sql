DROP TABLE cache;

CREATE TABLE cache (
    id_commessa VARCHAR NOT NULL,
    id_quotes VARCHAR NOT NULL,
    PRIMARY KEY (id_commessa, id_quotes),
    FOREIGN KEY (id_quotes) REFERENCES quotes(id_quotes)
);