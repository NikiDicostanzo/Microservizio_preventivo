
CREATE TABLE tratta (
    id_tratta VARCHAR PRIMARY KEY,
    latitudine INTEGER,
    longitudine INTEGER    
);

CREATE TABLE fornitore ( 
    id_fornitore VARCHAR(6) PRIMARY KEY,
    nome VARCHAR,
    paese VARCHAR
);

CREATE TABLE quotes (
                id_quotes VARCHAR(6) PRIMARY KEY,
                operativo VARCHAR(10),
                lunghezza INTEGER,
                larghezza INTEGER, 
                profondità INTEGER,
                id_fornitore VARCHAR,
                id_tratta VARCHAR,
                costo INTEGER,
                FOREIGN KEY (id_fornitore) REFERENCES fornitore (id_fornitore),
                FOREIGN KEY (id_tratta) REFERENCES tratta (id_tratta)
);
