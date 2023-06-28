package com.caribu.filiale;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class quotes {
    
 String id_quotes;

Double oLat;
Double oLon;
Double dLat;
Double dLon;

//                 operativo VARCHAR(10),
//                 lunghezza INTEGER,
//                 larghezza INTEGER, 
//                 profondità INTEGER,
//                 id_fornitore VARCHAR,
//                 costo INTEGER,

//etc...
   
    public JsonObject toJsonObject() {
        return JsonObject.mapFrom(this);
      }
  
}