package com.caribu.filiale;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class quotes {
    
 String id_tratta;
Integer latitudine;
Integer longitudine;

Double oLat;
Double oLon;
Double dLat;
Double dLon;

//etc...
   
    public JsonObject toJsonObject() {
        return JsonObject.mapFrom(this);
      }
  
}