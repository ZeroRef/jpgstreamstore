package org.zeroref.jpgstreamstore.serialization;

import com.google.gson.*;
import org.zeroref.jpgstreamstore.DomainEvent;

import java.lang.reflect.Type;
import java.util.Date;

public class EventSerializer {

    private Gson gson;

    public EventSerializer() {
        this.gson = new GsonBuilder().registerTypeAdapter(Date.class, new DateSerializer())
                .registerTypeAdapter(Date.class, new DateDeserializer()).serializeNulls().create();
    }

    public String serialize(DomainEvent aDomainEvent) {
        return gson.toJson(aDomainEvent);
    }

    public <T extends DomainEvent> T deserialize(String aSerialization, final Class<T> aType) {
        return gson.fromJson(aSerialization, aType);
    }

    private class DateSerializer implements JsonSerializer<Date> {
        public JsonElement serialize(Date source, Type typeOfSource, JsonSerializationContext context) {
            return new JsonPrimitive(Long.toString(source.getTime()));
        }
    }

    private class DateDeserializer implements JsonDeserializer<Date> {
        public Date deserialize(JsonElement json, Type typeOfTarget, JsonDeserializationContext context) throws JsonParseException {
            long time = Long.parseLong(json.getAsJsonPrimitive().getAsString());
            return new Date(time);
        }
    }
}
