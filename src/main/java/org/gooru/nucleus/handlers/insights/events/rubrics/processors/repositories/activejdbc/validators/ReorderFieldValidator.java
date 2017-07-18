package org.gooru.nucleus.handlers.insights.events.rubrics.processors.repositories.activejdbc.validators;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by ashish on 1/2/16.
 */
public class ReorderFieldValidator implements FieldValidator {
    private static final String REORDER_ID = "id";
    private static final String REORDER_SEQUENCE = "sequence_id";

    @Override
    public boolean validateField(Object value) {
        if (!(value instanceof JsonArray) || value == null || ((JsonArray) value).isEmpty()) {
            return false;
        }
        JsonArray input = (JsonArray) value;
        List<Integer> sequences = new ArrayList<>(input.size());
        for (Object o : input) {
            if (!(o instanceof JsonObject)) {
                return false;
            }
            JsonObject entry = (JsonObject) o;
            if ((entry.getMap().keySet().isEmpty() || entry.getMap().keySet().size() != 2)) {
                return false;
            }
            try {
                Integer sequence = entry.getInteger(REORDER_SEQUENCE);
                if (sequence == null) {
                    return false;
                }
                String idString = entry.getString(REORDER_ID);
                UUID id = UUID.fromString(idString);
                sequences.add(sequence);
            } catch (ClassCastException | IllegalArgumentException e) {
                return false;
            }
        }
        if (sequences.size() != input.size()) {
            return false;
        }
        for (int i = 1; i <= input.size(); i++) {
            if (!sequences.contains(i)) {
                return false;
            }
        }
        return true;
    }
}
