package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.Objects;

public class JsonValueTransform implements Transform
{

    private static final Logger log = new Logger(JsonValueTransform.class);
    private final String name;
    private final String fieldName;
    private final String key;
    private final long badJsonTolThreshold;

    @JsonCreator
    public JsonValueTransform(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("key") final String key,
            @JsonProperty("badJsonTolThreshold") long badJsonTolThreshold
    )
    {
        this.name = Preconditions.checkNotNull(name, "name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
        this.key = Preconditions.checkNotNull(key, "key");
        this.badJsonTolThreshold = Preconditions.checkNotNull(badJsonTolThreshold, "badJsonTolThreshold");
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @JsonProperty
    public String getKey() {
        return key;
    }

    @Override
    public RowFunction getRowFunction() {
        return new JsonValueRowFunction(fieldName, key, badJsonTolThreshold);
    }

    static class JsonValueRowFunction implements RowFunction {
        private final String fieldName;
        private final String key;
        private final long badJsonTolThreshold;
        private long exJsonCount = 0;

        public JsonValueRowFunction(String fieldName, String key, long badJsonTolThreshold) {
           this.fieldName = fieldName;
           this.key = key;
           this.badJsonTolThreshold = badJsonTolThreshold;
        }

        @Override
        public Object eval(Row row) {
            Object value = new Object();

            Object rawValue = getValueFromRow(row, fieldName);

            if (rawValue != null && !StringUtils.endsWithIgnoreCase(rawValue.toString(), "NULL")) {
                // {"a":"1","b":2,"c":{"d":{"g":{"h":3}}}, "e":null, "f":"null"}
                String rawString = rawValue.toString();

                if(StringUtils.isBlank(rawString)) {
                    return null;
                }

                try {
                    JsonNode jsonNode = new ObjectMapper().readValue(rawString, JsonNode.class).findPath(key);
                    if (jsonNode.isNull()) {
                        value = null;
                    } else {
                        value =  StringUtils.strip(jsonNode.toString(), "\"");
                    }
                } catch (Exception e) {
                    log.warn(e,"get json value failed! rawString:%s.", rawString);
                    exJsonCount++;
                    if (exJsonCount > badJsonTolThreshold && badJsonTolThreshold != -1) {
                        log.error(e, "@@ Probrom json count is max than:[%d]", badJsonTolThreshold);
                        throw new ParseException(e, " $$ Unable to parse row [%s]！ Probrom json count is max than:[%s]", rawString, String.valueOf(exJsonCount));
                    }
                }
            }

            return value;
        }
    }

    private static Object getValueFromRow(final Row row, final String column)
    {
        if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
            return row.getTimestampFromEpoch();
        } else {
            return row.getRaw(column);
        }
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JsonValueTransform that = (JsonValueTransform) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName);
    }

    @Override
    public String toString()
    {
        return "JsonValueTransform{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", key='" + key + '\'' +
                '}';
    }

}
