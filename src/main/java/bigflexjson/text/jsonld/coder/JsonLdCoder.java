package bigflexjson.text.jsonld.coder;

import com.wizzardo.tools.json.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import bigflexjson.text.jsonld.grammar.JsonLdField;
import bigflexjson.text.jsonld.grammar.JsonLdGrammar;

public class JsonLdCoder {

  private static final Logger LOGGER = Logger.getLogger(JsonLdCoder.class.getName());

  private final JsonLdGrammar grammar;

  public JsonLdCoder(JsonLdGrammar grammar) {

    this.grammar = grammar;
  }

  public Map<String, Object> decode(JsonObject object) {

    Map<String, Object> decodedObj = new HashMap<>();

    for (JsonLdField field : grammar.getFields()) {

      switch (field.getSrcType()) {

        case "STRING":
          fromStringType(field, object, decodedObj);
          break;

        default:
          throw new IllegalStateException(
              field.getSrcType() + " is not supported as a source type");
      }
    }

    return decodedObj;
  }

  private void fromStringType(JsonLdField field, JsonObject inputObject,
                              Map<String, Object> decodedObj) {

    String value;

    try {

      if (field.getSrcValue().equals("@value")) {

        value =
            inputObject.getAsJsonArray(field.getName()).get(0).asJsonObject().getAsString("@value");
      } else {

        value =
            inputObject.getAsJsonArray(field.getName()).get(0).asJsonObject().getAsString("@id")
                .split(field.getDelimiter())[1];
      }

      switch (field.getDestType()) {
        case "INTEGER":
          decodedObj.put(field.getDestName(), Long.valueOf(value));
          break;

        case "FLOAT":
          decodedObj.put(field.getDestName(), Double.valueOf(value));
          break;

        case "STRING":
          decodedObj.put(field.getDestName(), value);
          break;

        default:
          throw new IllegalStateException(
              field.getDestType() + " cannot be type casted from INTEGER");
      }
    } catch (NullPointerException e) {

      LOGGER.log(Level.SEVERE,
                 "Error with field " + field.getSrcValue() + " and object " + inputObject
                     .toString(), e);
    }
  }
}
