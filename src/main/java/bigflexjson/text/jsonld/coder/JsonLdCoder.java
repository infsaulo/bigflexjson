package bigflexjson.text.jsonld.coder;

import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;

import bigflexjson.text.jsonld.grammar.JsonLdField;
import bigflexjson.text.jsonld.grammar.JsonLdGrammar;

public class JsonLdCoder {

  private final JsonLdGrammar grammar;

  public JsonLdCoder(JsonLdGrammar grammar) {

    this.grammar = grammar;
  }

  public JsonObject decode(JsonObject object) {

    JsonObject decodedObj = new JsonObject();

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

  private void fromStringType(JsonLdField field, JsonObject inputObject, JsonObject decodedObj) {

    String value;

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
        decodedObj.put(field.getDestName(), new JsonItem(Integer.valueOf(value)));
        break;
      case "FLOAT":
        decodedObj.put(field.getDestName(), new JsonItem(Double.valueOf(value)));
        break;
      case "STRING":
        decodedObj.put(field.getDestName(), new JsonItem(value));
        break;
      default:
        throw new IllegalStateException(
            field.getDestType() + " cannot be type casted from INTEGER");
    }
  }
}