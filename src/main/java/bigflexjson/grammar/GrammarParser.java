package bigflexjson.grammar;

import java.util.List;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import com.wizzardo.tools.json.JsonTools;

public class GrammarParser {

  public static Grammar getGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final Grammar grammar = JsonTools.parse(jsonRepresentation, Grammar.class);

    Preconditions.checkNotNull(grammar.fields, "fields list must be present");

    validateFields(grammar.getFields());

    return grammar;
  }

  private static void validateFields(final List<Field> fields)
      throws IllegalStateException, NullPointerException {

    Preconditions.checkState(fields.size() > 0, "fields list must contain at least one element");

    for (final Field field : fields) {
      validateRequiredFields(field);
      validateField(field);
    }
  }

  private static void validateRequiredFields(final Field field) throws NullPointerException {

    Preconditions.checkNotNull(field.name, "name field must be present");
    Preconditions.checkNotNull(field.destName, "destName field must be present");
    Preconditions.checkNotNull(field.srcType, "srcType field must be present");
    Preconditions.checkNotNull(field.bqType, "bqType field must be present");
  }

  private static void validateField(final Field field) throws IllegalStateException {

    validateTypeMapping(field);
  }

  private static void validateIntegerMapping(final Field field) throws IllegalStateException {

    switch (field.getBqType()) {

      case "STRING":
        break;
      case "INTEGER":
        break;
      case "FLOAT":
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from INTEGER");
    }

  }

  private static void validateDecimalMapping(final Field field) throws IllegalStateException {

    switch (field.getBqType()) {

      case "STRING":
        break;
      case "FLOAT":
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from DECIMAL");
    }
  }

  private static void validateStringMapping(final Field field) throws IllegalStateException {

    switch (field.getBqType()) {

      case "STRING":
        break;
      case "BYTES":
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from STRING");
    }
  }

  private static void validateTypeMapping(final Field field) throws IllegalStateException {

    switch (field.getSrcType()) {
      case "INTEGER":
        validateIntegerMapping(field);
        break;
      case "DECIMAL":
        validateDecimalMapping(field);
        break;
      case "STRING":
        validateStringMapping(field);
        break;
      default:
        throw new IllegalStateException(field.getSrcType() + " is not supported as a source type");
    }
  }
}
