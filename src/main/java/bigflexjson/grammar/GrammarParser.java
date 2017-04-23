package bigflexjson.grammar;

import java.util.List;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import com.wizzardo.tools.json.JsonTools;

public class GrammarParser {

  public Grammar getGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final Grammar grammar = JsonTools.parse(jsonRepresentation, Grammar.class);

    Preconditions.checkNotNull(grammar.getFields(), "fields list must be present");

    validateFields(grammar.getFields());

    return grammar;
  }

  protected void validateFields(final List<? extends Field> fields)
      throws IllegalStateException, NullPointerException {

    Preconditions.checkState(fields.size() > 0, "fields list must contain at least one element");

    for (final Field field : fields) {
      validateRequiredFields(field);
      validateField(field);
    }
  }

  protected void validateRequiredFields(final Field field)
      throws NullPointerException, IllegalStateException {

    Preconditions.checkNotNull(field.getName(), "name field must be present");
    Preconditions.checkNotNull(field.getDestName(), "destName field must be present");
    Preconditions.checkNotNull(field.getSrcType(), "srcType field must be present");
    Preconditions.checkNotNull(field.getDestType(), "destType field must be present");
  }

  protected void validateField(final Field field) throws IllegalStateException {

    validateTypeMapping(field);
  }

  protected void validateIntegerMapping(final Field field) throws IllegalStateException {

    switch (field.getDestType()) {

      case "STRING":
        break;
      case "INTEGER":
        break;
      case "FLOAT":
        break;
      case "TIMESTAMP":
        break;
      default:
        throw new IllegalStateException(
            field.getDestType() + " cannot be type casted from INTEGER");
    }

  }

  protected void validateDecimalMapping(final Field field) throws IllegalStateException {

    switch (field.getDestType()) {

      case "STRING":
        break;
      case "FLOAT":
        break;
      case "TIMESTAMP":
        break;
      default:
        throw new IllegalStateException(
            field.getDestType() + " cannot be type casted from DECIMAL");
    }
  }

  protected void validateStringMapping(final Field field) throws IllegalStateException {

    switch (field.getDestType()) {

      case "STRING":
        break;
      case "INTEGER":
        break;
      case "BYTES":
        break;
      case "TIMESTAMP":
        break;
      case "BOOLEAN":
        break;
      default:
        throw new IllegalStateException(field.getDestType() + " cannot be type casted from STRING");
    }
  }

  protected void validateTypeMapping(final Field field) throws IllegalStateException {

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
