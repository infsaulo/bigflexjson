package bigflexjson.grammar.bigquery;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import com.wizzardo.tools.json.JsonTools;

import bigflexjson.grammar.Field;
import bigflexjson.grammar.GrammarParser;

public class BigQueryGrammarParser extends GrammarParser {

  public BigQueryGrammar getBigQueryGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final BigQueryGrammar grammar = JsonTools.parse(jsonRepresentation, BigQueryGrammar.class);

    Preconditions.checkNotNull(grammar.getFields(), "fields list must be present");

    validateFields(grammar.getFields());

    return grammar;
  }

  protected void validateRequiredFields(final BigQueryField field)
      throws NullPointerException, IllegalStateException {

    super.validateRequiredFields(field);

    if (field.getSrcType().equals("RECORD")) {
      Preconditions.checkState(field.getFields().size() > 0,
          "fields inside a record must contain at least one element");
      for (final BigQueryField innerField : field.getFields()) {
        validateRequiredFields(innerField);
        super.validateField(innerField);
      }
    }
  }

  private void validateRecordMapping(final Field field) throws IllegalStateException {
    switch (field.getDestType()) {

      case "RECORD":
        break;
      default:
        throw new IllegalStateException(field.getDestType() + " cannot be type casted from RECORD");
    }
  }

  @Override
  protected void validateTypeMapping(final Field field) throws IllegalStateException {

    switch (field.getSrcType()) {
      case "INTEGER":
        super.validateIntegerMapping(field);
        break;
      case "DECIMAL":
        super.validateDecimalMapping(field);
        break;
      case "STRING":
        super.validateStringMapping(field);
        break;
      case "RECORD":
        validateRecordMapping(field);
        break;
      default:
        throw new IllegalStateException(field.getSrcType() + " is not supported as a source type");
    }
  }
}
