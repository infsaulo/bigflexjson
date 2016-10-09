package bigflexjson.bigtable.grammar;

import java.util.List;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import com.wizzardo.tools.json.JsonTools;

import bigflexjson.grammar.GrammarParser;

public class BigTableGrammarParser extends GrammarParser {

  public BigTableGrammar getBigTableGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final BigTableGrammar grammar = JsonTools.parse(jsonRepresentation, BigTableGrammar.class);

    Preconditions.checkNotNull(grammar.getFields(), "fields list must be present");

    validateBigTableFields(grammar.getFields());

    return grammar;
  }

  protected void validateBigTableFields(final List<BigTableField> fields)
      throws IllegalStateException, NullPointerException {

    Preconditions.checkState(fields.size() > 0, "fields list must contain at least one element");

    for (final BigTableField field : fields) {
      validateBigTableRequiredFields(field);
      validateField(field);
    }
  }

  protected void validateBigTableRequiredFields(final BigTableField field)
      throws NullPointerException, IllegalStateException, IllegalArgumentException {

    super.validateRequiredFields(field);

    Preconditions.checkNotNull(field.isValueQualifier(), "isValueQualifier must be present");

    if (!field.isValueQualifier()) {

      Preconditions.checkNotNull(field.getDestQualifier(),
          "destQualifier must be present when value is not intended to be stored as qualifier");
    } else {

      Preconditions.checkArgument(field.getDestQualifier() == null,
          "destQualifier must be absent when value is intended to be stored as qualifier");
    }
  }
}
