package bigflexjson.text.jsonld.grammar;

import com.wizzardo.tools.json.JsonTools;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import java.util.List;

import bigflexjson.grammar.GrammarParser;

public class JsonLdGrammarParser extends GrammarParser {

  public JsonLdGrammar getJsonLdGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final JsonLdGrammar grammar = JsonTools.parse(jsonRepresentation, JsonLdGrammar.class);

    Preconditions.checkNotNull(grammar.getFields(), "fields list must be present");

    validateJsonLdFields(grammar.getFields());

    return grammar;
  }

  protected void validateJsonLdFields(final List<JsonLdField> fields)
      throws IllegalStateException, NullPointerException {

    Preconditions.checkState(fields.size() > 0, "fields list must contain at least one element");

    for (final JsonLdField field : fields) {
      validateJsonLdRequiredFields(field);
      validateField(field);
    }
  }

  protected void validateJsonLdRequiredFields(final JsonLdField field)
      throws NullPointerException, IllegalStateException {

    super.validateRequiredFields(field);

    Preconditions.checkNotNull(field.getDelimiter(), "delimiter field must be present");
    Preconditions.checkNotNull(field.getSrcValue(), "srcValue field must be present");


  }
}
