package bigflexjson.grammar;

import org.junit.Test;

public class GrammarParseTest {

  @Test
  public void testSemanticallyCorrectGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}" + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = IllegalStateException.class)
  public void testSemanticallyIncorrectGrammarRepr() {
    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"bqType\":\"INTEGER\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}" + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);
  }

  @Test(expected = IllegalStateException.class)
  public void testSyntaxIncorrectGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destNames\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"bqType\":\"STRING\", \"field_2\", \"srcSerialization\":\"hex\"}"
        + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = NullPointerException.class)
  public void testMissingRequiredFieldGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"bqType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}" + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = NullPointerException.class)
  public void testEmptyGrammarRepr() {

    final String grammarJsonRepr = "{}";

    GrammarParser.getGrammar(grammarJsonRepr);
  }

  @Test(expected = IllegalStateException.class)
  public void testEmptyFieldsGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":[]}";

    GrammarParser.getGrammar(grammarJsonRepr);

  }

  @Test
  public void testRecordFieldsGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"},"
        + "{\"name\":\"field3\", \"srcType\":\"RECORD\",\"bqType\":\"RECORD\", \"destName\":\"field_3\", \"fields\": "
        + "[{\"name\":\"innerfield1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"inner_field_1\"},"
        + "{\"name\":\"innerfield2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"inner_field_2\", "
        + "\"srcSerialization\":\"hex\"}]}]}";

    GrammarParser.getGrammar(grammarJsonRepr);
  }
}
