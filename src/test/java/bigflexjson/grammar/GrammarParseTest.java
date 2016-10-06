package bigflexjson.grammar;

import org.junit.Test;

import bigflexjson.bigquery.grammar.BigQueryGrammarParser;

public class GrammarParseTest {

  @Test
  public void testSemanticallyCorrectGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}, "
        + "{\"name\":\"field3\",\"srcType\":\"INTEGER\",\"destType\":\"TIMESTAMP\", \"destName\":\"field_3\"},"
        + "{\"name\":\"field4\",\"srcType\":\"DECIMAL\",\"destType\":\"TIMESTAMP\", \"destName\":\"field_4\"},"
        + "{\"name\":\"field5\",\"srcType\":\"STRING\",\"destType\":\"TIMESTAMP\", \"destName\":\"field_5\"},"
        + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = IllegalStateException.class)
  public void testSemanticallyIncorrectGrammarRepr() {
    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"destType\":\"INTEGER\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}" + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);
  }

  @Test(expected = IllegalStateException.class)
  public void testSyntaxIncorrectGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destNames\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"destType\":\"STRING\", \"field_2\", \"srcSerialization\":\"hex\"}"
        + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = NullPointerException.class)
  public void testMissingRequiredFieldGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"BYTES\",\"destType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"}" + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = NullPointerException.class)
  public void testEmptyGrammarRepr() {

    final String grammarJsonRepr = "{}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);
  }

  @Test(expected = IllegalStateException.class)
  public void testEmptyFieldsGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":[]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);

  }

  @Test
  public void testRecordFieldsGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"},"
        + "{\"name\":\"field3\", \"srcType\":\"RECORD\",\"destType\":\"RECORD\", \"destName\":\"field_3\", \"fields\": "
        + "[{\"name\":\"innerfield1\",\"srcType\":\"INTEGER\",\"destType\":\"INTEGER\", \"destName\":\"inner_field_1\"},"
        + "{\"name\":\"innerfield2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"inner_field_2\", "
        + "\"srcSerialization\":\"hex\"}]}]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    parser.getGrammar(grammarJsonRepr);
  }
}
