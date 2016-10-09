package bigflexjson.bigtable.grammar;

import org.junit.Test;

public class BigTableGrammarPaserTest {

  @Test
  public void testGrammarParserConsistent() {

    final String grammarRepr =
        "{\"fields\":[{\"name\":\"field1\",\"srcType\":\"INTEGER\", \"destName\":\"field_1\", "
            + "\"destType\":\"STRING\", \"isQualifier\":false, \"destQualifier\": \"qualifierName\"},"
            + "{\"name\":\"field2\",\"srcType\":\"INTEGER\", \"destName\":\"field_2\", \"destType\":\"STRING\", "
            + "\"isQualifier\":true}]}";

    final BigTableGrammarParser parser = new BigTableGrammarParser();
    parser.getBigTableGrammar(grammarRepr);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGrammarParserValueInconsistentAsQualifier() {

    final String grammarRepr =
        "{\"fields\":[{\"name\":\"field1\",\"srcType\":\"INTEGER\", \"destName\":\"field_1\", "
            + "\"destType\":\"STRING\", \"isQualifier\":false, \"destQualifier\": \"qualifierName\"},"
            + "{\"name\":\"field2\",\"srcType\":\"INTEGER\", \"destName\":\"field_2\", \"destType\":\"STRING\", "
            + "\"isQualifier\":true, \"destQualifier\":\"qualifier\"}]}";

    final BigTableGrammarParser parser = new BigTableGrammarParser();
    parser.getBigTableGrammar(grammarRepr);
  }

  @Test(expected = NullPointerException.class)
  public void testGrammarParserValueInconsistenAsNotQualifier() {

    final String grammarRepr =
        "{\"fields\":[{\"name\":\"field1\",\"srcType\":\"INTEGER\", \"destName\":\"field_1\", "
            + "\"destType\":\"STRING\", \"isQualifier\":false},"
            + "{\"name\":\"field2\",\"srcType\":\"INTEGER\", \"destName\":\"field_2\", \"destType\":\"STRING\", "
            + "\"isQualifier\":true}]}";

    final BigTableGrammarParser parser = new BigTableGrammarParser();
    parser.getBigTableGrammar(grammarRepr);
  }
}
