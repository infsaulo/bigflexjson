package bigflexjson.text.csv.coder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.grammar.Grammar;
import bigflexjson.grammar.GrammarParser;

public class CsvJsonReprCoderTest {

  @Mock
  final Context context = new Context(true);

  @Test
  public void testCsvJsonReprCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"0\",\"srcType\":\"STRING\",\"destType\":\"INTEGER\", \"destName\":\"field_1\"},"
        + "{\"name\":\"1\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_2\"},"
        + "{\"name\":\"2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_3\"}"
        + "]}";

    final GrammarParser parser = new GrammarParser();
    final Grammar grammar = parser.getGrammar(grammarRepr);

    final String csvStr = "42,field2test,\"test,test2\"";
    final InputStream csvInputStream =
        new ByteArrayInputStream(csvStr.getBytes(StandardCharsets.UTF_8));

    final CsvJsonReprCoder coder = new CsvJsonReprCoder(grammar);

    final String jsonRepr = coder.decode(csvInputStream, context);

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();

    Assert.assertEquals(Long.valueOf(42), jsonObj.getAsLong("field_1"));
    Assert.assertEquals("field2test", jsonObj.getAsString("field_2"));
    Assert.assertEquals("test,test2", jsonObj.getAsString("field_3"));
  }

}
