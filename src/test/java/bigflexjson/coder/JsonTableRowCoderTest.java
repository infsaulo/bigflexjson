package bigflexjson.coder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import com.google.api.services.bigquery.model.TableRow;

import bigflexjson.grammar.Grammar;
import bigflexjson.grammar.GrammarParser;

public class JsonTableRowCoderTest {

  @Mock
  final Context context = new Context(true);

  @Test
  public void testJsonTableRowCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"STRING\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\"}"
        + "]}";

    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final String jsonObjStr = "{\"field1\":42, \"field2\": \"abgef102\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    Assert.assertTrue(((String) row.get("field_1")).equals("42"));
    Assert.assertTrue(((String) row.get("field_2")).equals("abgef102"));
  }

  @Test
  public void testJsonTableRowCoderWithRepeatedRecords() throws CoderException, IOException {
    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"objs\",\"srcType\":\"RECORD\",\"bqType\":\"RECORD\", \"destName\":\"objs\", \"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"STRING\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\"}"
        + "]}]}";

    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final String jsonObjStr =
        "{\"objs\":[{\"field1\":42,\"field2\":\"12\"},{\"field1\":13,\"field2\":\"testObj\"}]}";

    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    Assert.assertTrue(((List<Object>) row.get("objs")).size() == 2);
  }

}
