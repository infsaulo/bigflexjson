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
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\", \"isRepeated\":true}"
        + "]}]}";

    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final String jsonObjStr =
        "{\"objs\":[{\"field1\":42,\"field2\":[\"12\"]},{\"field1\":13,\"field2\":[\"testObj\", \"testObj2\"]}]}";

    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    Assert.assertTrue(((List<Object>) row.get("objs")).size() == 2);

    final List<TableRow> list = (List<TableRow>) row.get("objs");
    final List<String> field2 = (List<String>) list.get(0).get("field_2");
    Assert.assertTrue(field2.get(0).equals("12"));
  }

  @Test
  public void testRepeatedFieldTableRowCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"STRING\", \"destName\":\"field_1\", "
        + "\"isRepeated\": true},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\"}"
        + "]}";

    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final String jsonObjStr = "{\"field1\":[42,23,12], \"field2\": \"abgef102\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);
    Assert.assertTrue(((List<String>) row.get("field_1")).get(0).equals("42"));
    Assert.assertTrue(((List<String>) row.get("field_1")).get(1).equals("23"));
    Assert.assertTrue(((List<String>) row.get("field_1")).get(2).equals("12"));
    Assert.assertTrue(((List<String>) row.get("field_1")).size() == 3);
  }

  @Test
  public void testTxJsonTableRowCoder() throws CoderException, IOException {
    final String grammarRepr = "{" + "fields:[" + "{" + "name:\"hash\"," + "srcType:\"STRING\","
        + "bqType:\"STRING\"," + "destName:\"hash\"" + "}," + "{" + "name:\"version\","
        + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"version\"" + "}," + "{"
        + "name:\"size\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"size\""
        + "}," + "{" + "name:\"fee\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\","
        + "destName:\"fee\"" + "}," + "{" + "name:\"volumeIn\"," + "srcType:\"INTEGER\","
        + "bqType:\"INTEGER\"," + "destName:\"volumeIn\"" + "}," + "{" + "name:\"volumeOut\","
        + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"volumeOut\"" + "}," + "{"
        + "name:\"numInputs\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\","
        + "destName:\"numInputs\"" + "}," + "{" + "name:\"numOutputs\"," + "srcType:\"INTEGER\","
        + "bqType:\"INTEGER\"," + "destName:\"numOutputs\"" + "}," + "{" + "name:\"blockHash\","
        + "srcType:\"STRING\"," + "bqType:\"STRING\"," + "destName:\"blockHash\"" + "}," + "{"
        + "name:\"blockHeight\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\","
        + "destName:\"blockHeight\"" + "}," + "{" + "name:\"blockPosition\","
        + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"blockPosition\"" + "}," + "{"
        + "name:\"blockTimeEpochSecond\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\","
        + "destName:\"blockTimeEpochSecond\"" + "}," + "{" + "name:\"outputs\","
        + "srcType:\"RECORD\"," + "bqType:\"RECORD\"," + "destName:\"outputs\"," + "fields:[" + "{"
        + "name:\"index\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"index\""
        + "}," + "{" + "name:\"value\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\","
        + "destName:\"value\"" + "}," + "{" + "name:\"address\"," + "srcType:\"STRING\","
        + "bqType:\"STRING\"," + "destName:\"address\"" + "}," + "{" + "name:\"scriptPubKeyHex\","
        + "srcType:\"STRING\"," + "bqType:\"STRING\"," + "destName:\"scriptPubKeyHex\"" + "}," + "{"
        + "name:\"type\"," + "srcType:\"STRING\"," + "bqType:\"STRING\"," + "destName:\"type\""
        + "}," + "{" + "name:\"cluster\"," + "srcType:\"STRING\"," + "bqType:\"STRING\","
        + "destName:\"cluster\"" + "}" + "]" + "}," + "{" + "name:\"inputs\","
        + "srcType:\"RECORD\"," + "bqType:\"RECORD\"," + "destName:\"inputs\"," + "fields:[" + "{"
        + "name:\"previousTransactionHash\"," + "srcType:\"STRING\"," + "bqType:\"STRING\","
        + "destName:\"previousTransactionHash\"" + "}," + "{" + "name:\"previousIndex\","
        + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"previousIndex\"" + "}," + "{"
        + "name:\"value\"," + "srcType:\"INTEGER\"," + "bqType:\"INTEGER\"," + "destName:\"value\""
        + "}," + "{" + "name:\"address\"," + "srcType:\"STRING\"," + "bqType:\"STRING\","
        + "destName:\"address\"" + "}," + "{" + "name:\"scriptSigHex\"," + "srcType:\"STRING\","
        + "bqType:\"STRING\"," + "destName:\"scriptSigHex\"" + "}," + "{"
        + "name:\"scriptPubKeyHex\"," + "srcType:\"STRING\"," + "bqType:\"STRING\","
        + "destName:\"scriptPubKeyHex\"" + "}," + "{" + "name:\"type\"," + "srcType:\"STRING\","
        + "bqType:\"STRING\"," + "destName:\"type\"" + "}," + "{" + "name:\"sigHashType\","
        + "srcType:\"STRING\"," + "bqType:\"STRING\"," + "destName:\"sigHashType\"" + "}," + "{"
        + "name:\"cluster\"," + "srcType:\"STRING\"," + "bqType:\"STRING\","
        + "destName:\"cluster\"" + "}" + "]" + "}" + "]" + "}";

    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final String jsonObjStr =
        "{\"blockTimeEpochSecond\": 1415133245, \"fee\": 550000, \"hash\": \"5dec6aa6f0e8e15613e89cb40f499b81f143ac75d5ce482933168cc3e8671c96\", \"blockHash\": \"0000000000000000023dbebb7043b0420cb52047e22e48e6f37c2e15cb480733\", \"volumeIn\": 4001550006, \"outputs\": [{\"index\": 0, \"value\": 4000000000, \"cluster\": \"7dd5b463245c36e523145877f7899d04a7e1b3b0\", \"scriptPubKeyHex\": \"1f8b08000000000000002b5b29e27454f7298fcddac2db36ce7b5db6757ed1fddc20d2b106007a51610a19000000\", \"address\": \"42c52de50c3cad71db3c43bd44b689f42df38014\", \"type\": \"PUBKEYHASH\"}, {\"index\": 1, \"value\": 1000006, \"cluster\": \"90b090327d39966b46bb5d74eca3063f55327e68\", \"scriptPubKeyHex\": \"1f8b08000000000000002b5b2912586667c8e021713a700aefdf893ad12ae7af49fdec5803000be0be6b19000000\", \"address\": \"51763e31004818cb51940dfd912c5b24cfd61af9\", \"type\": \"PUBKEYHASH\"}], \"volumeOut\": 4001000006, \"blockPosition\": 1305, \"version\": 1, \"numOutputs\": 2, \"blockHeight\": 328542, \"numInputs\": 342, \"size\": 50516}";

    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);
  }
}
