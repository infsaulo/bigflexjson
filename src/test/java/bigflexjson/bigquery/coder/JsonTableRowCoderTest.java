package bigflexjson.bigquery.coder;

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

import bigflexjson.bigquery.grammar.BigQueryGrammar;
import bigflexjson.bigquery.grammar.BigQueryGrammarParser;

public class JsonTableRowCoderTest {

  @Mock
  final Context context = new Context(true);

  @Test
  public void testJsonTableRowCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"STRING\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_2\"}"
        + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);

    final String jsonObjStr = "{\"field1\":42, \"field2\": \"abgef102\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    Assert.assertTrue(((String) row.get("field_1")).equals("42"));
    Assert.assertTrue(((String) row.get("field_2")).equals("abgef102"));
  }

  @Test
  public void testTimestampJsonTableRowCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"TIMESTAMP\", \"destName\":\"field_1\"},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"TIMESTAMP\", \"destName\":\"field_2\"}"
        + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);


    final String jsonObjStr = "{\"field1\":1471910400, \"field2\": \"2014-08-19 07:41:35.220\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    Assert.assertTrue(((long) row.get("field_1")) == 1471910400);
    Assert.assertTrue(((String) row.get("field_2")).equals("2014-08-19 07:41:35.220"));
  }

  @Test
  public void testJsonTableRowCoderWithRepeatedRecords() throws CoderException, IOException {
    final String grammarRepr =
        "{\"fields\":[" + "{\"name\":\"objs\",\"srcType\":\"RECORD\",\"destType\":\"RECORD\", "
            + "\"destName\":\"objs\", \"isRepeated\": true, \"fields\":["
            + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"STRING\","
            + " \"destName\":\"field_1\"},"
            + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", "
            + "\"destName\":\"field_2\", \"isRepeated\":true}" + "]}]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);


    final String jsonObjStr = "{\"objs\":[{\"field1\":42,\"field2\":[\"12\"]},{\"field1\":13,"
        + "\"field2\":[\"testObj\", \"testObj2\"]}]}";

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
  public void testJsonTableRowCoderWithNestedRecord() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"RECORD\",\"destType\":\"RECORD\", \"destName\":\"field_1\", "
        + "\"isRepeated\": true, \"fields\":"
        + "[{\"name\":\"field3\",\"srcType\":\"RECORD\",\"destType\":\"RECORD\", "
        + "\"destName\":\"field_3\",\"isRepeated\": true, "
        + "\"fields\":[{\"name\":\"field4\",\"srcType\":\"RECORD\",\"destType\":\"RECORD\", "
        + "\"destName\":\"field_4\", "
        + "\"fields\":[{\"name\":\"field5\",\"srcType\":\"STRING\", \"destType\":\"STRING\", "
        + "\"destName\":\"field_5\"}]}]}]},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", "
        + "\"destName\":\"field_2\"}" + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);


    final String jsonObjStr = "{\"field1\":[{\"field3\":[{\"field4\":{\"field5\":\"test5\"}}] },"
        + "{\"field3\":[{\"field4\":{\"field5\":\"test6\"}}]}], \"field2\": \"abgef102\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);

    final List<TableRow> field1List = (List<TableRow>) row.get("field_1");
    Assert.assertTrue(field1List.size() == 2);
    final List<TableRow> field30List = (List<TableRow>) field1List.get(0).get("field_3");
    Assert.assertTrue(field30List.size() == 1);
    Assert
        .assertTrue(((TableRow) field30List.get(0).get("field_4")).get("field_5").equals("test5"));
    final List<TableRow> field31List = (List<TableRow>) field1List.get(1).get("field_3");
    Assert.assertTrue(field31List.size() == 1);
    Assert
        .assertTrue(((TableRow) field31List.get(0).get("field_4")).get("field_5").equals("test6"));
  }

  @Test
  public void testRepeatedFieldTableRowCoder() throws CoderException, IOException {

    final String grammarRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"destType\":\"STRING\", \"destName\":\"field_1\", "
        + "\"isRepeated\": true},"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"destType\":\"STRING\", \"destName\":\"field_2\"}"
        + "]}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);


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
        + "destType:\"STRING\"," + "destName:\"hash\"" + "}," + "{" + "name:\"version\","
        + "srcType:\"INTEGER\"," + "destType:\"INTEGER\"," + "destName:\"version\"" + "}," + "{"
        + "name:\"size\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\"," + "destName:\"size\""
        + "}," + "{" + "name:\"fee\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\","
        + "destName:\"fee\"" + "}," + "{" + "name:\"volumeIn\"," + "srcType:\"INTEGER\","
        + "destType:\"INTEGER\"," + "destName:\"volumeIn\"" + "}," + "{" + "name:\"volumeOut\","
        + "srcType:\"INTEGER\"," + "destType:\"INTEGER\"," + "destName:\"volumeOut\"" + "}," + "{"
        + "name:\"numInputs\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\","
        + "destName:\"numInputs\"" + "}," + "{" + "name:\"numOutputs\"," + "srcType:\"INTEGER\","
        + "destType:\"INTEGER\"," + "destName:\"numOutputs\"" + "}," + "{" + "name:\"blockHash\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"blockHash\"" + "}," + "{"
        + "name:\"blockHeight\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\","
        + "destName:\"blockHeight\"" + "}," + "{" + "name:\"blockPosition\","
        + "srcType:\"INTEGER\"," + "destType:\"INTEGER\"," + "destName:\"blockPosition\"" + "},"
        + "{" + "name:\"blockTimeEpochSecond\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\","
        + "destName:\"blockTimeEpochSecond\"" + "}," + "{" + "name:\"outputs\","
        + "srcType:\"RECORD\"," + "destType:\"RECORD\"," + "destName:\"outputs\","
        + "isRepeated:true," + "fields:[" + "{" + "name:\"index\"," + "srcType:\"INTEGER\","
        + "destType:\"INTEGER\"," + "destName:\"index\"" + "}," + "{" + "name:\"value\","
        + "srcType:\"INTEGER\"," + "destType:\"INTEGER\"," + "destName:\"value\"" + "}," + "{"
        + "name:\"address\"," + "srcType:\"STRING\"," + "destType:\"STRING\","
        + "destName:\"address\"" + "}," + "{" + "name:\"scriptPubKeyHex\"," + "srcType:\"STRING\","
        + "destType:\"STRING\"," + "destName:\"scriptPubKeyHex\"" + "}," + "{" + "name:\"type\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"type\"" + "}," + "{"
        + "name:\"cluster\"," + "srcType:\"STRING\"," + "destType:\"STRING\","
        + "destName:\"cluster\"" + "}" + "]" + "}," + "{" + "name:\"inputs\","
        + "srcType:\"RECORD\"," + "destType:\"RECORD\"," + "destName:\"inputs\","
        + "isRepeated:true," + "fields:[" + "{" + "name:\"previousTransactionHash\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"previousTransactionHash\""
        + "}," + "{" + "name:\"previousIndex\"," + "srcType:\"INTEGER\"," + "destType:\"INTEGER\","
        + "destName:\"previousIndex\"" + "}," + "{" + "name:\"value\"," + "srcType:\"INTEGER\","
        + "destType:\"INTEGER\"," + "destName:\"value\"" + "}," + "{" + "name:\"address\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"address\"" + "}," + "{"
        + "name:\"scriptSigHex\"," + "srcType:\"STRING\"," + "destType:\"STRING\","
        + "destName:\"scriptSigHex\"" + "}," + "{" + "name:\"scriptPubKeyHex\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"scriptPubKeyHex\"" + "},"
        + "{" + "name:\"type\"," + "srcType:\"STRING\"," + "destType:\"STRING\","
        + "destName:\"type\"" + "}," + "{" + "name:\"sigHashType\"," + "srcType:\"STRING\","
        + "destType:\"STRING\"," + "destName:\"sigHashType\"" + "}," + "{" + "name:\"cluster\","
        + "srcType:\"STRING\"," + "destType:\"STRING\"," + "destName:\"cluster\"" + "}" + "]" + "}"
        + "]" + "}";

    final BigQueryGrammarParser parser = new BigQueryGrammarParser();
    final BigQueryGrammar grammar = parser.getBigQueryGrammar(grammarRepr);


    final String jsonObjStr = "{\"blockTimeEpochSecond\": 1415133245, \"fee\": 550000, "
        + "\"hash\": \"5dec6aa6f0e8e15613e89cb40f499b81f143ac75d5ce482933168cc3e8671c96\", "
        + "\"blockHash\": \"0000000000000000023dbebb7043b0420cb52047e22e48e6f37c2e15cb480733\", "
        + "\"volumeIn\": 4001550006, \"outputs\": [{\"index\": 0, \"value\": 4000000000, "
        + "\"cluster\": \"7dd5b463245c36e523145877f7899d04a7e1b3b0\", " + "\"scriptPubKeyHex\": "
        + "\"1f8b08000000000000002b5b29e27454f7298fcddac2db36ce7b5db6757ed1fddc20d2b106007a51610a19000000\", "
        + "\"address\": \"42c52de50c3cad71db3c43bd44b689f42df38014\", \"type\": \"PUBKEYHASH\"}, "
        + "{\"index\": 1, \"value\": 1000006, \"cluster\": \"90b090327d39966b46bb5d74eca3063f55327e68\", "
        + "\"scriptPubKeyHex\": "
        + "\"1f8b08000000000000002b5b2912586667c8e021713a700aefdf893ad12ae7af49fdec5803000be0be6b19000000\", "
        + "\"address\": \"51763e31004818cb51940dfd912c5b24cfd61af9\", \"type\": \"PUBKEYHASH\"}], "
        + "\"volumeOut\": 4001000006, \"blockPosition\": 1305, \"version\": 1, \"numOutputs\": 2, "
        + "\"blockHeight\": 328542, \"numInputs\": 342, \"size\": 50516}";

    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonTableRowCoder coder = new JsonTableRowCoder(grammar);

    final TableRow row = coder.decode(jsonObjInputStream, context);
  }
}
