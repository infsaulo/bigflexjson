package bigflexjson.text.jsonld.coder;

import com.wizzardo.tools.json.JsonTools;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import bigflexjson.text.jsonld.grammar.JsonLdGrammar;
import bigflexjson.text.jsonld.grammar.JsonLdGrammarParser;

public class JsonLdCoderTest {

  @Test
  public void testJsonLdCoder() {

    String txJsonLd = "{\"@id\":\"http://ethereum.ethstats.io/Tx_0x02e6df5d45801108e65a18fb688f827d"
                      + "ac301945eb266219de245157279a4253\","
                      + "\"@type\":[\"http://ethon.consensys.net/Msg\",\"http://ethon.consensys.net/Tx\","
                      + "\"http://ethon.consensys.net/ValueTx\"],\"http://ethon.consensys.net/txHash\":"
                      + "[{\"@value\":\"02e6df5d45801108e65a18fb688f827dac301945eb266219de245157279a4253\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/txIndex\":[{\"@value\":\"1\",\"@type\":"
                      + "\"http://www.w3.org/2001/XMLSchema#integer\"}],\"http://ethon.consensys.net/txNonce\":"
                      + "[{\"@value\":\"1\",\"@type\":\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/txGasUsed\":[{\"@value\":\"21000\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/msgGasPrice\":[{\"@value\":\"22000000000\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/msgGasLimit\":[{\"@value\":\"21000\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/value\":[{\"@value\":\"100000000000000000\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/txV\":[{\"@value\":\"1\",\"@type\":"
                      + "\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/txR\":[{\"@value\":"
                      + "\"f75a0370f4b8b0627accbc1192bf609833bfe49b9ec5e2b12d28d1ad0a4be0b5\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/txS\":[{\"@value\":"
                      + "\"64593f956935163bacf084179e3b5c5b8c5fbc52eabf97e607d85a6769f5992a\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/msgPayload\":[{\"@value\":\"\",\"@type\":"
                      + "\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/txPublicKey\":[{\"@value\":"
                      + "\"7023771e858e9efd39e57e1471f178082c4b113fd3a154f8915d9e9cd1922fab057cef66"
                      + "427c843faf8ae2af1f6b10f71c2d0477378989b6a8e8a63eeff56e9f\","
                      + "\"@type\":\"http://www.w3.org/2001/XMLSchema#hexBinary\"}],"
                      + "\"http://ethon.consensys.net/number\":[{\"@value\":\"3482563\",\"@type\":"
                      + "\"http://www.w3.org/2001/XMLSchema#integer\"}],"
                      + "\"http://ethon.consensys.net/blockCreationTime\":"
                      + "[{\"@value\":\"2017-04-05T17:33:16.000Z\",\"@type\":\"http://www.w3.org/"
                      + "2001/XMLSchema#dateTime\"}],"
                      + "\"http://ethon.consensys.net/from\":[{\"@id\":"
                      + "\"http://ethereum.ethstats.io/Account_0x6621c106003430a1b791ab44fd5a4d872f530a3f\"}],"
                      + "\"http://ethon.consensys.net/to\":[{\"@id\":"
                      + "\"http://ethereum.ethstats.io/Account_0x716aa9ba55f5990e6d2a02620d72bec43f95d961\"}],"
                      + "\"http://ethon.consensys.net/hasReceipt\":[{\"@id\":"
                      + "\"http://ethereum.ethstats.io/Receipt_0x02e6df5d45801108e65a18fb688f827dac"
                      + "301945eb266219de245157279a4253\"}]}";

    String jsonLdGrammarStr = "{\"fields\":\n"
                              + "[\n"
                              + "{\n"
                              + "    \"name\":\"http://ethon.consensys.net/txHash\",\n"
                              + "    \"destName\": \"hash\",\n"
                              + "    \"srcType\": \"STRING\",\n"
                              + "    \"destType\": \"STRING\",\n"
                              + "    \"srcValue\": \"@value\",\n"
                              + "    \"delimiter\": \"\"\n"
                              + "},\n"
                              + "{\n"
                              + "    \"name\":\"http://ethon.consensys.net/txIndex\",\n"
                              + "    \"destName\": \"index\",\n"
                              + "    \"srcType\": \"STRING\",\n"
                              + "    \"destType\": \"INTEGER\",\n"
                              + "    \"srcValue\": \"@value\",\n"
                              + "    \"delimiter\": \"\"\n"
                              + "},\n"
                              + "{\n"
                              + "    \"name\":\"http://ethon.consensys.net/from\",\n"
                              + "    \"destName\": \"from\",\n"
                              + "    \"srcType\": \"STRING\",\n"
                              + "    \"destType\": \"STRING\",\n"
                              + "    \"srcValue\": \"@id\",\n"
                              + "    \"delimiter\": \"_\"\n"
                              + "}\n"
                              + "]\n"
                              + "}";

    JsonLdGrammarParser grammarParser = new JsonLdGrammarParser();
    JsonLdGrammar grammar = grammarParser.getJsonLdGrammar(jsonLdGrammarStr);

    JsonLdCoder coder = new JsonLdCoder(grammar);
    Map<String, Object> decodedJson = coder.decode(JsonTools.parse(txJsonLd).asJsonObject());

    Assert.assertEquals("02e6df5d45801108e65a18fb688f827dac301945eb266219de245157279a4253",
                        decodedJson.get("hash"));
    Assert.assertTrue(1L == (long) decodedJson.get("index"));
    Assert.assertEquals("0x6621c106003430a1b791ab44fd5a4d872f530a3f",
                        decodedJson.get("from"));
  }
}
