package bigflexjson.bigtable.coder;

import static org.junit.Assert.fail;

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

import com.google.bigtable.v2.Mutation;

import bigflexjson.bigtable.grammar.BigTableGrammar;
import bigflexjson.bigtable.grammar.BigTableGrammarParser;

public class JsonListMutationCoderTest {

  @Mock
  final Context context = new Context(true);

  @Test
  public void testJsonListMutationCoder() throws CoderException, IOException {

    final String grammarRepr =
        "{\"fields\":[{\"name\":\"field1\",\"srcType\":\"INTEGER\", \"destName\":\"field_1\", "
            + "\"destType\":\"STRING\", \"isQualifier\":false, \"destQualifier\": \"qualifierName\"},"
            + "{\"name\":\"field2\",\"srcType\":\"STRING\", \"destName\":\"field_2\", \"destType\":\"STRING\", "
            + "\"isQualifier\":true}]}";

    final BigTableGrammarParser parser = new BigTableGrammarParser();
    final BigTableGrammar grammar = parser.getBigTableGrammar(grammarRepr);

    final String jsonObjStr = "{\"field1\":42, \"field2\": \"qualifier\"}";
    final InputStream jsonObjInputStream =
        new ByteArrayInputStream(jsonObjStr.getBytes(StandardCharsets.UTF_8));

    final JsonListMutationCoder coder = new JsonListMutationCoder(grammar);

    final List<Mutation> mutations = coder.decode(jsonObjInputStream, context);

    for (final Mutation mutation : mutations) {
      final String familyName = mutation.getSetCell().getFamilyName();
      switch (familyName) {
        case "field_1":
          Assert.assertEquals("42", mutation.getSetCell().getValue().toStringUtf8());
          break;
        case "field_2":
          Assert.assertEquals("qualifier",
              mutation.getSetCell().getColumnQualifier().toStringUtf8());
          break;
        default:
          fail();
          break;
      }
    }
  }
}
