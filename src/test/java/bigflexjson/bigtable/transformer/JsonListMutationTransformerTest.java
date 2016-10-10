package bigflexjson.bigtable.transformer;

import static org.junit.Assert.fail;

import java.util.List;

import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Assert;
import org.junit.Test;

import com.google.bigtable.v2.Mutation;

import bigflexjson.bigtable.grammar.BigTableGrammar;
import bigflexjson.bigtable.grammar.BigTableGrammarParser;

public class JsonListMutationTransformerTest {

  @Test
  public void testJsonListMutationTransformer() throws Exception {

    final String grammarRepr =
        "{\"fields\":[{\"name\":\"field1\",\"srcType\":\"INTEGER\", \"destName\":\"field_1\", "
            + "\"destType\":\"STRING\", \"isQualifier\":false, \"destQualifier\": \"qualifierName\"},"
            + "{\"name\":\"field2\",\"srcType\":\"STRING\", \"destName\":\"field_2\", \"destType\":\"STRING\", "
            + "\"isQualifier\":true}]}";

    final BigTableGrammarParser parser = new BigTableGrammarParser();
    final BigTableGrammar grammar = parser.getBigTableGrammar(grammarRepr);

    final JsonListMutationTransformer transformer = new JsonListMutationTransformer(grammar);
    final DoFnTester<String, List<Mutation>> tester = DoFnTester.of(transformer);

    final String jsonObjStr = "{\"field1\":42, \"field2\": \"qualifier\"}";

    final List<List<Mutation>> mutations = tester.processBundle(jsonObjStr);

    for (final Mutation mutation : mutations.get(0)) {
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
