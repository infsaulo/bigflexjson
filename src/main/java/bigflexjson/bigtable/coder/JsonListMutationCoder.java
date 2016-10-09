package bigflexjson.bigtable.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;

import com.google.api.client.util.Charsets;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell.Builder;
import com.google.protobuf.ByteString;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.bigtable.grammar.BigTableField;
import bigflexjson.bigtable.grammar.BigTableGrammar;

public class JsonListMutationCoder extends AtomicCoder<List<Mutation>> {

  private static final long serialVersionUID = 7366189023262848525L;

  private final BigTableGrammar grammar;

  public JsonListMutationCoder(final BigTableGrammar grammar) {

    this.grammar = grammar;
  }

  public JsonListMutationCoder() {

    grammar = null;
  }

  @Override
  public void encode(final List<Mutation> value, final OutputStream outStream,
      final Context context) throws CoderException, IOException {

  }

  @Override
  public List<Mutation> decode(final InputStream inStream, final Context context)
      throws CoderException, IOException {

    final byte[] bytes = StreamUtils.getBytes(inStream);

    final JsonObject object = JsonTools.parse(bytes).asJsonObject();

    final List<Mutation> mutations = buildListMutationWithGrammar(object);

    return mutations;
  }

  private List<Mutation> buildListMutationWithGrammar(final JsonObject object) {

    final List<Mutation> mutations = new ArrayList<>();

    final List<BigTableField> fields = grammar.getFields();

    for (final BigTableField field : fields) {

      final Builder builder = Mutation.SetCell.newBuilder();
      builder.setFamilyName(field.getDestName());
      builder.setTimestampMicros(-1);

      // Get the value
      byte[] value;
      switch (field.getSrcType()) {
        case "INTEGER":
          value = getValueFromInteger(object, field);
          break;
        case "STRING":
          value = getValueFromString(object, field);
          break;
        case "BYTES":
          value = getValueFromBytes(object, field);
          break;
        case "DECIMAL":
          value = getValueFromDecimal(object, field);
          break;
        default:
          throw new IllegalStateException(
              "srcType not recognized from the field " + field.getName());
      }

      if (field.isValueQualifier()) {

        builder.setColumnQualifier(ByteString.copyFrom(value));
        builder.setValue(ByteString.copyFrom("".getBytes(Charsets.UTF_8)));

      } else {

        builder.setColumnQualifier(
            ByteString.copyFrom(field.getDestQualifier().getBytes(Charsets.UTF_8)));
        builder.setValue(ByteString.copyFrom(value));
      }

      final com.google.bigtable.v2.Mutation.Builder setCellBuilder = Mutation.newBuilder();
      setCellBuilder.setSetCell(builder);

      mutations.add(setCellBuilder.build());
    }

    return mutations;
  }

  private byte[] getValueFromInteger(final JsonObject object, final BigTableField field) {

    final byte[] value = object.getAsLong(field.getName()).toString().getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromString(final JsonObject object, final BigTableField field) {

    final byte[] value = object.getAsString(field.getName()).getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromBytes(final JsonObject object, final BigTableField field) {

    final byte[] value = object.getAsString(field.getName()).getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromDecimal(final JsonObject object, final BigTableField field) {

    final byte[] value = object.getAsDouble(field.getName()).toString().getBytes(Charsets.UTF_8);

    return value;
  }

}
