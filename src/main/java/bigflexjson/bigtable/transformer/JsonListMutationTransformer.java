package bigflexjson.bigtable.transformer;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.api.client.util.Charsets;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell.Builder;
import com.google.protobuf.ByteString;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.bigtable.grammar.BigTableField;
import bigflexjson.bigtable.grammar.BigTableGrammar;
import bigflexjson.grammar.Field;

public class JsonListMutationTransformer extends DoFn<String, KV<ByteString, Iterable<Mutation>>> {

  private static final long serialVersionUID = 7366189023262848525L;

  private final BigTableGrammar grammar;

  public JsonListMutationTransformer(final BigTableGrammar grammar) {

    this.grammar = grammar;
  }

  public JsonListMutationTransformer() {

    grammar = null;
  }

  @Override
  public void processElement(final ProcessContext entry) throws Exception {

    final byte[] bytes = entry.element().getBytes(Charsets.UTF_8);

    final JsonObject object = JsonTools.parse(bytes).asJsonObject();

    final List<Mutation> mutations = buildListMutationWithGrammar(object);

    entry.output(KV.of(ByteString.copyFrom(object.getAsString("rowkey").getBytes(Charsets.UTF_8)),
        mutations));
  }

  private List<Mutation> buildListMutationWithGrammar(final JsonObject object) {

    final List<Mutation> mutations = new ArrayList<>();

    final List<BigTableField> fields = grammar.getFields();

    for (final BigTableField field : fields) {

      if (object.get(field.getName()) != null) {

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

          if (field.isColumnAndQualifier()) {

            final Field columnField = field.getColumnField();

            byte[] columnValue;
            switch (columnField.getSrcType()) {
              case "INTEGER":
                columnValue = getValueFromInteger(object, columnField);
                break;
              case "STRING":
                columnValue = getValueFromString(object, columnField);
                break;
              case "BYTES":
                columnValue = getValueFromBytes(object, columnField);
                break;
              case "DECIMAL":
                columnValue = getValueFromDecimal(object, columnField);
                break;
              default:
                throw new IllegalStateException(
                    "srcType not recognized from the field " + columnField.getName());
            }

            builder.setValue(ByteString.copyFrom(columnValue));

          } else {

            builder.setValue(ByteString.copyFrom("".getBytes(Charsets.UTF_8)));
          }

        } else {

          builder.setColumnQualifier(
              ByteString.copyFrom(field.getDestQualifier().getBytes(Charsets.UTF_8)));
          builder.setValue(ByteString.copyFrom(value));
        }

        final com.google.bigtable.v2.Mutation.Builder setCellBuilder = Mutation.newBuilder();
        setCellBuilder.setSetCell(builder);

        mutations.add(setCellBuilder.build());
      }
    }

    return mutations;
  }

  private byte[] getValueFromInteger(final JsonObject object, final Field field) {

    final byte[] value = object.getAsLong(field.getName()).toString().getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromString(final JsonObject object, final Field field) {

    final byte[] value = object.getAsString(field.getName()).getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromBytes(final JsonObject object, final Field field) {

    final byte[] value = object.getAsString(field.getName()).getBytes(Charsets.UTF_8);

    return value;
  }

  private byte[] getValueFromDecimal(final JsonObject object, final Field field) {

    final byte[] value = object.getAsDouble(field.getName()).toString().getBytes(Charsets.UTF_8);

    return value;
  }
}
