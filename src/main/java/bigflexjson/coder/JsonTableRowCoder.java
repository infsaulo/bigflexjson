package bigflexjson.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;

import com.google.api.services.bigquery.model.TableRow;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

public class JsonTableRowCoder extends AtomicCoder<TableRow> {

  private static final long serialVersionUID = 4331898456496382910L;

  private final Grammar grammar;

  public JsonTableRowCoder(final Grammar grammar) {

    this.grammar = grammar;
  }

  public JsonTableRowCoder() {

    grammar = null;
  }

  @Override
  public void encode(final TableRow value, final OutputStream outStream, final Context context)
      throws CoderException, IOException {

  }

  @Override
  public TableRow decode(final InputStream inStream, final Context context)
      throws CoderException, IOException {

    final byte[] bytes = StreamUtils.getBytes(inStream);

    final JsonObject object = JsonTools.parse(bytes).asJsonObject();

    final TableRow row = buildTableRowWithGrammar(object);

    return row;
  }

  private TableRow buildTableRowWithGrammar(final JsonObject inputObject) {

    final TableRow row = new TableRow();

    for (final Field field : grammar.getFields()) {

      switch (field.getSrcType()) {
        case "INTEGER":
          fromIntegerType(field, inputObject, row);
          break;
        case "DECIMAL":
          fromDecimalType(field, inputObject, row);
          break;
        case "STRING":
          fromStringType(field, inputObject, row);
          break;
        case "RECORD":
          fromRecordType(field, inputObject, row);
          break;
        default:
          throw new IllegalStateException(
              field.getSrcType() + " is not supported as a source type");
      }
    }
    return row;
  }

  private void fromIntegerType(final Field field, final JsonObject obj, final TableRow row) {

    switch (field.getBqType()) {

      case "STRING":
        row.set(field.getDestName(), String.valueOf(obj.getAsInteger(field.getName())));
        break;
      case "INTEGER":
        row.set(field.getDestName(), obj.getAsInteger(field.getName()));
        break;
      case "FLOAT":
        row.set(field.getDestName(), (double) obj.getAsInteger(field.getName()));
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from INTEGER");
    }
  }

  private void fromDecimalType(final Field field, final JsonObject obj, final TableRow row) {

    switch (field.getBqType()) {

      case "STRING":
        row.set(field.getDestName(), String.valueOf(obj.getAsDouble(field.getName())));
        break;
      case "FLOAT":
        row.set(field.getDestName(), obj.getAsDouble(field.getName()));
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from DECIMAL");
    }
  }

  private void fromStringType(final Field field, final JsonObject obj, final TableRow row) {

    switch (field.getBqType()) {

      case "STRING":
        row.set(field.getDestName(), obj.getAsString(field.getName()));
        break;
      case "BYTES":
        row.set(field.getDestName(), obj.getAsString(field.getName())
            .getBytes(Charset.forName(field.getSrcSerialization())));
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from STRING");
    }
  }

  private void fromRecordType(final Field field, final JsonObject obj, final TableRow row) {

    switch (field.getBqType()) {
      case "RECORD":
        row.set(field.getDestName(), obj.getAsJsonArray(field.getName()));
        break;
      default:
        throw new IllegalStateException(field.getBqType() + " cannot be type casted from RECORD");
    }
  }
}
