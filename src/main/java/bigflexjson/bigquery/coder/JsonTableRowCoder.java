package bigflexjson.bigquery.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.StreamUtils;

import com.google.api.services.bigquery.model.TableRow;
import com.wizzardo.tools.json.JsonArray;
import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import bigflexjson.bigquery.grammar.BigQueryField;
import bigflexjson.bigquery.grammar.BigQueryGrammar;

public class JsonTableRowCoder extends AtomicCoder<TableRow> {

  private static final long serialVersionUID = 4331898456496382910L;

  private final BigQueryGrammar grammar;

  public JsonTableRowCoder(final BigQueryGrammar grammar) {

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

    for (final BigQueryField field : grammar.getFields()) {

      if (inputObject.containsKey(field.getName())) {
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
    }
    return row;
  }

  private void fromIntegerType(final BigQueryField field, final JsonObject obj,
      final TableRow row) {

    switch (field.getDestType()) {

      case "STRING":
        if (field.isRepeated()) {
          final List<String> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final String object = String.valueOf((long) innerField.get());
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), String.valueOf(obj.getAsLong(field.getName())));
        }
        break;
      case "INTEGER":
      case "TIMESTAMP":
        if (field.isRepeated()) {
          final List<Long> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final long object = (long) innerField.get();
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), obj.getAsLong(field.getName()));
        }
        break;
      case "FLOAT":
        if (field.isRepeated()) {
          final List<Double> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final double object = ((long) innerField.get());
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), (double) obj.getAsLong(field.getName()));
        }
        break;
      default:
        throw new IllegalStateException(
            field.getDestType() + " cannot be type casted from INTEGER");
    }
  }

  private void fromDecimalType(final BigQueryField field, final JsonObject obj,
      final TableRow row) {

    switch (field.getDestType()) {

      case "STRING":
        if (field.isRepeated()) {
          final List<String> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final String object = String.valueOf((double) innerField.get());
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), String.valueOf(obj.getAsDouble(field.getName())));
        }
        break;
      case "FLOAT":
        if (field.isRepeated()) {
          final List<Double> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final double object = (double) innerField.get();
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), obj.getAsDouble(field.getName()));
        }
        break;
      case "TIMESTAMP":
        if (field.isRepeated()) {
          final List<Double> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final double object = (double) innerField.get();
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), obj.getAsLong(field.getName()));
        }
        break;
      default:
        throw new IllegalStateException(
            field.getDestType() + " cannot be type casted from DECIMAL");
    }
  }

  private void fromStringType(final BigQueryField field, final JsonObject obj, final TableRow row) {

    switch (field.getDestType()) {

      case "STRING":
      case "TIMESTAMP":
        if (field.isRepeated()) {
          final List<String> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final String object = (String) innerField.get();
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), obj.getAsString(field.getName()));
        }
        break;
      case "BYTES":
        if (field.isRepeated()) {
          final List<byte[]> fields = new ArrayList<>();
          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());
          for (final JsonItem innerField : jsonFields) {
            final byte[] object =
                ((String) innerField.get()).getBytes(Charset.forName(field.getSrcSerialization()));
            fields.add(object);
          }
          row.set(field.getDestName(), fields);

        } else {
          row.set(field.getDestName(), obj.getAsString(field.getName())
              .getBytes(Charset.forName(field.getSrcSerialization())));
        }
        break;
      default:
        throw new IllegalStateException(field.getDestType() + " cannot be type casted from STRING");
    }
  }

  private void fromRecordType(final BigQueryField field, final JsonObject obj, final TableRow row) {

    switch (field.getDestType()) {

      case "RECORD":
        if (field.isRepeated()) {
          final List<TableRow> fields = new ArrayList<>();
          final List<BigQueryField> recordFields = field.getFields();

          final JsonArray jsonFields = obj.getAsJsonArray(field.getName());

          for (final JsonItem innerField : jsonFields) {
            final JsonObject jsonObject = innerField.asJsonObject();
            final TableRow innerRow = new TableRow();
            for (final BigQueryField recordField : recordFields) {
              if (jsonObject.containsKey(recordField.getName())) {
                switch (recordField.getSrcType()) {
                  case "INTEGER":
                    fromIntegerType(recordField, jsonObject, innerRow);
                    break;
                  case "DECIMAL":
                    fromDecimalType(recordField, jsonObject, innerRow);
                    break;
                  case "STRING":
                    fromStringType(recordField, jsonObject, innerRow);
                    break;
                  case "RECORD":
                    final List<TableRow> innerFields = new ArrayList<>();
                    final List<BigQueryField> innerRecordFields = recordField.getFields();
                    final JsonArray innerJsonFields =
                        jsonObject.getAsJsonArray(recordField.getName());
                    for (final JsonItem recInnerField : innerJsonFields) {
                      final JsonObject recJsonObject = recInnerField.asJsonObject();
                      final TableRow recInnerRow = new TableRow();
                      for (final BigQueryField recRecordField : innerRecordFields) {
                        if (recJsonObject.containsKey(recRecordField.getName())) {
                          switch (recRecordField.getSrcType()) {
                            case "INTEGER":
                              fromIntegerType(recRecordField, recJsonObject, recInnerRow);
                              break;
                            case "DECIMAL":
                              fromDecimalType(recRecordField, recJsonObject, recInnerRow);
                              break;
                            case "STRING":
                              fromStringType(recRecordField, recJsonObject, recInnerRow);
                              break;
                            case "RECORD":
                              fromRecordType(recRecordField, recJsonObject, recInnerRow);
                              break;
                            default:
                              throw new IllegalStateException(recRecordField.getSrcType()
                                  + " is not supported as a source type");
                          }
                        }
                      }
                      innerFields.add(recInnerRow);
                    }
                    innerRow.set(recordField.getDestName(), innerFields);
                    break;
                  default:
                    throw new IllegalStateException(
                        field.getSrcType() + " is not supported as a source type");
                }
              }
            }
            fields.add(innerRow);
          }

          row.set(field.getDestName(), fields);

        } else {

          final List<BigQueryField> recordFields = field.getFields();

          final JsonObject jsonObject = obj.getAsJsonObject(field.getName());

          final TableRow innerRow = new TableRow();
          for (final BigQueryField recordField : recordFields) {
            if (jsonObject.containsKey(recordField.getName())) {
              switch (recordField.getSrcType()) {
                case "INTEGER":
                  fromIntegerType(recordField, jsonObject, innerRow);
                  break;
                case "DECIMAL":
                  fromDecimalType(recordField, jsonObject, innerRow);
                  break;
                case "STRING":
                  fromStringType(recordField, jsonObject, innerRow);
                  break;
                case "RECORD":
                  final List<TableRow> innerFields = new ArrayList<>();
                  final List<BigQueryField> innerRecordFields = recordField.getFields();
                  final JsonArray innerJsonFields =
                      jsonObject.getAsJsonArray(recordField.getName());
                  for (final JsonItem recInnerField : innerJsonFields) {
                    final JsonObject recJsonObject = recInnerField.asJsonObject();
                    final TableRow recInnerRow = new TableRow();
                    for (final BigQueryField recRecordField : innerRecordFields) {
                      if (recJsonObject.containsKey(recRecordField.getName())) {
                        switch (recRecordField.getSrcType()) {
                          case "INTEGER":
                            fromIntegerType(recRecordField, recJsonObject, recInnerRow);
                            break;
                          case "DECIMAL":
                            fromDecimalType(recRecordField, recJsonObject, recInnerRow);
                            break;
                          case "STRING":
                            fromStringType(recRecordField, recJsonObject, recInnerRow);
                            break;
                          case "RECORD":
                            fromRecordType(recRecordField, recJsonObject, recInnerRow);
                            break;
                          default:
                            throw new IllegalStateException(
                                recRecordField.getSrcType() + " is not supported as a source type");
                        }
                      }
                    }
                    innerFields.add(recInnerRow);
                  }
                  innerRow.set(recordField.getDestName(), innerFields);
                  break;
                default:
                  throw new IllegalStateException(
                      field.getSrcType() + " is not supported as a source type");
              }
            }
          }
          row.set(field.getDestName(), innerRow);
        }
        break;
      default:
        throw new IllegalStateException(field.getDestType() + " cannot be type casted from RECORD");
    }
  }
}
