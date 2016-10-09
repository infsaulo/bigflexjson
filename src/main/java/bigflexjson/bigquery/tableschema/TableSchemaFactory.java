package bigflexjson.bigquery.tableschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;

import bigflexjson.bigquery.grammar.BigQueryField;
import bigflexjson.bigquery.grammar.BigQueryGrammar;
import bigflexjson.grammar.DestTypes;

public class TableSchemaFactory implements Serializable {

  private static final long serialVersionUID = 1465681315148137897L;

  private static TableFieldSchema getRecordFieldSchema(final BigQueryField field) {

    final TableFieldSchema recordSchema = new TableFieldSchema();
    final List<BigQueryField> recordFields = field.getFields();
    final List<String> bigQueryTypes = Arrays.asList(DestTypes.names());
    final List<TableFieldSchema> recordSchemaList = new ArrayList<>();

    for (final BigQueryField innerField : recordFields) {

      Preconditions.checkArgument(bigQueryTypes.contains(field.getDestType()));
      recordSchemaList.add(getFieldSchema(innerField));
    }

    recordSchema.setName(field.getDestName()).setType(field.getDestType())
        .setFields(recordSchemaList);

    return recordSchema;
  }

  private static TableFieldSchema getFieldSchema(final BigQueryField field) {

    TableFieldSchema fieldSchema = new TableFieldSchema();
    if (field.getDestType().equals(DestTypes.RECORD.name())) {

      fieldSchema = getRecordFieldSchema(field);
    } else {

      fieldSchema.setName(field.getDestName()).setType(field.getDestType());
    }

    if (field.isRepeated()) {
      fieldSchema.setMode("REPEATED");
    }

    return fieldSchema;
  }

  public static TableSchema getTableSchema(final BigQueryGrammar grammar) {

    final TableSchema schema = new TableSchema();
    final List<BigQueryField> fields = grammar.getFields();
    final List<String> bigQueryTypes = Arrays.asList(DestTypes.names());

    final List<TableFieldSchema> fieldSchemaList = new ArrayList<>();

    for (final BigQueryField field : fields) {
      Preconditions.checkArgument(bigQueryTypes.contains(field.getDestType()));
      fieldSchemaList.add(getFieldSchema(field));
    }

    schema.setFields(fieldSchemaList);

    return schema;
  }

}
