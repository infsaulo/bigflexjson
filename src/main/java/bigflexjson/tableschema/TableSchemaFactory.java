package bigflexjson.tableschema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;

import bigflexjson.grammar.BigQueryTypes;
import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

public class TableSchemaFactory implements Serializable {

  private static final long serialVersionUID = 1465681315148137897L;

  private static TableFieldSchema getRecordFieldSchema(final Field field) {

    final TableFieldSchema recordSchema = new TableFieldSchema();
    final List<Field> recordFields = field.getFields();
    final List<String> bigQueryTypes = Arrays.asList(BigQueryTypes.names());
    final List<TableFieldSchema> recordSchemaList = new ArrayList<>();

    for (final Field innerField : recordFields) {

      Preconditions.checkArgument(bigQueryTypes.contains(field.getBqType()));
      recordSchemaList.add(getFieldSchema(innerField));
    }

    recordSchema.setName(field.getDestName()).setType(field.getBqType())
        .setFields(recordSchemaList);

    return recordSchema;
  }

  private static TableFieldSchema getFieldSchema(final Field field) {

    TableFieldSchema fieldSchema = new TableFieldSchema();
    if (field.getBqType().equals(BigQueryTypes.RECORD.name())) {

      fieldSchema = getRecordFieldSchema(field);
    } else {

      fieldSchema.setName(field.getDestName()).setType(field.getBqType());
    }

    if (field.isRepeated()) {
      fieldSchema.setMode("REPEATED");
    }

    return fieldSchema;
  }

  public static TableSchema getTableSchema(final Grammar grammar) {

    final TableSchema schema = new TableSchema();
    final List<Field> fields = grammar.getFields();
    final List<String> bigQueryTypes = Arrays.asList(BigQueryTypes.names());

    final List<TableFieldSchema> fieldSchemaList = new ArrayList<>();

    for (final Field field : fields) {
      Preconditions.checkArgument(bigQueryTypes.contains(field.getBqType()));
      fieldSchemaList.add(getFieldSchema(field));
    }

    schema.setFields(fieldSchemaList);

    return schema;
  }

}
