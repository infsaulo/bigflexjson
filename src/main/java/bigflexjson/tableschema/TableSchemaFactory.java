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

  public static TableSchema getTableSchema(final Grammar grammar) {

    final TableSchema schema = new TableSchema();
    final List<Field> fields = grammar.getFields();
    final TableFieldSchema[] fieldSchemas = new TableFieldSchema[fields.size()];
    final List<String> bigQueryTypes = Arrays.asList(BigQueryTypes.names());

    for (int index = 0; index < fields.size(); index++) {

      Preconditions.checkArgument(bigQueryTypes.contains(fields.get(index).getBqType()));
      final TableFieldSchema fieldSchema = new TableFieldSchema();

      if (fields.get(index).getBqType().equals(BigQueryTypes.RECORD.name())) {

        final List<Field> innerFields = fields.get(index).getFields();
        final List<TableFieldSchema> innerFieldsSchemaList = new ArrayList<>();

        for (int innerIndex = 0; innerIndex < innerFields.size(); innerIndex++) {

          final TableFieldSchema innerFieldSchema = new TableFieldSchema();
          if (innerFields.get(innerIndex).getBqType().equals(BigQueryTypes.RECORD.name())) {
            final List<Field> recInnerFields = innerFields.get(innerIndex).getFields();
            final List<TableFieldSchema> recInnerFieldsSchemaList = new ArrayList<>();
            for (int recInnerIndex = 0; recInnerIndex < recInnerFields.size(); recInnerIndex++) {
              final TableFieldSchema recInnerFieldSchema = new TableFieldSchema();
              recInnerFieldSchema.setName(recInnerFields.get(recInnerIndex).getDestName())
                  .setType(recInnerFields.get(recInnerIndex).getBqType());

              if (recInnerFields.get(recInnerIndex).isRepeated()) {
                recInnerFieldSchema.setMode("REPEATED");
              }

              recInnerFieldsSchemaList.add(recInnerFieldSchema);
            }
            innerFieldSchema.setName(innerFields.get(innerIndex).getDestName())
                .setType(innerFields.get(innerIndex).getBqType())
                .setFields(recInnerFieldsSchemaList);
          } else {
            innerFieldSchema.setName(innerFields.get(innerIndex).getDestName())
                .setType(innerFields.get(innerIndex).getBqType());

          }
          if (innerFields.get(innerIndex).isRepeated()) {
            innerFieldSchema.setMode("REPEATED");
          }

          innerFieldsSchemaList.add(innerFieldSchema);
        }

        fieldSchema.setName(fields.get(index).getDestName()).setType(fields.get(index).getBqType())
            .setFields(innerFieldsSchemaList);

      } else {

        fieldSchema.setName(fields.get(index).getDestName()).setType(fields.get(index).getBqType());
      }

      if (fields.get(index).isRepeated()) {
        fieldSchema.setMode("REPEATED");
      }

      fieldSchemas[index] = fieldSchema;
    }

    schema.setFields(Arrays.asList(fieldSchemas));
    return schema;
  }

}
