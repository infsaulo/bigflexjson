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

      if (fields.get(index).getBqType().equals(BigQueryTypes.RECORD.name())) {

        final List<Field> innerFields = fields.get(index).getFields();
        final List<TableFieldSchema> innerFieldsSchemaList = new ArrayList<>();

        for (int innerIndex = 0; innerIndex < innerFields.size(); innerIndex++) {

          innerFieldsSchemaList
              .add(new TableFieldSchema().setName(innerFields.get(innerIndex).getDestName())
                  .setType(innerFields.get(innerIndex).getBqType()));
        }

        fieldSchemas[index] = new TableFieldSchema().setName(fields.get(index).getDestName())
            .setType(fields.get(index).getBqType()).setFields(innerFieldsSchemaList)
            .setMode("REPEATED");

      } else {

        fieldSchemas[index] = new TableFieldSchema().setName(fields.get(index).getDestName())
            .setType(fields.get(index).getBqType());
      }
    }

    schema.setFields(Arrays.asList(fieldSchemas));
    return schema;
  }

}
