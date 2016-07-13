package bigflexjson.tableschema;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

public class TableSchemaFactory {

  public static TableSchema getTableSchema(final Grammar grammar) {

    final TableSchema schema = new TableSchema();
    final List<Field> fields = grammar.getFields();
    final TableFieldSchema[] fieldSchemas = new TableFieldSchema[fields.size()];

    for (int index = 0; index < fields.size(); index++) {

      fieldSchemas[index] = new TableFieldSchema().setName(fields.get(index).getDestName())
          .setType(fields.get(index).getBqType());
    }

    schema.setFields(Arrays.asList(fieldSchemas));
    return schema;
  }

}
