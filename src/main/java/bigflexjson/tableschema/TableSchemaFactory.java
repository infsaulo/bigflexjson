package bigflexjson.tableschema;

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;

import bigflexjson.grammar.BigQueryTypes;
import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

public class TableSchemaFactory {

  public static TableSchema getTableSchema(final Grammar grammar) {

    final TableSchema schema = new TableSchema();
    final List<Field> fields = grammar.getFields();
    final TableFieldSchema[] fieldSchemas = new TableFieldSchema[fields.size()];
    final List<String> bigQueryTypes = Arrays.asList(BigQueryTypes.names());

    for (int index = 0; index < fields.size(); index++) {

      Preconditions.checkArgument(bigQueryTypes.contains(fields.get(index).getBqType()));

      fieldSchemas[index] = new TableFieldSchema().setName(fields.get(index).getDestName())
          .setType(fields.get(index).getBqType());
    }

    schema.setFields(Arrays.asList(fieldSchemas));
    return schema;
  }

}
