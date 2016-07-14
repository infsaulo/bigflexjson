package bigflexjson.tableschema;

import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import bigflexjson.grammar.Field;
import bigflexjson.grammar.Grammar;

@RunWith(MockitoJUnitRunner.class)
public class TableSchemaFactoryTest {

  @Mock
  Field field1;

  @Mock
  Field field2;

  @Mock
  Field field3;

  @Mock
  Grammar consistentGrammar;

  @Mock
  Grammar inconsistentGrammar;

  @Test
  public void testTableSchemaFactory() {

    when(field1.getBqType()).thenReturn("INTEGER");
    when(field1.getDestName()).thenReturn("field_1");
    when(field2.getBqType()).thenReturn("STRING");
    when(field2.getDestName()).thenReturn("field_2");


    final List<Field> consistentFields = ImmutableList.of(field1, field2);

    when(consistentGrammar.getFields()).thenReturn(consistentFields);

    final TableSchema consistentTableSchema = TableSchemaFactory.getTableSchema(consistentGrammar);

    final List<TableFieldSchema> fieldsSchema = consistentTableSchema.getFields();

    for (final TableFieldSchema fieldSchema : fieldsSchema) {
      final String fieldName = fieldSchema.getName();
      switch (fieldName) {
        case "field_1":
          Assert.assertTrue(fieldSchema.getType().equals("INTEGER"));
          break;
        case "field_2":
          Assert.assertTrue(fieldSchema.getType().equals("STRING"));
          break;
        default:
          break;
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInconsistentTableSchemaFactory() {

    when(field1.getBqType()).thenReturn("INTEGER");
    when(field1.getDestName()).thenReturn("field_1");
    when(field2.getBqType()).thenReturn("STRING");
    when(field2.getDestName()).thenReturn("field_2");
    when(field3.getBqType()).thenReturn("NOTBQTYPE");
    when(field3.getDestName()).thenReturn("field_3");

    final List<Field> inconsistentFields = ImmutableList.of(field1, field2, field3);

    when(inconsistentGrammar.getFields()).thenReturn(inconsistentFields);

    TableSchemaFactory.getTableSchema(inconsistentGrammar);
  }

}
