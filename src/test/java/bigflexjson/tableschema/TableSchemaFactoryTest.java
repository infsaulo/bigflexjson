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
import bigflexjson.grammar.GrammarParser;

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

  @Test
  public void testRecordTableSchemaFactory() {
    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"field_1\", \"isRepeated\": true },"
        + "{\"name\":\"field2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"field_2\", "
        + "\"srcSerialization\":\"hex\"},"
        + "{\"name\":\"field3\", \"srcType\":\"RECORD\",\"bqType\":\"RECORD\", \"destName\":\"field_3\", \"isRepeated\": true,  \"fields\": "
        + "[{\"name\":\"innerfield1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"inner_field_1\"},"
        + "{\"name\":\"innerfield2\",\"srcType\":\"STRING\",\"bqType\":\"STRING\", \"destName\":\"inner_field_2\", "
        + "\"srcSerialization\":\"hex\"},{\"name\":\"innerfield3\",\"srcType\":\"RECORD\",\"bqType\":\"RECORD\", \"destName\":\"inner_field_3\", \"fields\": [{\"name\":\"innerinnerfield1\",\"srcType\":\"INTEGER\",\"bqType\":\"INTEGER\", \"destName\":\"inner_inner_field_1\", \"isRepeated\": true }]}]}]}";

    final Grammar grammar = GrammarParser.getGrammar(grammarJsonRepr);

    final TableSchema table = TableSchemaFactory.getTableSchema(grammar);

    for (final TableFieldSchema fieldSchema : table.getFields()) {
      final String fieldName = fieldSchema.getName();
      switch (fieldName) {
        case "field_1":
          Assert.assertTrue(fieldSchema.getType().equals("INTEGER"));
          Assert.assertTrue(fieldSchema.getMode().equals("REPEATED"));
          break;
        case "field_2":
          Assert.assertTrue(fieldSchema.getType().equals("STRING"));
          break;
        case "field_3":
          Assert.assertTrue(fieldSchema.getType().equals("RECORD"));
          Assert.assertTrue(fieldSchema.getMode().equals("REPEATED"));
          for (final TableFieldSchema innerField : fieldSchema.getFields()) {
            final String innerFieldName = innerField.getName();
            switch (innerFieldName) {
              case "inner_field_1":
                Assert.assertTrue(innerField.getType().equals("INTEGER"));
                break;
              case "inner_field_2":
                Assert.assertTrue(innerField.getType().equals("STRING"));
                break;
              case "inner_field_3":
                Assert.assertTrue(innerField.getType().equals("RECORD"));
                for (final TableFieldSchema innerInnerField : innerField.getFields()) {
                  final String innerInnerFieldName = innerInnerField.getName();
                  switch (innerInnerFieldName) {
                    case "inner_inner_field_1":
                      Assert.assertTrue(innerInnerField.getType().equals("INTEGER"));
                      Assert.assertTrue(innerInnerField.getMode().equals("REPEATED"));
                      break;
                    default:
                      break;
                  }
                }
                break;
              default:
                break;
            }
          }
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
