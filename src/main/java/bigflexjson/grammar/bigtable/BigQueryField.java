package bigflexjson.grammar.bigtable;

import java.util.List;

import bigflexjson.grammar.Field;

public class BigQueryField extends Field {

  private static final long serialVersionUID = 769620709074629987L;

  // Indicates if Field should be BigQuery's Repeated
  boolean isRepeated;

  // If bqType is RECORD should contains a list of Field
  List<BigQueryField> fields;

  public boolean isRepeated() {
    return isRepeated;
  }

  public List<BigQueryField> getFields() {

    return fields;
  }
}
