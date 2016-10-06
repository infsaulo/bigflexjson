package bigflexjson.grammar.bigquery;

import java.io.Serializable;
import java.util.List;

public class BigQueryGrammar implements Serializable {

  private static final long serialVersionUID = 6399559430588231251L;

  // List of Fields
  List<BigQueryField> fields;

  public List<BigQueryField> getFields() {

    return fields;
  }
}
