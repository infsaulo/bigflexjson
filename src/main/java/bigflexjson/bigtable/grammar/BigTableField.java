package bigflexjson.bigtable.grammar;

import bigflexjson.grammar.Field;

public class BigTableField extends Field {

  private static final long serialVersionUID = 3337249023499169764L;

  // Column qualifier if needed
  String destQualifier;

  // Value stored as qualifier
  boolean isQualifier;

  public String getDestQualifier() {
    return destQualifier;
  }

  public boolean isValueQualifier() {
    return isQualifier;
  }
}
