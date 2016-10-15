package bigflexjson.bigtable.grammar;

import bigflexjson.grammar.Field;

public class BigTableField extends Field {

  private static final long serialVersionUID = 3337249023499169764L;

  // Column qualifier if needed
  String destQualifier;

  // Value stored as qualifier
  boolean isQualifier;

  // Column field is thats necessary to store inside a qualifier
  Field columnField;

  // There is value to store in the column when this field is a qualifier
  boolean isColumnValue;

  public String getDestQualifier() {
    return destQualifier;
  }

  public boolean isValueQualifier() {
    return isQualifier;
  }

  public boolean isColumnAndQualifier() {
    return isColumnValue;
  }

  public Field getColumnField() {
    return columnField;
  }

}
