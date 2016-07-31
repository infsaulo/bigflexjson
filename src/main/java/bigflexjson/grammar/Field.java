package bigflexjson.grammar;

import java.io.Serializable;
import java.util.List;

public class Field implements Serializable {

  private static final long serialVersionUID = 769620709074629987L;

  // Original field's name
  String name;

  // Desirable end field's name
  String destName;

  // Type of data associated with the original field
  String srcType;

  // Google BigQuery desirable type of the end field
  String bqType;

  // Determines the kind of serialization used in the original field.
  String srcSerialization;

  // Indicates if Field should be BigQuery's Repeated
  boolean isRepeated;

  // If bqType is RECORD should contains a list of Field
  List<Field> fields;

  public String getName() {

    return name;
  }

  public String getDestName() {

    return destName;
  }

  public String getSrcType() {

    return srcType;
  }

  public String getBqType() {

    return bqType;
  }

  public String getSrcSerialization() {

    return srcSerialization;
  }

  public boolean isRepeated() {
    return isRepeated;
  }

  public List<Field> getFields() {

    return fields;
  }
}
