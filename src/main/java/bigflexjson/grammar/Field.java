package bigflexjson.grammar;

public class Field {

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
}
