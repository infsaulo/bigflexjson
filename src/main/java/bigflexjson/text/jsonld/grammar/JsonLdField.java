package bigflexjson.text.jsonld.grammar;


import bigflexjson.grammar.Field;

public class JsonLdField extends Field {

  private static final long serialVersionUID = 5752866539393833278L;

  // Defines if the value comes from @value or from @id
  String srcValue;

  // Defines if need to split the string of the value to extract the desired portion
  String delimiter;

  public String getSrcValue() {

    return srcValue;
  }

  public String getDelimiter() {

    return delimiter;
  }
}
