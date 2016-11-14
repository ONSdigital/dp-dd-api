package models;

public class DimensionFilter {
    private String concept;
    private String variable;

    public DimensionFilter(String concept, String variable) {
        this.concept = concept;
        this.variable = variable;
    }

    public String getConcept() {
        return concept;
    }

    public String getVariable() {
        return variable;
    }

}

