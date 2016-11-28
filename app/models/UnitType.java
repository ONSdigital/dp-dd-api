package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the unit_type database table.
 * 
 */
@Entity
@Table(name="unit_type")
@NamedQuery(name="UnitType.findAll", query="SELECT u FROM UnitType u")
public class UnitType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="unit_type")
	private String unitType;

	//bi-directional many-to-one association to Variable
	@OneToMany(mappedBy="unitTypeBean")
	private List<Variable> variables;

	public UnitType() {
	}

	public UnitType(String unitType) {
		this.unitType = unitType;
	}

	public String getUnitType() {
		return this.unitType;
	}

	public void setUnitType(String unitType) {
		this.unitType = unitType;
	}

	public List<Variable> getVariables() {
		return this.variables;
	}

	public void setVariables(List<Variable> variables) {
		this.variables = variables;
	}

	public Variable addVariable(Variable variable) {
		getVariables().add(variable);
		variable.setUnitTypeBean(this);

		return variable;
	}

	public Variable removeVariable(Variable variable) {
		getVariables().remove(variable);
		variable.setUnitTypeBean(null);

		return variable;
	}

}