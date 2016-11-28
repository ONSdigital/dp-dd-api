package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the value_domain database table.
 * 
 */
@Entity
@Table(name="value_domain")
@NamedQuery(name="ValueDomain.findAll", query="SELECT v FROM ValueDomain v")
public class ValueDomain implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="value_domain")
	private String valueDomain;

	//bi-directional many-to-one association to Variable
	@OneToMany(mappedBy="valueDomainBean")
	private List<Variable> variables;

	public ValueDomain() {
	}

	public ValueDomain(String valueDomain) {
		this.valueDomain = valueDomain;
	}

	public String getValueDomain() {
		return this.valueDomain;
	}

	public void setValueDomain(String valueDomain) {
		this.valueDomain = valueDomain;
	}

	public List<Variable> getVariables() {
		return this.variables;
	}

	public void setVariables(List<Variable> variables) {
		this.variables = variables;
	}

	public Variable addVariable(Variable variable) {
		getVariables().add(variable);
		variable.setValueDomainBean(this);

		return variable;
	}

	public Variable removeVariable(Variable variable) {
		getVariables().remove(variable);
		variable.setValueDomainBean(null);

		return variable;
	}

}