package models;

import java.io.Serializable;
import javax.persistence.*;
import java.util.List;


/**
 * The persistent class for the time_type database table.
 * 
 */
@Entity
@Table(name="time_type")
@NamedQuery(name="TimeType.findAll", query="SELECT t FROM TimeType t")
public class TimeType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name="time_type")
	private String timeType;

	//bi-directional many-to-one association to TimePeriod
	@OneToMany(mappedBy="timeTypeBean")
	private List<TimePeriod> timePeriods;

	public TimeType() {
	}

	public String getTimeType() {
		return this.timeType;
	}

	public void setTimeType(String timeType) {
		this.timeType = timeType;
	}

	public List<TimePeriod> getTimePeriods() {
		return this.timePeriods;
	}

	public void setTimePeriods(List<TimePeriod> timePeriods) {
		this.timePeriods = timePeriods;
	}

	public TimePeriod addTimePeriod(TimePeriod timePeriod) {
		getTimePeriods().add(timePeriod);
		timePeriod.setTimeTypeBean(this);

		return timePeriod;
	}

	public TimePeriod removeTimePeriod(TimePeriod timePeriod) {
		getTimePeriods().remove(timePeriod);
		timePeriod.setTimeTypeBean(null);

		return timePeriod;
	}

}