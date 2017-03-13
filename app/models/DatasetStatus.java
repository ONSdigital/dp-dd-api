package models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.UUID;

/**
 * Represents the status of a dataset - the total number of rows and the number processed so far.
 */
public class DatasetStatus {

    private final long lastUpdateTime;
    private final long totalRows;
    private final long rowsProcessed;
    private final UUID datasetID;

    @JsonCreator
    public DatasetStatus(
            @JsonProperty("lastUpdate") long lastUpdateTime,
            @JsonProperty("totalRows") long totalRows,
            @JsonProperty("rowsProcessed") long rowsProcessed,
            @JsonProperty("datasetID") UUID datasetID) {
        this.lastUpdateTime = lastUpdateTime;
        this.totalRows = totalRows;
        this.rowsProcessed = rowsProcessed;
        this.datasetID = datasetID;
    }

    @JsonProperty("lastUpdate")
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getRowsProcessed() {
        return rowsProcessed;
    }

    public UUID getDatasetID() {
        return datasetID;
    }

    @JsonIgnore
    public boolean isComplete() {
        return totalRows == rowsProcessed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatasetStatus that = (DatasetStatus) o;

        if (lastUpdateTime != that.lastUpdateTime) return false;
        if (totalRows != that.totalRows) return false;
        if (rowsProcessed != that.rowsProcessed) return false;
        return datasetID != null ? datasetID.equals(that.datasetID) : that.datasetID == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        result = 31 * result + (int) (totalRows ^ (totalRows >>> 32));
        result = 31 * result + (int) (rowsProcessed ^ (rowsProcessed >>> 32));
        result = 31 * result + (datasetID != null ? datasetID.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("lastUpdateTime", lastUpdateTime)
                .append("totalRows", totalRows)
                .append("rowsProcessed", rowsProcessed)
                .append("datasetID", datasetID)
                .toString();
    }
}
