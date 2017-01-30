package models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
