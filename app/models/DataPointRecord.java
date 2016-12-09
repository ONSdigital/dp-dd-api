package models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Structure of records received from the CSV splitter via Kafka. Each record contains a single
 * line of the original CSV, plus some metadata about the file it came from and dataset it is for, etc.
 * <p>
 * Example:
 * <pre>{@code
 * {
 * "index": 10619,
 * "row": "0,,Person,,Count,,,,,,,,,,W06000022,,,,,,,,,,,,,,,,,,,,,Sex,Sex,,Females,Females,,,,Age,Age,,Age 50 and over,Age 50 and over,,,,Residence Type,Residence Type,,Lives in a household,Lives in a household,,,",
 * "filename": "AF001EW.csv",
 * "startTime": 1481214210,
 * "datasetID": "ac31776f-17a8-4e68-a673-e19589b23496"
 * }
 * }</pre>
 */
public final class DataPointRecord {
    private final long index;
    private final String rowData;
    private final String filename;
    private final long startTime;
    private final UUID datasetID;

    @JsonCreator
    public DataPointRecord(
            @JsonProperty("index") long index,
            @JsonProperty("row") String rowData,
            @JsonProperty("filename") String filename,
            @JsonProperty("startTime") long startTime,
            @JsonProperty("datasetID") UUID datasetID) {
        this.index = index;
        this.rowData = requireNonNull(rowData);
        this.filename = requireNonNull(filename);
        this.startTime = startTime;
        this.datasetID = requireNonNull(datasetID);
    }

    public long getIndex() {
        return index;
    }

    public String getRowData() {
        return rowData;
    }

    public String getFilename() {
        return filename;
    }

    public long getStartTime() {
        return startTime;
    }

    public UUID getDatasetID() {
        return datasetID;
    }

    @Override
    public boolean equals(Object other) {
        // Ignore startTime in equality/hashCode as that it just for performance monitoring
        if (this == other) { return true; }
        if (!(other instanceof DataPointRecord)) { return false; }
        DataPointRecord that = (DataPointRecord) other;
        return this.index == that.index && Objects.equals(this.rowData, that.rowData)
                && Objects.equals(this.filename, that.filename) && Objects.equals(this.datasetID, that.datasetID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, rowData, filename, datasetID);
    }

    @Override
    public String toString() {
        return "DataPointRecord{" +
                "index=" + index +
                ", rowData='" + rowData + '\'' +
                ", filename='" + filename + '\'' +
                ", startTime=" + startTime +
                ", datasetID=" + datasetID +
                '}';
    }
}
