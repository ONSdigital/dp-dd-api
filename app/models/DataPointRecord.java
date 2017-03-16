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
 * "s3URL": "s3://some-bucket/dir1/AF001EW.csv",
 * "startTime": 1481214210,
 * "datasetID": "ac31776f-17a8-4e68-a673-e19589b23496"
 * }
 * }</pre>
 */
public final class DataPointRecord {
    private final long index;
    private final String rowData;
    private final String s3URL;
    private final long startTime;
    private final UUID datasetID;
    private final UUID datapointID;

    @JsonCreator
    public DataPointRecord(
            @JsonProperty("index") long index,
            @JsonProperty("row") String rowData,
            @JsonProperty("s3URL") String s3URL,
            @JsonProperty("startTime") long startTime,
            @JsonProperty("datasetID") UUID datasetID,
            @JsonProperty("rowID") UUID datapointID) {
        this.index = index;
        this.rowData = requireNonNull(rowData);
        this.s3URL = requireNonNull(s3URL);
        this.startTime = startTime;
        this.datasetID = requireNonNull(datasetID);
        this.datapointID = requireNonNull(datapointID);
    }

    public long getIndex() {
        return index;
    }

    public String getRowData() {
        return rowData;
    }

    public String getS3URL() {
        return s3URL;
    }

    public long getStartTime() {
        return startTime;
    }

    public UUID getDatasetID() {
        return datasetID;
    }

    public UUID getDatapointID() {
        return datapointID;
    }

    @Override
    public boolean equals(Object other) {
        // Ignore startTime in equality/hashCode as that it just for performance monitoring
        if (this == other) { return true; }
        if (!(other instanceof DataPointRecord)) { return false; }
        DataPointRecord that = (DataPointRecord) other;
        return this.index == that.index && Objects.equals(this.rowData, that.rowData)
                && Objects.equals(this.s3URL, that.s3URL) && Objects.equals(this.datasetID, that.datasetID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, rowData, s3URL, datasetID);
    }

    @Override
    public String toString() {
        return "DataPointRecord{" +
                "index=" + index +
                ", rowData='" + rowData + '\'' +
                ", s3URL='" + s3URL + '\'' +
                ", startTime=" + startTime +
                ", datasetID=" + datasetID +
                ", datapointID=" + datapointID +
                '}';
    }
}
