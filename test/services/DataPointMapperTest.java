package services;

import exceptions.DataPointParseException;
import exceptions.DatapointMappingException;
import models.DataPointRecord;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class DataPointMapperTest extends TestNGSuite {

    @Mock
    private EntityManager mockEntityManager;

    @Mock
    private EntityManagerFactory mockEntityManagerFactory;

    @Mock
    private DatapointParser mockDatapointParser;
    @Mock
    private Query mockQuery;
    @Mock
    private Query mockUpdateQuery;

    @Mock
    private EntityTransaction mockTransaction;

    private DataPointMapper dataPointMapper;

    @BeforeMethod
    public void createRecordProcessor() {
        MockitoAnnotations.initMocks(this);
        when(mockEntityManagerFactory.createEntityManager()).thenReturn(mockEntityManager);
        when(mockEntityManager.createNamedQuery(DataSet.INSERT_PROCESSED_COUNT_QUERY)).thenReturn(mockQuery);
        when(mockEntityManager.createNamedQuery(DataSet.UPDATE_PROCESSED_COUNT_QUERY)).thenReturn(mockUpdateQuery);
        dataPointMapper = new DataPointMapper(() -> mockDatapointParser, mockEntityManagerFactory);
    }

    @Test
    public void shouldParseJsonRecordsCorrectly() throws Exception {
        final JSONObject json = getTestJsonRecord();

        final DataPointRecord record = dataPointMapper.parseDataPointRecord(json.toString());

        assertThat(record).as("Parsed Datapoint record")
                .isNotNull()
                .hasFieldOrPropertyWithValue("index", json.getLong("index"))
                .hasFieldOrPropertyWithValue("s3URL", json.getString("s3URL"))
                .hasFieldOrPropertyWithValue("startTime", json.getLong("startTime"))
                .hasFieldOrPropertyWithValue("rowData", json.getString("row"))
                .hasFieldOrPropertyWithValue("datasetID", UUID.fromString(json.getString("datasetID")));
    }

    @Test(expectedExceptions = DataPointParseException.class)
    public void shouldRejectRecordsWithNoDatasetId() throws Exception {
        final JSONObject json = getTestJsonRecord();
        json.remove("datasetID");

        dataPointMapper.parseDataPointRecord(json.toString());
    }

    @Test(expectedExceptions = DataPointParseException.class)
    public void shouldRejectRecordsWithNoS3URL() throws Exception {
        final JSONObject json = getTestJsonRecord();
        json.remove("s3URL");

        dataPointMapper.parseDataPointRecord(json.toString());
    }

    @Test(expectedExceptions = DataPointParseException.class)
    public void shouldRejectRecordsWithNoRowData() throws Exception {
        final JSONObject json = getTestJsonRecord();
        json.remove("row");

        dataPointMapper.parseDataPointRecord(json.toString());
    }

    @Test
    public void shouldRejectRecordsWithNoIndex() throws Exception {
        final JSONObject json = getTestJsonRecord();
        json.remove("index");

        dataPointMapper.parseDataPointRecord(json.toString());
    }

    @Test
    public void shouldRejectRecordsWithNoStartTime() throws Exception {
        final JSONObject json = getTestJsonRecord();
        json.remove("startTime");

        dataPointMapper.parseDataPointRecord(json.toString());
    }

    @Test
    public void shouldReturnExistingDatasetWhenItExists() throws Exception {
        UUID datasetId = UUID.randomUUID();
        DataSet dataSet = new DataSet();
        when(mockEntityManager.find(DataSet.class, datasetId)).thenReturn(dataSet);

        DataSet result = dataPointMapper.findOrCreateDataset(datasetId, "", mockEntityManager);

        assertThat(result).isSameAs(dataSet);
        verify(mockEntityManager).find(DataSet.class, datasetId);
        verifyNoMoreInteractions(mockEntityManager);
    }

    @Test
    public void shouldCreateDatasetIfDoesNotExist() throws Exception {
        UUID datasetId = UUID.randomUUID();
        String s3URL = "s3://bucket/dir/file.csv";
        when(mockEntityManager.find(DataSet.class, datasetId)).thenReturn(null);

        DataSet result = dataPointMapper.findOrCreateDataset(datasetId, s3URL, mockEntityManager);

        verify(mockEntityManager).persist(result);
        assertThat(result).isNotNull()
                .hasFieldOrPropertyWithValue("id", datasetId)
                .hasFieldOrPropertyWithValue("s3URL", s3URL);
        verify(mockQuery).setParameter(DataSet.ID_PARAM, datasetId);
        verify(mockQuery).setParameter(DataSet.COUNT_PARAM, 0);
        verify(mockQuery).executeUpdate();
    }

    @Test
    public void shouldCallInputParserWithDataFromRecord() throws Exception {
        UUID actualDatapointId = UUID.randomUUID();
        DataPointRecord record = new DataPointRecord(42, "a,b,c", "test.csv", 1000, UUID.randomUUID(), actualDatapointId);
        DataSet dataSet = new DataSet();
        when(mockEntityManager.find(DataSet.class, record.getDatasetID())).thenReturn(dataSet);

        dataPointMapper.mapDataPoint(mockDatapointParser, record, mockEntityManager);

        verify(mockDatapointParser).parseRowdataDirectToTables(mockEntityManager, new String[]{"a", "b", "c"}, dataSet, actualDatapointId);
    }

    @Test(expectedExceptions = IOException.class)
    public void shouldFailIfRecordContainsInvalidCSVData() throws Exception {
        DataPointRecord record = new DataPointRecord(42, "a,b\",c", "test.csv", 1000, UUID.randomUUID(), UUID.randomUUID());
        DataSet dataSet = new DataSet();
        when(mockEntityManager.find(DataSet.class, record.getDatasetID())).thenReturn(dataSet);

        dataPointMapper.mapDataPoint(mockDatapointParser, record, mockEntityManager);
    }

    @Test
    public void shouldUpdateRowCount() throws Exception {
        JSONObject jsonRecord = getTestJsonRecord();
        List<String> records = Arrays.asList(jsonRecord.toString(), jsonRecord.toString());

        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);

        dataPointMapper.mapDataPoints(records);

        verify(mockEntityManager).getTransaction();
        verify(mockTransaction).begin();
        verify(mockUpdateQuery).setParameter(DataSet.ID_PARAM, UUID.fromString(jsonRecord.getString("datasetID")));
        verify(mockUpdateQuery).setParameter(DataSet.COUNT_PARAM, records.size());
        verify(mockUpdateQuery).executeUpdate();
        verify(mockTransaction).commit();
    }

    @Test(expectedExceptions = DatapointMappingException.class)
    public void shouldRollbackTransactionOnError() throws Exception {
        List<String> records = Collections.singletonList("gibberish");

        when(mockEntityManager.getTransaction()).thenReturn(mockTransaction);

        try {
            dataPointMapper.mapDataPoints(records);
        } finally {

            verify(mockEntityManager).getTransaction();
            verify(mockTransaction).begin();
            verify(mockTransaction).rollback();
        }
    }

    private JSONObject getTestJsonRecord() throws Exception {
        return new JSONObject(readResource("testRecord.json"));
    }

    private String readResource(String resourceName) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/" + resourceName)) {
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        }
    }
}