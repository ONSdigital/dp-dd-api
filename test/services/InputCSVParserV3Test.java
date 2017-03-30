package services;

import exceptions.DatapointMappingException;
import models.DataPointRecord;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static services.InputCSVParserV3.END_OF_FILE;
import static utils.LambdaMatcher.argThatMatches;

public class InputCSVParserV3Test extends TestNGSuite {

    public static final String OBSERVATION = "010.23";
    public static final BigDecimal OBSERVATION_VALUE = new BigDecimal(OBSERVATION);
    public static final String MARKING = "marking";
    public static final String OBSERVATION_TYPE_VALUE = "123";

    @Mock
    private EntityManager entityManagerMock;
    @Mock
    private DataSet datasetMock;
    @Mock
    private TypedQuery<HierarchyEntry> hierarchyQueryMock;
    @Mock
    private TypedQuery<DimensionValue> dimensionValueQuery;
    @Mock
    private TypedQuery<Dimension> dimensionQuery;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private DimensionValue dimensionValueMock;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private HierarchyEntry hierarchyEntryMock;

    private InputCSVParserV3 testObj;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        testObj = new InputCSVParserV3();

        when(entityManagerMock.createNamedQuery(HierarchyEntry.FIND_QUERY, HierarchyEntry.class)).thenReturn(hierarchyQueryMock);
        when(entityManagerMock.createNamedQuery(DimensionValue.FIND_QUERY, DimensionValue.class)).thenReturn(dimensionValueQuery);
        when(entityManagerMock.createNamedQuery(Dimension.FIND_BY_DATA_SET_AND_NAME, Dimension.class)).thenReturn(dimensionQuery);

        when(hierarchyQueryMock.setParameter(anyString(), anyString())).thenReturn(hierarchyQueryMock);
        when(hierarchyQueryMock.setFlushMode(any(FlushModeType.class))).thenReturn(hierarchyQueryMock);
        when(dimensionValueQuery.setParameter(anyString(), anyString())).thenReturn(dimensionValueQuery);
        when(dimensionValueQuery.setFlushMode(any(FlushModeType.class))).thenReturn(dimensionValueQuery);
        when(dimensionQuery.setParameter(anyString(), any())).thenReturn(dimensionQuery);
        when(dimensionQuery.setFlushMode(any(FlushModeType.class))).thenReturn(dimensionQuery);
        when(dimensionValueQuery.getSingleResult()).thenReturn(dimensionValueMock);
        when(hierarchyQueryMock.getSingleResult()).thenReturn(hierarchyEntryMock);
        when(dimensionQuery.getSingleResult()).thenThrow(new NoResultException());
    }

    @Test
    public void shouldCreateDataPointAndAllDimensionValues() throws DatapointMappingException {
        // given a csv row with 2 dimensions
        CSVRow row = new CSVRow()
                .addDimension("", "dimension1", "value1")
                .addDimension(null, "dimension2", "value2");
        // that do not exist yet
        when(dimensionValueQuery.getSingleResult()).thenThrow(new NoResultException());

        UUID datasetID = UUID.randomUUID();
        UUID datapointID = UUID.randomUUID();

        // when parse is invoked
        long index = 1;
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock, createRecord(datasetID, datapointID, index));

        // then each dimensionValue should be persisted
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension1", "value1"));
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension2", "value2"));
        // and a datapoint should be persisted
        verify(entityManagerMock).merge(argThatMatches(point ->
                point instanceof DataPoint
                && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                && OBSERVATION_TYPE_VALUE.equals(((DataPoint)point).getObservationTypeValue())
                && MARKING.equals(((DataPoint)point).getDataMarking())
                && ((DataPoint)point).getDimensionValues().size()==2
                && ((DataPoint)point).getDatasetId().equals(datasetID)
                && ((DataPoint)point).getRowIndex().equals(index)
        ));
        // and the following should have been persisted: 2 dimensions and 2 dimension values
        verify(entityManagerMock, times(4)).persist(anyObject());
    }

    @Test
    public void shouldCreateDataPointWithNullMarkerAndType() throws DatapointMappingException {
        UUID datasetID = UUID.randomUUID();
        UUID datapointID = UUID.randomUUID();
        long index = 1;

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, new String[] {OBSERVATION, null, null}, datasetMock, createRecord(datasetID, datapointID, index));


        // then the datapoint should be persisted with null values for marker and type
        verify(entityManagerMock).merge(argThatMatches(point ->
                point instanceof DataPoint
                && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                && ((DataPoint)point).getObservationTypeValue() == null
                && ((DataPoint)point).getDataMarking() == null
                        && ((DataPoint)point).getDatasetId().equals(datasetID)
                        && ((DataPoint)point).getRowIndex().equals(index)
        ));
    }

    @Test
    public void shouldCreateDataPointWithTypeMarkingIfNonNumeric() throws DatapointMappingException {
        UUID datasetID = UUID.randomUUID();
        UUID datapointID = UUID.randomUUID();
        long index = 1;

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, new String[] {OBSERVATION, null, "x"}, datasetMock, createRecord(datasetID, datapointID, index));

        // then the datapoint should be persisted with null values for marker and type
        verify(entityManagerMock).merge(argThatMatches(point ->
                point instanceof DataPoint
                && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                && ((DataPoint)point).getDataMarking() == null
                        && ((DataPoint)point).getDatasetId().equals(datasetID)
                        && ((DataPoint)point).getRowIndex().equals(index)
                && "x".equals(((DataPoint)point).getObservationTypeValue())
        ));
    }

    @Test
    public void shouldNotCreateDimensionValueIfItAlreadyExists() throws DatapointMappingException {
        // given a csv row with 2 dimensions
        CSVRow row = new CSVRow()
                .addDimension("", "dimension1", "value1")
                .addDimension(null, "dimension2", "value2");
        // where one exists but the other does not
        DimensionValue existingValue = new DimensionValue();
        UUID datasetID = UUID.randomUUID();
        UUID datapointID = UUID.randomUUID();

        // when parse is invoked
        long index = 1;
        when(dimensionValueQuery.getSingleResult()).thenThrow(new NoResultException()).thenReturn(existingValue);

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock, createRecord(datasetID, datapointID, index));

        // then only one dimensionValue should be persisted
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension1", "value1"));
        verify(entityManagerMock, never()).persist(dimensionValueWith(null, "dimension2", "value2"));
        // and a datapoint should be persisted
        verify(entityManagerMock).merge(argThatMatches(point ->
                point instanceof DataPoint
                        && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                        && OBSERVATION_TYPE_VALUE.equals(((DataPoint)point).getObservationTypeValue())
                        && MARKING.equals(((DataPoint)point).getDataMarking())
                        && ((DataPoint)point).getDimensionValues().size()==2
                        && ((DataPoint)point).getDimensionValues().contains(existingValue)
                        && ((DataPoint)point).getDatasetId().equals(datasetID)
                        && ((DataPoint)point).getRowIndex().equals(index)
        ));

    }

    @Test
    public void shouldLinkToHierarchyIfItExists() throws DatapointMappingException {
        // given a csv row with 2 new dimension values, each with a hierarchy
        CSVRow row = new CSVRow()
                .addDimension("h1", "dimension1", "value")
                .addDimension("h2", "dimension2", "value");
        // with matching entries
        HierarchyEntry entry1 =  mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        HierarchyEntry entry2 =  mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        when(entry1.getHierarchy().getId()).thenReturn("h1");
        when(entry2.getHierarchy().getId()).thenReturn("h2");
        when(dimensionValueQuery.getSingleResult()).thenThrow(new NoResultException());
        when(hierarchyQueryMock.getSingleResult()).thenReturn(entry1).thenReturn(entry2);
        when(dimensionValueMock.getHierarchyEntry().getHierarchy().getId()).thenReturn("h1").thenReturn("h2");

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock, createRecord());

        // then each dimensionValue persisted should have had the correct hierarchy entry
        verify(entityManagerMock).persist(dimensionValueWith(entry1, "dimension1", "value"));
        verify(entityManagerMock).persist(dimensionValueWith(entry2, "dimension2", "value"));
    }

    @Test
    public void shouldNotThrowExceptionIfDimensionValueAlreadyExistsButDoesntHaveSameHierarchy() {
        // given a csv row with a dimension with a hierarchy
        CSVRow row = new CSVRow()
                .addDimension("h1", "dimension1", "value");
        // with matching hierarchy
        HierarchyEntry hierarchyEntry = mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        when(hierarchyQueryMock.getSingleResult()).thenReturn(hierarchyEntry);
        when(hierarchyEntry.getHierarchy().getId()).thenReturn("hi");

        // and dimension value exists
        DimensionValue existingValue = new DimensionValue();
        when(dimensionValueQuery.getSingleResult()).thenThrow(new NoResultException()).thenReturn(existingValue);
        // but does not reference the same hierarchy
        existingValue.setHierarchyEntry(new HierarchyEntry());

        // when parse is invoked
        try {
            testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock, createRecord());
        } catch (DatapointMappingException e) {
            fail("Should not throw exception");
        }

    }

    @Test
    public void shouldThrowExceptionIfHierarchyEntryDoesNotExist() {
        // given a csv row with no matching hierarchy entry
        CSVRow row = new CSVRow().addDimension("doesNotExist", "dimensionName", "value");
        NoResultException noResultException = new NoResultException();
        when(dimensionValueQuery.getSingleResult()).thenThrow(new NoResultException());
        when(hierarchyQueryMock.getSingleResult()).thenThrow(noResultException);

        // when parse is invoked
        try {
            testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock, createRecord());
            fail("Should throw exception");
        } catch (DatapointMappingException e) {
            // then an exception should be thrown
            assertThat(e.getCause(), is(noResultException));
        }
    }

    @Test
    public void shouldDoNothingAtEndOfFile() throws DatapointMappingException {
        // when given the end of file marker
        testObj.parseRowdataDirectToTables(entityManagerMock, new String[] {END_OF_FILE, "foo"}, datasetMock, createRecord());

        // then nothing should be persisted
        verifyZeroInteractions(entityManagerMock);
    }

    private DataPointRecord createRecord() {
        return createRecord(UUID.randomUUID(), UUID.randomUUID(), 1);
    }

    private DataPointRecord createRecord(UUID datasetID, UUID datapointID, long index) {
        return new DataPointRecord(index, "", "", System.currentTimeMillis(), datasetID, datapointID);
    }

    private Object dimensionValueWith(HierarchyEntry hierarchyEntry, String name, String value) {
        return argThatMatches(dimensionValue ->
                dimensionValue instanceof DimensionValue
                        && ((DimensionValue) dimensionValue).getHierarchyEntry() == hierarchyEntry
                        && name.equals(((DimensionValue) dimensionValue).getDimension().getName())
                        && value.equals(((DimensionValue) dimensionValue).getValue())
            );
    }

    private static class CSVRow extends ArrayList<String> {

        public CSVRow() {
            super();
            this.add(OBSERVATION);
            this.add(MARKING);
            this.add(OBSERVATION_TYPE_VALUE);
        }

        private CSVRow addDimension(String hierarchyId, String name, String value) {
            this.add(hierarchyId);
            this.add(name);
            this.add(value);
            return this;
        }

        public String[] toArray() {
            return super.toArray(new String[this.size()]);
        }
    }

}