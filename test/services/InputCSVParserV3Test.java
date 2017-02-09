package services;

import exceptions.DatapointMappingException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.DataPoint;
import uk.co.onsdigital.discovery.model.DimensionValue;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.HierarchyEntry;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.TypedQuery;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.fail;
import static services.InputCSVParser.END_OF_FILE;
import static utils.LambdaMatcher.argThatMatches;

public class InputCSVParserV3Test {

    public static final String OBSERVATION = "010.23";
    public static final BigDecimal OBSERVATION_VALUE = new BigDecimal(OBSERVATION);
    public static final String MARKING = "marking";
    public static final String OBSERVATION_TYPE = "123";
    public static final BigDecimal OBSERVATION_TYPE_VALUE = new BigDecimal(OBSERVATION_TYPE);

    @Mock
    private EntityManager entityManagerMock;
    @Mock
    private DimensionalDataSet datasetMock;
    @Mock
    private TypedQuery<HierarchyEntry> hierarchyQuery;
    @Mock
    private TypedQuery<DimensionValue> dimensionValueQuery;

    private InputCSVParserV3 testObj;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        testObj = new InputCSVParserV3();

        when(entityManagerMock.createNamedQuery(HierarchyEntry.FIND_QUERY, HierarchyEntry.class)).thenReturn(hierarchyQuery);
        when(entityManagerMock.createNamedQuery(DimensionValue.FIND_QUERY, DimensionValue.class)).thenReturn(dimensionValueQuery);

        when(hierarchyQuery.setParameter(anyString(), anyString())).thenReturn(hierarchyQuery);
        when(dimensionValueQuery.setParameter(anyString(), anyString())).thenReturn(dimensionValueQuery);
    }

    @Test
    public void shouldCreateDataPointAndAllDimensionValues() throws DatapointMappingException {
        // given a csv row with 2 dimensions
        CSVRow row = new CSVRow()
                .addDimension("", "dimension1", "value1")
                .addDimension(null, "dimension2", "value2");
        // that do not exist yet
        when(dimensionValueQuery.getResultList()).thenReturn(Collections.emptyList());

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock);

        // then each dimensionValue should be persisted
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension1", "value1"));
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension2", "value2"));
        // and a datapoint should be persisted
        verify(entityManagerMock).persist(argThatMatches(point ->
                point instanceof DataPoint
                && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                && OBSERVATION_TYPE_VALUE.equals(((DataPoint)point).getObservationTypeValue())
                && MARKING.equals(((DataPoint)point).getDataMarking())
                && ((DataPoint)point).getDimensionValues().size()==2
        ));
        // and nothing else should have been persisted
        verify(entityManagerMock, times(3)).persist(anyObject());
    }

    @Test
    public void shouldCreateDataPointWithNullMarkerAndType() throws DatapointMappingException {
        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, new String[] {OBSERVATION, null, null}, datasetMock);

        // then the datapoint should be persisted with null values for marker and type
        verify(entityManagerMock).persist(argThatMatches(point ->
                point instanceof DataPoint
                && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                && ((DataPoint)point).getObservationTypeValue() == null
                && ((DataPoint)point).getDataMarking() == null
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
        when(dimensionValueQuery.getResultList()).thenReturn(Collections.emptyList()).thenReturn(Collections.singletonList(existingValue));

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock);

        // then only one dimensionValue should be persisted
        verify(entityManagerMock).persist(dimensionValueWith(null, "dimension1", "value1"));
        verify(entityManagerMock, never()).persist(dimensionValueWith(null, "dimension2", "value2"));
        // and a datapoint should be persisted
        verify(entityManagerMock).persist(argThatMatches(point ->
                point instanceof DataPoint
                        && OBSERVATION_VALUE.equals(((DataPoint)point).getObservation())
                        && OBSERVATION_TYPE_VALUE.equals(((DataPoint)point).getObservationTypeValue())
                        && MARKING.equals(((DataPoint)point).getDataMarking())
                        && ((DataPoint)point).getDimensionValues().size()==2
                        && ((DataPoint)point).getDimensionValues().contains(existingValue)
        ));

    }

    @Test
    public void shouldLinkToHierarchyIfItExists() throws DatapointMappingException {
        // given a csv row with 2 dimensions, each with a hierarchy
        CSVRow row = new CSVRow()
                .addDimension("h1", "dimension1", "value")
                .addDimension("h2", "dimension2", "value");
        // with matching entries
        HierarchyEntry entry1 =  mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        HierarchyEntry entry2 =  mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        when(entry1.getHierarchy().getId()).thenReturn("h1");
        when(entry2.getHierarchy().getId()).thenReturn("h2");
        when(hierarchyQuery.getSingleResult()).thenReturn(entry1).thenReturn(entry2);

        // when parse is invoked
        testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock);

        // then each dimensionValue persisted should have had the correct hierarchy entry
        verify(entityManagerMock).persist(dimensionValueWith(entry1, "dimension1", "value"));
        verify(entityManagerMock).persist(dimensionValueWith(entry2, "dimension2", "value"));
    }

    @Test
    public void shouldThrowExceptionIfDimensionValueAlreadyExistsButDoesntHaveHierarchy() {
        // given a csv row with a dimension with a hierarchy
        CSVRow row = new CSVRow()
                .addDimension("h1", "dimension1", "value");
        // with matching hierarchy
        HierarchyEntry hierarchyEntry = mock(HierarchyEntry.class, RETURNS_DEEP_STUBS);
        when(hierarchyQuery.getSingleResult()).thenReturn(hierarchyEntry);
        when(hierarchyEntry.getHierarchy().getId()).thenReturn("hi");

        // and dimension value exists
        DimensionValue existingValue = new DimensionValue();
        when(dimensionValueQuery.getResultList()).thenReturn(Collections.emptyList()).thenReturn(Collections.singletonList(existingValue));
        // but does not reference the same hierarchy
        existingValue.setHierarchyEntry(new HierarchyEntry());

        // when parse is invoked
        try {
            testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock);
            fail("Should throw exception");
        } catch (DatapointMappingException e) {
            // expected behaviour
        }

    }

    @Test
    public void shouldThrowExceptionIfHierarchyEntryDoesNotExist() {
        // given a csv row with no matching hierarchy entry
        CSVRow row = new CSVRow().addDimension("doesNotExist", "dimensionName", "value");
        NoResultException noResultException = new NoResultException();
        when(hierarchyQuery.getSingleResult()).thenThrow(noResultException);

        // when parse is invoked
        try {
            testObj.parseRowdataDirectToTables(entityManagerMock, row.toArray(), datasetMock);
            fail("Should throw exception");
        } catch (DatapointMappingException e) {
            // then an exception should be thrown
            assertThat(e.getCause(), is(noResultException));
        }
    }

    @Test
    public void shouldDoNothingAtEndOfFile() throws DatapointMappingException {
        // when given the end of file marker
        testObj.parseRowdataDirectToTables(entityManagerMock, new String[] {END_OF_FILE, "foo"}, datasetMock);

        // then nothing should be persisted
        verifyZeroInteractions(entityManagerMock);
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
            this.add(OBSERVATION_TYPE);
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