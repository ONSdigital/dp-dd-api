package services;

import exceptions.DatasetStatusException;
import models.DatasetStatus;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.scalatest.testng.TestNGSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.DataSet;

import javax.persistence.*;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class DatasetStatusUpdaterTest extends TestNGSuite {

    public static final UUID DATASET_ID = UUID.randomUUID();

    @Mock
    private EntityManagerFactory factoryMock;
    @Mock
    private EntityManager managerMock;
    @Mock
    private DataSet dataSetMock;
    @Mock
    private Query countQueryMock;
    @Mock
    private EntityTransaction transactionMock;

    @Mock
    private Supplier<Long> timeNowMock;

    private DatasetStatusUpdater testObj;

    private DatasetStatus status;
    private long timestamp = System.currentTimeMillis();


    @BeforeMethod
    public void setup() throws Exception {
        initMocks(this);
        testObj = new DatasetStatusUpdater(factoryMock);
        status = new DatasetStatus(timestamp, 100, 0, DATASET_ID);

        when(factoryMock.createEntityManager()).thenReturn(managerMock);
        when(managerMock.createNamedQuery(DataSet.GET_PROCESSED_COUNT_QUERY)).thenReturn(countQueryMock);
        when(managerMock.getTransaction()).thenReturn(transactionMock);
        when(timeNowMock.get()).thenReturn(timestamp);

        testObj.setTimeNow(timeNowMock);
    }

    @Test
    public void shouldReturnStatusUnchangedIfDatasetNotFound() throws DatasetStatusException {
        // given a status representing a dataset that does not exist
        when(managerMock.find(DataSet.class, DATASET_ID)).thenReturn(null);

        // when updateStatuses is called
        List<DatasetStatus> result = testObj.updateStatuses(singletonList(status));

        // then status is returned unchanged
        verify(transactionMock).begin();
        verify(transactionMock).commit();
        assertThat(result, is(singletonList(status)));
    }

    @Test
    public void shouldReturnStatusUnchangedIfProcessedCountNotFound() throws DatasetStatusException {
        // given a matching dataset
        when(managerMock.find(DataSet.class, DATASET_ID)).thenReturn(dataSetMock);
        // with a value for totalRows
        when(dataSetMock.getTotalRowCount()).thenReturn(status.getTotalRows());
        // for which no more rows have been processed
        when(countQueryMock.getSingleResult()).thenReturn(null);

        // when updateStatuses is called
        List<DatasetStatus> result = testObj.updateStatuses(singletonList(status));

        // then status is returned unchanged
        verify(transactionMock).begin();
        verify(transactionMock).commit();
        assertThat(result, is(singletonList(status)));
    }

    @Test
    public void shouldReturnStatusUnchangedIfNoRowsImportedSinceLastUpdate() throws DatasetStatusException {
        // given a matching dataset
        when(managerMock.find(DataSet.class, DATASET_ID)).thenReturn(dataSetMock);
        // with a value for totalRows
        when(dataSetMock.getTotalRowCount()).thenReturn(status.getTotalRows());
        // for which no more rows have been processed
        when(countQueryMock.getSingleResult()).thenReturn(status.getRowsProcessed());

        // when updateStatuses is called
        List<DatasetStatus> result = testObj.updateStatuses(singletonList(status));

        // then status is returned unchanged
        verify(transactionMock).begin();
        verify(transactionMock).commit();
        assertThat(result, is(singletonList(status)));
    }

    @Test
    public void shouldUpdateTotalRows() throws DatasetStatusException {
        // given a matching dataset
        when(managerMock.find(DataSet.class, DATASET_ID)).thenReturn(dataSetMock);
        // with null totalRows
        when(dataSetMock.getTotalRowCount()).thenReturn(null);
        // for which 2 rows have been processed
        when(countQueryMock.getSingleResult()).thenReturn(2);

        // when updateStatuses is called
        long lastUpdated = System.currentTimeMillis();
        List<DatasetStatus> result = testObj.updateStatuses(singletonList(status));

        // then an updated status is returned
        assertThat(result.size(), is(1));
        assertThat(result.get(0).getRowsProcessed(), is(2L));
        assertThat(result.get(0).getTotalRows(), is(status.getTotalRows()));
        assertThat(result.get(0).getLastUpdateTime(), equalTo(timestamp));
        assertThat(result.get(0).getDatasetID(), is(DATASET_ID));

        // and the DataSet was updated
        InOrder inOrder = inOrder(transactionMock, dataSetMock);
        inOrder.verify(transactionMock).begin();
        inOrder.verify(dataSetMock).setTotalRowCount(status.getTotalRows());
        inOrder.verify(transactionMock).commit();
    }

    @Test
    public void shouldSetStatusToCompleteAndDeleteRowIndexes() throws DatasetStatusException {
        // given a matching dataset
        when(managerMock.find(DataSet.class, DATASET_ID)).thenReturn(dataSetMock);
        // with a value for totalRows
        when(dataSetMock.getTotalRowCount()).thenReturn(status.getTotalRows());
        // for which all rows have been processed
        when(countQueryMock.getSingleResult()).thenReturn(status.getTotalRows());

        // when updateStatuses is called
        long lastUpdated = System.currentTimeMillis();
        List<DatasetStatus> result = testObj.updateStatuses(singletonList(status));

        // then an updated status is returned
        assertThat(result.size(), is(1));
        assertThat(result.get(0).getRowsProcessed(), is(status.getTotalRows()));
        assertThat(result.get(0).getTotalRows(), is(status.getTotalRows()));
        assertThat(result.get(0).getLastUpdateTime(), equalTo(timestamp));
        assertThat(result.get(0).getDatasetID(), is(DATASET_ID));

        // and the DataSet was updated
        InOrder inOrder = inOrder(transactionMock, dataSetMock);
        inOrder.verify(transactionMock).begin();
        inOrder.verify(dataSetMock).setStatus(DataSet.STATUS_COMPLETE);
        inOrder.verify(transactionMock).commit();

    }

    @Test
    public void shouldRollBackTransactionOnException() throws DatasetStatusException {
        // given an exception will occur when retrieving the dataset
        when(managerMock.find(DataSet.class, DATASET_ID)).thenThrow(new RuntimeException("oops"));

        // when updateStatuses is called
        try {
            testObj.updateStatuses(singletonList(status));
            fail("Should re-throw exeption");
        } catch (DatasetStatusException e) {
            // expected behaviour
        }

        InOrder inOrder = inOrder(transactionMock);
        inOrder.verify(transactionMock).begin();
        inOrder.verify(transactionMock).rollback();
    }

    @Test
    public void shouldReturnDatasetCompleteIfStatusIsComplete() throws DatasetStatusException {
        when(managerMock.find(DataSet.class, status.getDatasetID()))
                .thenReturn(dataSetMock);

        when(dataSetMock.getStatus())
                .thenReturn(DataSet.STATUS_COMPLETE);

        when(managerMock.createNamedQuery(DataSet.GET_PROCESSED_COUNT_QUERY))
                .thenReturn(countQueryMock);

        when(countQueryMock.getSingleResult())
                .thenReturn(status.getRowsProcessed());

        DatasetStatus expected = new DatasetStatus(status.getLastUpdateTime(), status.getTotalRows(),
                status.getTotalRows(), status.getDatasetID());

        List<DatasetStatus> expectedResult = new ArrayList<>();
        expectedResult.add(expected);

        List<DatasetStatus> statuses = new ArrayList<>();
        statuses.add(status);

        List<DatasetStatus> actual = testObj.updateStatuses(statuses);
        assertEquals(actual, expectedResult);

        verify(managerMock, never()).createNamedQuery(DataSet.GET_PROCESSED_COUNT_QUERY);
        verify(transactionMock, times(1)).commit();
        verifyZeroInteractions(countQueryMock);
    }
}