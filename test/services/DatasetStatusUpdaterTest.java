package services;

import exceptions.DatasetStatusException;
import models.DatasetStatus;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.DimensionalDataSetRowIndex;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DatasetStatusUpdaterTest {

    public static final UUID DATASET_ID = UUID.randomUUID();

    @Mock
    private EntityManagerFactory factoryMock;
    @Mock
    private EntityManager managerMock;
    @Mock
    private DimensionalDataSet dataSetMock;
    @Mock
    private Query countQueryMock;
    @Mock
    private Query deleteQueryMock;
    @Mock
    private EntityTransaction transactionMock;

    private DatasetStatusUpdater testObj;

    private DatasetStatus status;


    @BeforeMethod
    public void setup() throws Exception {
        initMocks(this);
        testObj = new DatasetStatusUpdater(factoryMock);
        status = new DatasetStatus(System.currentTimeMillis(), 100, 0, DATASET_ID);

        when(factoryMock.createEntityManager()).thenReturn(managerMock);
        when(managerMock.createNamedQuery(DimensionalDataSetRowIndex.COUNT_QUERY)).thenReturn(countQueryMock);
        when(managerMock.createNamedQuery(DimensionalDataSetRowIndex.DELETE_QUERY)).thenReturn(deleteQueryMock);
        when(managerMock.getTransaction()).thenReturn(transactionMock);
    }

    @Test
    public void shouldReturnStatusUnchangedIfDatasetNotFound() throws DatasetStatusException {
        // given a status representing a dataset that does not exist
        when(managerMock.find(DimensionalDataSet.class, DATASET_ID)).thenReturn(null);

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
        when(managerMock.find(DimensionalDataSet.class, DATASET_ID)).thenReturn(dataSetMock);
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
        when(managerMock.find(DimensionalDataSet.class, DATASET_ID)).thenReturn(dataSetMock);
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
        assertThat(result.get(0).getLastUpdateTime(), is(greaterThanOrEqualTo(lastUpdated)));
        assertThat(result.get(0).getDatasetID(), is(DATASET_ID));

        // and the DataSet was updated
        InOrder inOrder = inOrder(transactionMock, dataSetMock);
        inOrder.verify(transactionMock).begin();
        inOrder.verify(dataSetMock).setTotalRowCount(status.getTotalRows());
        inOrder.verify(transactionMock).commit();

        verifyZeroInteractions(deleteQueryMock);
    }

    @Test
    public void shouldSetStatusToCompleteAndDeleteRowIndexes() throws DatasetStatusException {
        // given a matching dataset
        when(managerMock.find(DimensionalDataSet.class, DATASET_ID)).thenReturn(dataSetMock);
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
        assertThat(result.get(0).getLastUpdateTime(), is(greaterThanOrEqualTo(lastUpdated)));
        assertThat(result.get(0).getDatasetID(), is(DATASET_ID));

        // and the DataSet was updated
        InOrder inOrder = inOrder(transactionMock, dataSetMock, deleteQueryMock);
        inOrder.verify(transactionMock).begin();
        inOrder.verify(dataSetMock).setStatus(DimensionalDataSet.STATUS_COMPLETE);
        // and row indexes are deleted
        inOrder.verify(deleteQueryMock).executeUpdate();
        inOrder.verify(transactionMock).commit();

    }

    @Test
    public void shouldRollBackTransactionOnException() throws DatasetStatusException {
        // given an exception will occur when retrieving the dataset
        when(managerMock.find(DimensionalDataSet.class, DATASET_ID)).thenThrow(new RuntimeException("oops"));

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
}