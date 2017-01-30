package services;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import exceptions.DataPointParseException;
import exceptions.DatasetStatusException;
import models.DatasetStatus;
import play.Logger;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.DimensionalDataSetRowIndex;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Updates the status of a dataset once the number of rows processed equals the total number of rows.
 */
public class DatasetStatusUpdater {

    private static final Logger.ALogger logger = Logger.of(DatasetStatusUpdater.class);

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final EntityManagerFactory entityManagerFactory;

    public DatasetStatusUpdater(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    /**
     * Checks the status of the datasets identified in the given messages,
     * updating the status and totalRows properties of the affected datasets.
     * @param jsonDataPoints List of DatasetStatus objects in json format.
     * @return a List of incomplete datasets as DatasetStatus objects
     * @throws DatasetStatusException
     */
    public List<DatasetStatus> checkStatus(List<String> jsonDataPoints) throws DatasetStatusException {
        final List<DatasetStatus> incompleteDatasets = new ArrayList<>();
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction tx = entityManager.getTransaction();
        tx.begin();
        try {

            for (String record : jsonDataPoints) {
                logger.info("Processing dataset: {}", record);

                DatasetStatus status = parseDataPointRecord(record);
                status = processDatasetStatus(status, entityManager);
                if (status.isComplete()) {
                    logger.info("Dataset {} is complete with all {} rows processed", status.getDatasetID(), status.getTotalRows());
                } else {
                    incompleteDatasets.add(status);
                }
            }

            logger.debug("Committing transaction.");
            tx.commit();
            logger.info("Finished processing {} data points", jsonDataPoints.size());
        } catch (Exception ex) {
            logger.error("Aborting transaction due to error: {}", ex, ex);
            tx.rollback();
            throw new DatasetStatusException(ex.getMessage());
        }
        return incompleteDatasets;
    }

    private DatasetStatus processDatasetStatus(DatasetStatus status, EntityManager entityManager) {
        DimensionalDataSet dimensionalDataSet = entityManager.find(DimensionalDataSet.class, status.getDatasetID());
        if (dimensionalDataSet == null) {
            logger.error("Unable to find dataset {}", status.getDatasetID());
            return status;
        }
        Query query = entityManager.createNamedQuery(DimensionalDataSetRowIndex.COUNT_QUERY);
        query.setParameter(DimensionalDataSetRowIndex.DATASET_PARAMETER, status.getDatasetID());
        long count = ((Number) query.getSingleResult()).longValue();
        if (count == status.getTotalRows()) {
            dimensionalDataSet.setStatus("complete");
            query =  entityManager.createNamedQuery(DimensionalDataSetRowIndex.DELETE_QUERY);
            query.setParameter(DimensionalDataSetRowIndex.DATASET_PARAMETER, status.getDatasetID());
            query.executeUpdate();
        }
        if (dimensionalDataSet.getTotalRowCount() == null) {
            dimensionalDataSet.setTotalRowCount(status.getTotalRows());
        }
        return createUpdatedStatus(status, count);
    }

    private DatasetStatus createUpdatedStatus(DatasetStatus status, long rowsProcessed) {
        long lastUpdate  = status.getLastUpdateTime();
        if (rowsProcessed > status.getRowsProcessed()) {
            lastUpdate = System.currentTimeMillis();
        }
        return new DatasetStatus(lastUpdate, status.getTotalRows(), rowsProcessed, status.getDatasetID());
    }


    DatasetStatus parseDataPointRecord(String json) throws DataPointParseException, IOException {
        try {
            return jsonMapper.readValue(json, DatasetStatus.class);
        } catch (JsonMappingException | JsonParseException ex) {
            throw new DataPointParseException(ex);
        }
    }

}
