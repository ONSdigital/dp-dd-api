package services;

import au.com.bytecode.opencsv.CSVParser;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import exceptions.DataPointParseException;
import exceptions.DatapointMappingException;
import models.DataPointRecord;
import play.Logger;
import uk.co.onsdigital.discovery.model.DataSet;
import uk.co.onsdigital.discovery.model.DataSetRowIndex;

import javax.persistence.*;
import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Service to receive messages from the {@link actors.KafkaActor}, process them and insert them into the database.
 */
public class DataPointMapper {

    private static final Logger.ALogger logger = Logger.of(DataPointMapper.class);

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final CSVParser csvParser = new CSVParser();
    private final EntityManagerFactory entityManagerFactory;
    private final Supplier<DatapointParser> datapointParserSupplier;

    public DataPointMapper(Supplier<DatapointParser> parserSupplier, EntityManagerFactory entityManagerFactory) {
        this.datapointParserSupplier = requireNonNull(parserSupplier);
        this.entityManagerFactory = requireNonNull(entityManagerFactory);
    }

    /**
     * Processes the given JSON records from the CSV Splitter and insert them into the database. All records in the
     * batch will be inserted in a single transaction. If an error occurs the entire batch will be aborted.
     *
     * @param jsonDataPoints the data points to process.
     */
    public void mapDataPoints(List<String> jsonDataPoints) throws DatapointMappingException {
        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        EntityTransaction tx = entityManager.getTransaction();
        tx.begin();
        try (DatapointParser parser = datapointParserSupplier.get()) {

            Set<UUID> datasetIds = new HashSet<>();
            for (String record : jsonDataPoints) {
                logger.debug("Processing data point: {}", record);

                final DataPointRecord dataPointRecord = parseDataPointRecord(record);
                datasetIds.add(dataPointRecord.getDatasetID());
                mapDataPoint(parser, dataPointRecord, entityManager);
            }

            logger.debug("Committing transaction.");
            tx.commit();
            logger.info("Finished processing {} data points for dataset(s) {}", jsonDataPoints.size(), datasetIds);
        } catch (Exception ex) {
            logger.error("Aborting transaction due to error: {}", ex, ex);
            tx.rollback();
            throw new DatapointMappingException(ex.getMessage());
        }
    }

    DataSet findOrCreateDataset(UUID datasetId, String s3URL, EntityManager entityManager) {
        DataSet dataSet = entityManager.find(DataSet.class, datasetId);
        if (dataSet == null) {
            dataSet = new DataSet(s3URL, null);
            dataSet.setTitle(s3URL.substring(s3URL.lastIndexOf("/") + 1));
            dataSet.setId(datasetId);
            dataSet.setStatus(DataSet.STATUS_NEW);
            entityManager.persist(dataSet);
        }
        return dataSet;
    }

    DataPointRecord parseDataPointRecord(String json) throws DataPointParseException, IOException {
        try {
            return jsonMapper.readValue(json, DataPointRecord.class);
        } catch (JsonMappingException | JsonParseException ex) {
            throw new DataPointParseException(ex);
        }
    }

    void mapDataPoint(final DatapointParser datapointParser, final DataPointRecord dataPointRecord, EntityManager entityManager) throws IOException, DatapointMappingException {
        try {
            final String[] rowDataArray = csvParser.parseLine(dataPointRecord.getRowData());
            logger.debug("rowDataArray: {}", (Object) rowDataArray);

            DataSet dataSet = findOrCreateDataset(dataPointRecord.getDatasetID(), dataPointRecord.getS3URL(), entityManager);

            datapointParser.parseRowdataDirectToTables(entityManager, rowDataArray, dataSet);
            createDatasetRowIndex(dataPointRecord.getDatasetID(), dataPointRecord.getIndex(),entityManager);
        } catch (RuntimeException e) {
            throw new DatapointMappingException("Invalid row: " + dataPointRecord, e);
        }
    }

    private void createDatasetRowIndex(UUID datasetId, long rowIndex, EntityManager entityManager) {
        DataSetRowIndex dataSetRowIndex = new DataSetRowIndex();
        dataSetRowIndex.setDatasetId(datasetId);
        dataSetRowIndex.setRowIndex(rowIndex);
        entityManager.persist(dataSetRowIndex);
    }

}
