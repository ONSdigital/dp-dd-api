package services;

import au.com.bytecode.opencsv.CSVParser;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import exceptions.DataPointParseException;
import exceptions.DatapointMappingException;
import models.DataPointRecord;
import org.hibernate.Session;
import play.Logger;
import uk.co.onsdigital.discovery.model.*;

import javax.persistence.*;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Service to receive messages from the {@link actors.KafkaActor}, process them and insert them into the database.
 *
 * This version requires the following tables:
 *
 * <pre>
 CREATE TABLE data_set_row (
 data_set_id uuid NOT NULL,
 row_index INTEGER NOT NULL,
 data_marking varchar(255),
 observation numeric,
 observation_type_value varchar(255),
 PRIMARY KEY (data_set_id, row_index)
 );

 CREATE TABLE dimension_value_row_index (
 dimension_value_id uuid not null,
 row_index INTEGER NOT NULL,
 PRIMARY KEY (dimension_value_id, row_index)
 );
 * </pre>
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
        Map<String, PreparedStatement> statements = new LinkedHashMap<>();
        try (DatapointParser parser = datapointParserSupplier.get()) {

            logger.debug("Creating prepared statements");
            Session session = entityManager.unwrap(Session.class);
            session.doWork(connection -> {
                statements.put("dimension", connection.prepareStatement("INSERT INTO dimension (id, data_set_id, name, type) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING"));
                statements.put("dimension_value", connection.prepareStatement("INSERT INTO dimension_value (id, dimension_id, value, hierarchy_entry_id) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING"));
                statements.put("datapoint", connection.prepareStatement("INSERT INTO data_set_row (data_set_id, row_index, observation, observation_type_value, data_marking) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"));
                statements.put("dimension_value_datapoint", connection.prepareStatement("INSERT INTO dimension_value_row_index (dimension_value_id, row_index) VALUES (?, ?) ON CONFLICT DO NOTHING"));
            });

            Map<UUID, Integer> datasetCounts = new HashMap<>();
            for (String record : jsonDataPoints) {
                logger.debug("Processing data point: {}", record);

                final DataPointRecord dataPointRecord = parseDataPointRecord(record);
                mapDataPoint(parser, dataPointRecord, entityManager, statements);
                UUID datasetID = dataPointRecord.getDatasetID();
                datasetCounts.put(datasetID, datasetCounts.getOrDefault(datasetID, 0) + 1);
            }

            logger.debug("Updating dataset counts: {}", datasetCounts);
            for (Map.Entry<UUID, Integer> entry : datasetCounts.entrySet()) {
                Query query = entityManager.createNamedQuery(DataSet.UPDATE_PROCESSED_COUNT_QUERY);
                query.setParameter(DataSet.ID_PARAM, entry.getKey());
                query.setParameter(DataSet.COUNT_PARAM, entry.getValue());
                query.executeUpdate();
            }
            logger.debug("Closing prepared statements");
            for (PreparedStatement statement : statements.values()) {
                statement.executeBatch();
                statement.close();
            }
            logger.debug("Committing transaction.");
            tx.commit();
            logger.info("Finished processing {} data points for dataset(s) {}", jsonDataPoints.size(), datasetCounts.keySet());
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
            Session session = entityManager.unwrap(Session.class);
            session.doWork(connection -> {
                PreparedStatement statement = connection.prepareStatement("INSERT INTO data_set (id, title, s3_url, status, major_version, minor_version) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING");
                int idx = 1;
                statement.setObject(idx++, datasetId);
                statement.setString(idx++, s3URL.substring(s3URL.lastIndexOf("/") + 1));
                statement.setString(idx++, s3URL);
                statement.setString(idx++, DataSet.STATUS_NEW);
                statement.setInt(idx++, 0);
                statement.setInt(idx++, 0);
                statement.executeUpdate();
                statement.close();
            });
            Query rowCountQuery = entityManager.createNamedQuery(DataSet.INSERT_PROCESSED_COUNT_QUERY);
            rowCountQuery.setParameter(DataSet.ID_PARAM, datasetId);
            rowCountQuery.setParameter(DataSet.COUNT_PARAM, 0);
            rowCountQuery.executeUpdate();
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

    void mapDataPoint(final DatapointParser datapointParser, final DataPointRecord dataPointRecord, EntityManager entityManager, Map<String, PreparedStatement> statements) throws IOException, DatapointMappingException {
        try {
            final String[] rowDataArray = csvParser.parseLine(dataPointRecord.getRowData());
            logger.debug("rowDataArray: {}", (Object) rowDataArray);

            DataSet dataSet = findOrCreateDataset(dataPointRecord.getDatasetID(), dataPointRecord.getS3URL(), entityManager);

            datapointParser.parseRowdataDirectToTables(entityManager, rowDataArray, dataSet, dataPointRecord.getDatapointID(), statements, dataPointRecord.getIndex());
        } catch (RuntimeException e) {
            throw new DatapointMappingException("Invalid row: " + dataPointRecord, e);
        }
    }

}
