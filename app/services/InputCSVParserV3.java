package services;

import exceptions.DatapointMappingException;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;
import play.Logger;
import uk.co.onsdigital.discovery.model.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * Capable of importing rows in the v3 format into the db. The format is as follows:
 * <p>
 * Observation,Data_Marking,Observation_Type_Value,Dimension_Hierarchy_1,Dimension_Name_1,Dimension_Value_1,Dimension_Hierarchy_2,Dimension_Name_2,Dimension_Value_2,...
 * I.e. the first 3 columns are fixed, followed by triplets that define a dimension: Hierarchy id (optional), Name and Value.
 */
public class InputCSVParserV3 implements DatapointParser {

    private static final Logger.ALogger logger = Logger.of(InputCSVParserV3.class);
    static final String END_OF_FILE = "*********";

    public static final int OBSERVATION_INDEX = 0;
    public static final int DATA_MARKING_INDEX = 1;
    public static final int OBSERVATION_TYPE_INDEX = 2;
    public static final int DIMENSION_START_INDEX = 3;

    private final ConcurrentMap<DimensionValueKey, DimensionValue> valueCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<DimensionKey, Dimension> dimensionCache = new ConcurrentHashMap<>();

    public InputCSVParserV3() {
    }

    public void parseRowdataDirectToTables(EntityManager em, String[] rowData, final DataSet dds, final UUID datapointId, Map<String, PreparedStatement> statements, long rowIndex) throws DatapointMappingException {

        String observation = getStringValue(rowData[OBSERVATION_INDEX], "0");
        if (END_OF_FILE.equals(observation)) {
            logger.info("Found end-of-file marker");
            return;
        }

        ArrayList<DimensionValue> dimensions = new ArrayList<>();
        Session session = em.unwrap(Session.class);

        for (int i = DIMENSION_START_INDEX; i < rowData.length; i = i + 3) {
            String hierarchyId = rowData[i];
            String dimensionName = rowData[i + 1];
            String dimensionValue = rowData[i + 2];

            logger.debug("Creating dimension for hierarchyId: " + hierarchyId + " and dimensionName: " + dimensionName + " and dimensionValue: " + dimensionValue + " ....");

            final DimensionValueKey key = new DimensionValueKey(dds.getId(), dimensionName, dimensionValue);
            final DimensionValue dimension = valueCache.computeIfAbsent(key, k -> {
                try {
                    return em.createNamedQuery(DimensionValue.FIND_QUERY, DimensionValue.class)
                            .setParameter(DimensionValue.DATASET_ID_PARAM, dds.getId())
                            .setParameter(DimensionValue.NAME_PARAM, dimensionName)
                            .setParameter(DimensionValue.VALUE_PARAM, dimensionValue)
                            .setFlushMode(FlushModeType.COMMIT)
                            .getSingleResult();
                } catch (NoResultException e) {
                    final HierarchyEntry hierarchyEntry = getHierarchyEntry(em, hierarchyId, dimensionValue);
                    Dimension dim = findOrCreateDimension(em, dds, dimensionName, hierarchyEntry, statements);
                    DimensionValue value = new DimensionValue(k.dimensionValue);
                    value.setDimension(dim);
                    value.setHierarchyEntry(hierarchyEntry);
                    session.doWork(connection -> {
                        PreparedStatement statement = statements.get("dimension_value");
                        int idx = 1;
                        statement.setObject(idx++, value.getId());
                        statement.setObject(idx++, dim.getId());
                        statement.setString(idx++, value.getValue());
                        statement.setObject(idx++, hierarchyEntry == null ? null : hierarchyEntry.getId());
                        statement.addBatch();
                    });
                    return value;
                }
            });

            dimensions.add(dimension);

        }

        session.doWork(connection -> {
            // insert datapoint
            PreparedStatement statement = statements.get("datapoint");
            int idx = 1;
            statement.setObject(idx++, dds.getId());
            statement.setLong(idx++, rowIndex);
            statement.setBigDecimal(idx++, new BigDecimal(observation));
            statement.setString(idx++, rowData[OBSERVATION_TYPE_INDEX]);
            statement.setString(idx++, isEmpty(rowData[DATA_MARKING_INDEX]) ? null : rowData[DATA_MARKING_INDEX]);
            statement.addBatch();
            // insert dimensions
            statement = statements.get("dimension_value_datapoint");
            for (DimensionValue dimension : dimensions) {
                idx = 1;
                statement.setObject(idx++, dimension.getId());
                statement.setObject(idx++, rowIndex);
                statement.addBatch();
            }
        });
    }

    private Dimension findOrCreateDimension(EntityManager em, DataSet dataSet, String name, HierarchyEntry hierarchyEntry, Map<String, PreparedStatement> statements) {
        final DimensionKey key = new DimensionKey(dataSet.getId(), name);
        return dimensionCache.computeIfAbsent(key, k -> {
            try {
                return em.createNamedQuery(Dimension.FIND_BY_DATA_SET_AND_NAME, Dimension.class)
                        .setParameter(Dimension.DATA_SET_PARAM, dataSet.getId())
                        .setParameter(Dimension.NAME_PARAM, name)
                        .setFlushMode(FlushModeType.COMMIT)
                        .getSingleResult();
            } catch (NoResultException ex) {
                Dimension dimension = new Dimension();
                dimension.setId(UUID.randomUUID());
                dimension.setName(name);
                dimension.setDataSet(dataSet);
                dimension.setType(hierarchyEntry != null ? hierarchyEntry.getHierarchy().getType() : Hierarchy.TYPE_NON_HIERARCHICAL);

                Session session = em.unwrap(Session.class);
                session.doWork(connection -> {
                    PreparedStatement statement = statements.get("dimension");
                    int idx = 1;
                    statement.setObject(idx++, dimension.getId());
                    statement.setObject(idx++, dataSet.getId());
                    statement.setString(idx++, name);
                    statement.setString(idx++, hierarchyEntry != null ? hierarchyEntry.getHierarchy().getType() : Hierarchy.TYPE_NON_HIERARCHICAL);
                    statement.addBatch();
                });
                return dimension;
            }
        });
    }

    private HierarchyEntry getHierarchyEntry(EntityManager em, String hierarchyId, String dimensionValue) {
        if (isNotEmpty(hierarchyId)) {
            try {
                return em.createNamedQuery(HierarchyEntry.FIND_QUERY, HierarchyEntry.class)
                        .setParameter(HierarchyEntry.HIERARCHY_ID_PARAM, hierarchyId)
                        .setParameter(HierarchyEntry.CODE_PARAM, dimensionValue)
                        .setFlushMode(FlushModeType.COMMIT) // Standing data, so should never need to flush first
                        .getSingleResult();
            } catch (NoResultException e) {
                throw new DatapointMappingException("Invalid data! No Hierarchy entry found in " + hierarchyId + " to match " + dimensionValue, e);
            }
        }
        return null;
    }


    private String getStringValue(String rowDatum, String defaultValue) {
        return rowDatum.trim().isEmpty() ? defaultValue : rowDatum.trim();
    }

    @Override
    public void close() {
        valueCache.clear();
    }

    private static class DimensionValueKey {
        private final UUID dataSetId;
        private final String dimensionName;
        private final String dimensionValue;

        public DimensionValueKey(UUID dataSetId, String dimensionName, String dimensionValue) {
            this.dataSetId = dataSetId;
            this.dimensionName = dimensionName;
            this.dimensionValue = dimensionValue;
        }

        @Override
        public boolean equals(Object that) {
            return this == that || that instanceof DimensionValueKey
                    && Objects.equals(this.dataSetId, ((DimensionValueKey) that).dataSetId)
                    && Objects.equals(this.dimensionName, ((DimensionValueKey) that).dimensionName)
                    && Objects.equals(this.dimensionValue, ((DimensionValueKey) that).dimensionValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataSetId, dimensionName, dimensionValue);
        }
    }

    private static class DimensionKey {
        private final UUID dataSetId;
        private final String name;

        public DimensionKey(UUID dataSetId, String name) {
            this.dataSetId = dataSetId;
            this.name = name;
        }

        @Override
        public boolean equals(Object that) {
            return this == that || that instanceof DimensionKey
                    && Objects.equals(dataSetId, ((DimensionKey) that).dataSetId)
                    && Objects.equals(name, ((DimensionKey) that).name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataSetId, name);
        }
    }
}
