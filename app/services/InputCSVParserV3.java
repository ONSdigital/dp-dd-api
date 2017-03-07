package services;

import exceptions.DatapointMappingException;
import play.Logger;
import uk.co.onsdigital.discovery.model.*;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.commons.lang3.StringUtils.defaultString;
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

    public InputCSVParserV3() {
    }

    public void parseRowdataDirectToTables(EntityManager em, String[] rowData, final DataSet dds) throws DatapointMappingException {

        String observation = getStringValue(rowData[OBSERVATION_INDEX], "0");
        if (END_OF_FILE.equals(observation)) {
            logger.info("Found end-of-file marker");
            return;
        }

        ArrayList<DimensionValue> dimensions = new ArrayList<>();

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
                            .getSingleResult();
                } catch (NoResultException e) {
                    final HierarchyEntry hierarchyEntry = getHierarchyEntry(em, hierarchyId, dimensionValue);
                    Dimension dim = em.find(Dimension.class, new Dimension.DimensionPK(dds, k.dimensionName));
                    if (dim == null) {
                        dim = new Dimension(dds, dimensionName);
                        // There may be more than one hierarchy associated with a hierarchical dimension, but we
                        // assume they will all have the same type e.g. "geography" otherwise madness will ensue.
                        if (hierarchyEntry != null) {
                            dim.setType(hierarchyEntry.getHierarchy().getType());
                        } else {
                            dim.setType(Hierarchy.TYPE_NON_HIERARCHICAL);
                        }
                        em.persist(dim);
                    }
                    DimensionValue value = new DimensionValue(k.dimensionValue);
                    value.setDimension(dim);
                    value.setHierarchyEntry(hierarchyEntry);
                    em.persist(value);
                    return value;
                }
            });

            String existingHierarchyId = dimension.getHierarchyEntry() == null ? "" : defaultString(dimension.getHierarchyEntry().getHierarchy().getId());

            dimensions.add(dimension);

        }

        DataPoint dataPoint = new DataPoint();
        dataPoint.setId(UUID.randomUUID());
        dataPoint.setObservation(new BigDecimal(observation));
        if (isNotEmpty(rowData[DATA_MARKING_INDEX])) {
            dataPoint.setDataMarking(rowData[DATA_MARKING_INDEX]);
        }
        dataPoint.setObservationTypeValue(rowData[OBSERVATION_TYPE_INDEX]);
        dataPoint.setDimensionValues(dimensions);
        em.persist(dataPoint);
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
}
