package services;

import exceptions.DatapointMappingException;
import play.Logger;
import uk.co.onsdigital.discovery.model.DataPoint;
import uk.co.onsdigital.discovery.model.DimensionValue;
import uk.co.onsdigital.discovery.model.DimensionalDataSet;
import uk.co.onsdigital.discovery.model.HierarchyEntry;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static services.InputCSVParser.END_OF_FILE;

/**
 * Capable of importing rows in the v3 format into the db. The format is as follows:
 * <p>
 * Observation,Data_Marking,Observation_Type_Value,Dimension_Hierarchy_1,Dimension_Name_1,Dimension_Value_1,Dimension_Hierarchy_2,Dimension_Name_2,Dimension_Value_2,...
 * I.e. the first 3 columns are fixed, followed by triplets that define a dimension: Hierarchy id (optional), Name and Value.
 */
public class InputCSVParserV3 implements DatapointParser {

    private static final Logger.ALogger logger = Logger.of(InputCSVParserV3.class);

    public static final int OBSERVATION_INDEX = 0;
    public static final int DATA_MARKING_INDEX = 1;
    public static final int OBSERVATION_TYPE_INDEX = 2;
    public static final int DIMENSION_START_INDEX = 3;

    public InputCSVParserV3() {
    }

    public void parseRowdataDirectToTables(EntityManager em, String[] rowData, final DimensionalDataSet dds) throws DatapointMappingException {

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

            DimensionValue dimension;
            List<DimensionValue> existing = em.createNamedQuery(DimensionValue.FIND_QUERY, DimensionValue.class)
                    .setParameter(DimensionValue.DATASET_ID_PARAM, dds.getId())
                    .setParameter(DimensionValue.NAME_PARAM, dimensionName)
                    .setParameter(DimensionValue.VALUE_PARAM, dimensionValue)
                    .getResultList();
            if (existing.isEmpty()) {
                dimension = new DimensionValue(dds.getId(), dimensionName, dimensionValue);
                dimension.setHierarchyEntry(getHierarchyEntry(em, hierarchyId, dimensionValue));
                em.persist(dimension);
            } else {
                dimension = existing.get(0);
            }

            String existingHierarchyId = dimension.getHierarchyEntry() == null ? "" : defaultString(dimension.getHierarchyEntry().getHierarchyId());
            if (!existingHierarchyId.equals(defaultString(hierarchyId))) {
                throw new DatapointMappingException("Inconsistent data! Existing DimensionValue " + dimension + " has hierarchy id " + existingHierarchyId + " - expected " + hierarchyId);
            }


            dimensions.add(dimension);

        }

        DataPoint dataPoint = new DataPoint();
        dataPoint.setId(UUID.randomUUID());
        dataPoint.setObservation(new BigDecimal(observation));
        if (isNotEmpty(rowData[DATA_MARKING_INDEX])) {
            dataPoint.setDataMarking(rowData[DATA_MARKING_INDEX]);
        }
        if (isNotEmpty(rowData[OBSERVATION_TYPE_INDEX])) {
            dataPoint.setObservationTypeValue(new BigDecimal(rowData[OBSERVATION_TYPE_INDEX]));
        }
        dataPoint.setDimensionValues(dimensions);
        em.persist(dataPoint);
    }

    private HierarchyEntry getHierarchyEntry(EntityManager em, String hierarchyId, String dimensionValue) throws DatapointMappingException {
        if (isNotEmpty(hierarchyId)) {
            try {
                return em.createNamedQuery(HierarchyEntry.FIND_QUERY, HierarchyEntry.class)
                        .setParameter(HierarchyEntry.HIERARCHY_ID_PARAM, hierarchyId)
                        .setParameter(HierarchyEntry.CODE_PARAM, dimensionValue)
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

}
