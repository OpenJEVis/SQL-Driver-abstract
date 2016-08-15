/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.sqldatasource;

import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;

/**
 * Parse the target configuration from an DataPoint object
 *
 * TODO: The SQlDriver work with to moch strings, should use more objects
 *
 * @author fs
 */
public class Target {

    private final JEVisAttribute targetAtt;
    private final String targetAttribute;
    private final long targetID;
    private final String timestampColumn;
    private final String valueColumn;
    private final String timestampType;
    private final String valueType;

    /**
     *
     * TOTO: catch/handel exeptions
     *
     * @param datapoint
     * @throws Exception
     */
    public Target(JEVisObject datapoint) throws Exception {
        JEVisClass dpClass = datapoint.getJEVisClass();
        targetID = datapoint.getAttribute(SQLDriverAbstract.SQLDataPoint.TARGET).getLatestSample().getValueAsLong();
        JEVisType targetAttributeType = dpClass.getType(SQLDriverAbstract.SQLDataPoint.TARGETATTRIBUTE);
        targetAttribute = DatabaseHelper.getObjectAsString(datapoint, targetAttributeType);
        JEVisType timestampColumnType = dpClass.getType(SQLDriverAbstract.SQLDataPoint.TIMESTAMPCOLUMN);
        timestampColumn = DatabaseHelper.getObjectAsString(datapoint, timestampColumnType);
        JEVisType valueColumnType = dpClass.getType(SQLDriverAbstract.SQLDataPoint.VALUECOLUMN);
        valueColumn = DatabaseHelper.getObjectAsString(datapoint, valueColumnType);
        JEVisType timestampTypeType = dpClass.getType(SQLDriverAbstract.SQLDataPoint.TIMESTAMPTYPE);
        timestampType = DatabaseHelper.getObjectAsString(datapoint, timestampTypeType);
        JEVisType valueTypeType = dpClass.getType(SQLDriverAbstract.SQLDataPoint.VALUETYPE);
        valueType = DatabaseHelper.getObjectAsString(datapoint, valueTypeType);
        targetAtt = datapoint.getDataSource().getObject(targetID).getAttribute(targetAttribute);
    }

    public JEVisAttribute getAttribute() {
        return targetAtt;
    }

    public String getAttributeName() {
        return targetAttribute;
    }

    public long getObjectID() {
        return targetID;
    }

    public String getTimestampColumn() {
        return timestampColumn;
    }

    public String getValueColumn() {
        return valueColumn;
    }

    public String getTimestampType() {
        return timestampType;
    }

    public String getValueType() {
        return valueType;
    }

}
