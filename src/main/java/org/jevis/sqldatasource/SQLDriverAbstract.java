/**
 * Copyright (C) 2015 NeroBurner; Copyright (C) 2016 JingxuanMan
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * This driver is part of the OpenJEVis project, further project information are
 * published at <http://www.OpenJEVis.org/>.
 */
package org.jevis.sqldatasource;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;
import org.jevis.commons.driver.DataCollectorTypes;
import org.jevis.commons.driver.DataSource;
import org.jevis.commons.driver.Importer;
import org.jevis.commons.driver.ImporterFactory;
import org.jevis.commons.driver.Result;
import org.jevis.sqldatasource.error.SQLDriverError;
import org.jevis.sqldatasource.error.SQLDriverErrorNotFound;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 *
 * @author NeroBurner (AIT)
 * @author JingxuanMan (Envidatec)
 */
/**
 * This is an abstract driver to connect to a SQLServer Database. The structure
 * in JEVis for a single data point must be at least: SQL Server - SQL Channel
 * Directory - SQL Channel - Data Point Directory - Data Point. Besides you
 * could add SQL Variable Directory and SQL Variable
 */
public abstract class SQLDriverAbstract implements DataSource {

    private String _name;
    private String _host;
    private Integer _port;
    private String _schema;
    private String _dbUser;
    private String _dbPW;
    private Integer _connectionTimeout;
    private Integer _readTimeout;
    private String _timezone;
    private Boolean _enabled;
    private String _domain;

    protected Connection _con;
    private JEVisObject _dataSource;
    private List<JEVisObject> _channels;
    private List<Result> _result;
    private Importer _importer;

    interface SQLServer extends DataCollectorTypes.DataSource.DataServer {

        public final static String NAME = "SQL Server";
        public final static String PASSWORD = "Password";
        public final static String SCHEMA = "Schema";
        public final static String USER = "User";
        public final static String DOMAIN = "Domain";
    }

    interface SQLChannelDirectory extends DataCollectorTypes.ChannelDirectory {

        public final static String NAME = "SQL Channel Directory";
    }

    interface SQLChannel extends DataCollectorTypes.Channel {

        public final static String NAME = "SQL Channel";
        public final static String LAST_READOUT = "Last Readout";
        public final static String QUERY = "Query";
    }

    interface SQLDataPointDirectory extends DataCollectorTypes.DataPointDirectory {

        public final static String NAME = "SQL Data Point Directory";
    }

    interface SQLDataPoint extends DataCollectorTypes.DataPoint {

        public final static String NAME = "SQL Data Point";
        public final static String TARGET = "Target ID";
        public final static String TARGETATTRIBUTE = "Target Attribute";
        public final static String TIMESTAMPCOLUMN = "Timestamp Column";
        public final static String TIMESTAMPTYPE = "Timestamp Type";
        public final static String VALUETYPE = "Value Type";
        public final static String VALUECOLUMN = "Value Column";
    }

    interface SQLVariableDirectory {

        public final static String NAME = "SQL Variable Directory";
    }

    interface SQLVariable {

        public final static String NAME = "SQL Variable";
        public final static String VARIABLETYPE = "Variable Type";
        public final static String POSITION = "Position";
        public final static String CONDITION = "Condition";
    }

    private void initializeAttributes(JEVisObject sqlObject) {
        try {
            JEVisClass sqlType = sqlObject.getDataSource().getJEVisClass(getClassName());
            JEVisType host = sqlType.getType(SQLServer.HOST);
            JEVisType port = sqlType.getType(SQLServer.PORT);
            JEVisType schema = sqlType.getType(SQLServer.SCHEMA);
            JEVisType user = sqlType.getType(SQLServer.USER);
            JEVisType password = sqlType.getType(SQLServer.PASSWORD);
            JEVisType connectionTimeout = sqlType.getType(SQLServer.CONNECTION_TIMEOUT);
            JEVisType readTimeout = sqlType.getType(SQLServer.READ_TIMEOUT);
            JEVisType timezoneType = sqlType.getType(SQLServer.TIMEZONE);
            JEVisType enableType = sqlType.getType(SQLServer.ENABLE);
            _name = sqlObject.getName();
            _host = DatabaseHelper.getObjectAsString(sqlObject, host);
            _port = DatabaseHelper.getObjectAsInteger(sqlObject, port);
            _schema = DatabaseHelper.getObjectAsString(sqlObject, schema);
            JEVisAttribute userAttr = sqlObject.getAttribute(user);
            if (!userAttr.hasSample()) {
                _dbUser = "";
            } else {
                _dbUser = (String) userAttr.getLatestSample().getValue();
            }
            JEVisAttribute passAttr = sqlObject.getAttribute(password);
            if (!passAttr.hasSample()) {
                _dbPW = "";
            } else {
                _dbPW = (String) passAttr.getLatestSample().getValue();
            }
            _connectionTimeout = DatabaseHelper.getObjectAsInteger(sqlObject, connectionTimeout);
            _readTimeout = DatabaseHelper.getObjectAsInteger(sqlObject, readTimeout);
            _timezone = DatabaseHelper.getObjectAsString(sqlObject, timezoneType);
            _enabled = DatabaseHelper.getObjectAsBoolean(sqlObject, enableType);
        } catch (JEVisException ex) {
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject sqlObject) {
        try {
            _channels = getChannels(sqlObject);
            Logger.getLogger(
                      SQLDriverAbstract.class.getName()).log(Level.INFO, "Found Channels:");
            for (JEVisObject channel : _channels) {
                Logger.getLogger(
                          SQLDriverAbstract.class.getName()).log(Level.INFO, channel.getName());
            }
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(
                      SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

    private List<JEVisObject> getChannels(JEVisObject channelDir) throws JEVisException {
        ArrayList<JEVisObject> channels = new ArrayList<>();
        String msg = "ChannelDir: " + channelDir.getName();
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msg);
        JEVisClass channelDirClass
                  = channelDir.getDataSource().getJEVisClass(SQLChannelDirectory.NAME);
        JEVisClass channel
                  = channelDir.getDataSource().getJEVisClass(SQLChannel.NAME);
        List<JEVisObject> channelsDirs = channelDir.getChildren(channelDirClass, false);
        for (JEVisObject channelDirectory : channelsDirs) {
            channels.addAll(getChannels(channelDirectory));
        }
        channels.addAll(channelDir.getChildren(channel, false));
        return channels;
    }

    private List<JEVisObject> getVariables(JEVisObject variableDir) throws JEVisException {
        ArrayList<JEVisObject> variables = new ArrayList<>();
        String msg = "VariableDir: " + variableDir.getName();
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msg);
        JEVisClass variableDirClass
                  = variableDir.getDataSource().getJEVisClass(SQLVariableDirectory.NAME);
        JEVisClass variable
                  = variableDir.getDataSource().getJEVisClass(SQLVariable.NAME);
        List<JEVisObject> variableDirs = variableDir.getChildren(variableDirClass, false);
        for (JEVisObject variableDirectory : variableDirs) {
            variables.addAll(getVariables(variableDirectory));
        }
        variables.addAll(variableDir.getChildren(variable, false));
        return variables;
    }

    private List<JEVisObject> getDataPoints(JEVisObject dataPointDir) throws JEVisException {
        ArrayList<JEVisObject> dataPoints = new ArrayList<>();
        String msg = "DataPointDir: " + dataPointDir.getName();
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msg);
        JEVisClass dataPointDirClass
                  = dataPointDir.getDataSource().getJEVisClass(SQLDataPointDirectory.NAME);
        JEVisClass dataPoint
                  = dataPointDir.getDataSource().getJEVisClass(SQLDataPoint.NAME);
        List<JEVisObject> dataPointDirs = dataPointDir.getChildren(dataPointDirClass, false);
        for (JEVisObject dataPointDirectory : dataPointDirs) {
            dataPoints.addAll(getDataPoints(dataPointDirectory));
        }
        dataPoints.addAll(dataPointDir.getChildren(dataPoint, false));
        return dataPoints;
    }

//    abstract protected String loadJDBC(String host, int port, String schema, String dbUser, String dbPW) throws ClassNotFoundException, SQLException;
    abstract protected String loadJDBC(String host, int port, String schema, String dbUser, String dbPW, String domain)
              throws ClassNotFoundException, SQLException;

    abstract protected String getClassName();

    @Override
    public void parse(List<InputStream> input) {
    }

    @Override
    public void importResult() {
        _importer.importResult(_result);
    }

    @Override
    public void initialize(JEVisObject sqlObject) {
        _dataSource = sqlObject;
        initializeAttributes(sqlObject);
        initializeChannelObjects(sqlObject);
        _importer = ImporterFactory.getImporter(_dataSource);
        if (_importer != null) {
            _importer.initialize(_dataSource);
        }
    }

    @Override
    public void run() {
        try {
            String url = loadJDBC(_host, _port, _schema, _dbUser, _dbPW, _domain);
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(
                      SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        for (JEVisObject channel : _channels) {
            try {
                _result = new ArrayList<Result>();
                this.sendSampleRequest(channel);
                if (!_result.isEmpty()) {
                    this.importResult();
                }
            } catch (Exception ex) {
                Logger.getLogger(
                          SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType last_readoutType = channelClass.getType(SQLChannel.LAST_READOUT);
            JEVisType queryType = channelClass.getType(SQLChannel.QUERY);
            String last_readout = DatabaseHelper.getObjectAsString(channel, last_readoutType);
            String query = DatabaseHelper.getObjectAsString(channel, queryType);
            List<JEVisObject> variables;
            variables = getVariables(channel);
            Logger.getLogger(
                      SQLDriverAbstract.class.getName()).log(Level.INFO, "Found Variables:");
            List<JEVisObject> dataPoints;
            dataPoints = getDataPoints(channel);
            Logger.getLogger(
                      SQLDriverAbstract.class.getName()).log(Level.INFO, "Found DataPoints:");
            for (JEVisObject dp : dataPoints) {
                Logger.getLogger(
                          SQLDriverAbstract.class.getName()).log(Level.INFO, dp.getName());
            }
            String msg = "Prepared query: " + "\n" + query;
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msg);
            PreparedStatement ps = _con.prepareStatement(query);

            if (query.contains("?")) {
                for (JEVisObject va : variables) {
                    try {
                        setVariable(ps, va, channel);
                    } catch (Exception ex) {
                        System.out.println("Error in variable: " + ex);
                    }
                }
            }
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Query: " + "\n" + "SELECT Messzeit, Verbrauch FROM Energiedaten WHERE Messzeit > '2016-04-10 08:00:00'" + "\n" + "AND Verbrauch = 21;");
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Query: " + "\n" + "SELECT * FROM sample WHERE timestamp>'2016-04-27 11:00:00' AND value =24.45745;");

//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Query: " + "\n" + ps);
            try {
                ResultSet rs = ps.executeQuery();
                for (JEVisObject dp : dataPoints) {
                    while (rs.next()) {
                        try {
                            _result.add(parseResult(rs, dp));
                        } catch (Exception ex) {
                            System.out.println("Error while parsing sample: " + ex);
                        }
                    }
                }
            } catch (SQLException sqlError) {
                System.out.println("Error while executing Query: " + sqlError);
            }
        } catch (JEVisException | SQLException ex) {
            java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(
                      java.util.logging.Level.SEVERE, null, ex);
        }
        return null;
    }

    public void logError(SQLDriverError error, String text) {
        java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(
                  java.util.logging.Level.SEVERE, "{0} Error: {1}", new Object[]{error.getMessage(), text});
    }

    /**
     * Parse Result
     *
     * @param rs
     * @param dp
     * @return
     * @throws Exception
     */
    private Result parseResult(ResultSet rs, JEVisObject dp) throws Exception {
        JEVisClass dpClass = dp.getJEVisClass();
        long targetID = dp.getAttribute(SQLDataPoint.TARGET).getLatestSample().getValueAsLong();
        JEVisType targetAttributeType = dpClass.getType(SQLDataPoint.TARGETATTRIBUTE);
        String targetAttribute = DatabaseHelper.getObjectAsString(dp, targetAttributeType);
        JEVisType timestampColumnType = dpClass.getType(SQLDataPoint.TIMESTAMPCOLUMN);
        String timestampColumn = DatabaseHelper.getObjectAsString(dp, timestampColumnType);
        JEVisType valueColumnType = dpClass.getType(SQLDataPoint.VALUECOLUMN);
        String valueColumn = DatabaseHelper.getObjectAsString(dp, valueColumnType);
        JEVisType timestampTypeType = dpClass.getType(SQLDataPoint.TIMESTAMPTYPE);
        String timestampType = DatabaseHelper.getObjectAsString(dp, timestampTypeType);
        JEVisType valueTypeType = dpClass.getType(SQLDataPoint.VALUETYPE);
        String valueType = DatabaseHelper.getObjectAsString(dp, valueTypeType);

        String ts_str = rs.getString(timestampColumn);
        String val_str = rs.getString(valueColumn);
//        System.out.println("----------------------------------- Query----------------------------------- ");
        String msg = String.format("SQL-Driver: SQL-COL: %s, %s", ts_str, val_str);
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msg);
//        System.out.println("----------------------------------- Results----------------------------------- ");

        if (valueType.equalsIgnoreCase("double")) {
            double value = Double.parseDouble(val_str);
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        } else if (valueType.equalsIgnoreCase("float")) {
            float value = Float.parseFloat(val_str);
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        } else if (valueType.equalsIgnoreCase("long")) {
            long value = Long.parseLong(val_str);
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        } else if (valueType.equalsIgnoreCase("int")) {
            int value = Integer.parseInt(val_str);
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        } else if (valueType.equalsIgnoreCase("string")) {
            String value = val_str;
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        } else {
            logError(SQLDriverErrorNotFound.ERROR_404, "unknown value type: " + valueType);
            String value = val_str;
            DateTime dateTime = DateTimeFormat.forPattern(timestampType).parseDateTime(ts_str);
//            String msgs = String.format("After Parser: %s, %s", dateTime, value);
//            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, msgs);
//        JEVisObject targetObject
            Result result = new Result(targetID, targetAttribute, value, dateTime);
            return result;
        }
    }

    private void setVariable(PreparedStatement ps, JEVisObject va, JEVisObject channel) throws Exception {
        JEVisClass channelClass = channel.getJEVisClass();
        JEVisType last_readoutType = channelClass.getType(SQLChannel.LAST_READOUT);
        String last_readout = DatabaseHelper.getObjectAsString(channel, last_readoutType);
        String col_ts_format = "yyyy-MM-dd HH:mm:ss";
        DateTime lastReadout = null;
        if (channel.getAttribute(last_readoutType).hasSample()) {
            lastReadout = DatabaseHelper.getObjectAsDate(channel, last_readoutType,
                      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
        }
        if (lastReadout == null || DatabaseHelper.getObjectAsString(channel, last_readoutType).isEmpty()) {
            lastReadout = new DateTime(0);
        }
        DateTimeFormatter dbDateTimeFormatter = DateTimeFormat.forPattern(col_ts_format);
        last_readout = lastReadout.toString(DateTimeFormat.forPattern(col_ts_format));
        JEVisClass vaClass = va.getJEVisClass();
        JEVisType artType = vaClass.getType(SQLVariable.VARIABLETYPE);
        String art = DatabaseHelper.getObjectAsString(va, artType);
        int pos = Math.toIntExact(va.getAttribute(SQLVariable.POSITION).getLatestSample().getValueAsLong());
        JEVisType conditionType = vaClass.getType(SQLVariable.CONDITION);
        String condition = DatabaseHelper.getObjectAsString(va, conditionType);
        if (condition.equalsIgnoreCase("lastreadout")) {
            last_readout = lastReadout.toString(DateTimeFormat.forPattern(col_ts_format));
            condition = condition.replaceAll(condition, last_readout);
        }
        if (!va.getAttribute(SQLVariable.CONDITION).hasSample()) {
            condition = last_readout;
        }
        parseVariableFormat(ps, art, pos, condition, channel);
    }

    private void parseVariableFormat(PreparedStatement ps, String art, int pos, String condition, JEVisObject channel) throws Exception {
        if (art.equalsIgnoreCase("double")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Double.parseDouble(condition));
            ps.setDouble(pos, Double.parseDouble(condition));
        } else if (art.equalsIgnoreCase("float")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Float.parseFloat(condition));
            ps.setFloat(pos, Float.parseFloat(condition));
        } else if (art.equalsIgnoreCase("long")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Long.parseLong(condition));
            ps.setLong(pos, Long.parseLong(condition));
        } else if (art.equalsIgnoreCase("int")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Integer.parseInt(condition));
            ps.setInt(pos, Integer.parseInt(condition));
        } else if (art.equalsIgnoreCase("date")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Date.valueOf(condition));
            ps.setDate(pos, Date.valueOf(condition));
        } else if (art.equalsIgnoreCase("time")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + Time.valueOf(condition));
            ps.setTime(pos, Time.valueOf(condition));
        } else if (art.equalsIgnoreCase("timestamp")) {
            System.out.println("---> Set variable in type: " + art + "; Position: " + pos + "; Condition: " + condition);
            DateTimeFormatter dbDateTime = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
            ps.setTimestamp(pos, new Timestamp(dbDateTime.parseMillis(condition)));
        } else if (art.equalsIgnoreCase("string")) {
            System.out.println("---> Set variable in type: " + art + "Position: " + pos + "  " + condition);
            ps.setString(pos, condition);
        } else {
            logError(SQLDriverErrorNotFound.ERROR_404, "unknown varibale type: " + art);
        }
    }

}
