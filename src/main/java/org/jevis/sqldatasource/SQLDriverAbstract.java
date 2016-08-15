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
import org.jevis.api.JEVisSample;
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

    private String _name = "";
    private String _host;
    private Integer _port;
    private String _schema;
    private long _id = 0l;
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
            _id = sqlObject.getID();
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
            logErrorMessage(Level.SEVERE, ex, "Error while initialize SQL Server");
        }
    }

    private void initializeChannelObjects(JEVisObject sqlObject) {
        try {
            _channels = getChannels(sqlObject);
            logMessage(Level.INFO, "Found Channels:");
            for (JEVisObject channel : _channels) {
                logMessage(Level.INFO, "Channel: %s", channel.getName());
            }
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(
                    SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }

    private List<JEVisObject> getChannels(JEVisObject channelDir) throws JEVisException {
        ArrayList<JEVisObject> channels = new ArrayList<>();
        logMessage(Level.INFO, "ChannelDir: %s ", channelDir.getName());
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
        logMessage(Level.INFO, "VariableDir: %s ", variableDir.getName());
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
        logMessage(Level.INFO, "DataPointDir: %s", dataPointDir.getName());
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
        logMessage(Level.INFO, "Import %s samples", _result.size());
        _importer.importResult(_result);
        // is this the corret postion?

    }

    private void setLastReadout(List<Result> results, JEVisObject channel) {
        DateTime oldesSample = null;
        for (Result result : results) {
            if (oldesSample == null) {
                oldesSample = result.getDate();
            } else if (result.getDate().isAfter(oldesSample)) {
                oldesSample = result.getDate();
            }
        }
        if (oldesSample != null) {
            try {
                JEVisAttribute lastReadout = channel.getAttribute(SQLChannel.LAST_READOUT);
                JEVisSample sample = lastReadout.getLatestSample();

                String lts = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(oldesSample);
                lastReadout.buildSample(new DateTime(), lts).commit();
                logMessage(Level.INFO, "Set LastReadout to: %s", lts);

            } catch (Exception ex) {
                logErrorMessage(Level.SEVERE, ex, "Error while setting lastReadout: ", "");
            }
        }

    }

    @Override
    public void initialize(JEVisObject sqlObject) {
        logMessage(Level.INFO, "initialize SQLDriverAbstract Version %s", "2016-08-02");

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
                if (_result != null && !_result.isEmpty()) {
                    this.importResult();
                    setLastReadout(_result, channel);
                } else {
                    logMessage(Level.INFO, "Nothing to import");
                }
            } catch (Exception ex) {
                logErrorMessage(Level.SEVERE, ex, "Error in channel: " + channel.getID() + " " + channel.getName());
            }
        }
        try {
            if (_con != null) {
                _con.close();
            }
        } catch (Exception ex) {
            logMessage(Level.INFO, "Error while closing DB connection");
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
            logMessage(Level.INFO, "Found Variables");
            List<JEVisObject> dataPoints;
            dataPoints = getDataPoints(channel);
            logMessage(Level.INFO, "Found DataPoints");
            for (JEVisObject dp : dataPoints) {
                logMessage(Level.INFO, "DP: %s", dp.getName());
            }
            PreparedStatement ps = _con.prepareStatement(query);

            try {
                if (query.contains("?")) {
                    for (JEVisObject va : variables) {
                        try {
                            setVariable(ps, va, channel);
                        } catch (Exception ex) {
                            logErrorMessage(Level.SEVERE, ex, "Error in variable:");
                        }
                    }
                }

                logMessage(Level.INFO, "Query: %s", ps);

                List<Target> targets = new ArrayList<>();
                for (JEVisObject dp : dataPoints) {
                    try {
                        targets.add(new Target(dp));
                    } catch (Exception ex) {
                        logErrorMessage(Level.SEVERE, ex, "Error while reading taget configuration: " + ex);
                    }
                }

                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    for (Target target : targets) {
                        try {
                            Result result = parseResult(rs, target);
                            logMessage(Level.FINE, "After Parser: %s, %s", result.getDate(), result.getValue());
                            _result.add(result);
                        } catch (Exception ex) {
                            logErrorMessage(Level.FINE, ex, "Error while parsing sample");
                        }
                    }
                }

            } catch (SQLException sqlError) {
                logErrorMessage(Level.SEVERE, sqlError, "Error while executing query");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        } catch (JEVisException | SQLException ex) {
            java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(
                    java.util.logging.Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * Note: not in use? it should be used with the SQLDRiverError
     *
     * @param error
     * @param text
     */
    public void logError(SQLDriverError error, String text) {
        java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(
                java.util.logging.Level.SEVERE, "{0} Error: {1}", new Object[]{error.getMessage(), text});
    }

    /**
     * Common foramte logmessage to improve the readability of the log
     *
     * @param level Log level
     * @param message Message to formate
     * @param args List of arguments, insert using String.formate
     */
    public void logMessage(Level level, String message, Object... args) {
        String header = String.format("[ %s %s] ", _id, _name);
        String lmessage = String.format(message, args);
        try {
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(level, "{0}{1}", new Object[]{header, lmessage});
        } catch (Exception lex) {
            System.out.println("Logger error for message:  '" + message + "':" + lex);
        }
    }

    public void logErrorMessage(Level level, Exception ex, String message, Object... args) {
        String header = String.format("[ %s %s] ", _id, _name);
        String lmessage = String.format(message, args);
        try {
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(level, header + lmessage, ex);
        } catch (Exception lex) {
            System.out.println("Logger error for message:  '" + message + "':" + lex);
        }
    }

    private Result parseResult(ResultSet rs, Target target) throws Exception {

//        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.FINE, "TS-type:'" + timestampType + "' TS Colum: '" + timestampColumn + "' V-colum: '" + valueColumn + "'");
//        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.FINE, "Value: '" + rs.getString(valueColumn) + "' TS: '" + rs.getString(timestampColumn) + "'");
        DateTime dateTime = null;

        if (target.getTimestampType().equalsIgnoreCase("date")) {
            dateTime = new DateTime(rs.getDate(target.getTimestampColumn()).getTime());
        } else if (target.getTimestampType().equalsIgnoreCase("timestamp")) {
            dateTime = new DateTime(rs.getTimestamp(target.getTimestampColumn()).getTime());
        } else {
            dateTime = DateTimeFormat.forPattern(target.getTimestampType()).parseDateTime(rs.getString(target.getTimestampColumn()));
        }

        if (target.getValueType().equalsIgnoreCase("double")) {
            Double value = rs.getDouble(target.getValueColumn());
            Result result = new Result(target.getObjectID(), target.getAttributeName(), value, dateTime);
            return result;
        } else if (target.getValueType().equalsIgnoreCase("float")) {
            Float value = rs.getFloat(target.getValueColumn());
            Result result = new Result(target.getObjectID(), target.getAttributeName(), value, dateTime);
            return result;
        } else if (target.getValueType().equalsIgnoreCase("long")) {
            Long value = rs.getLong(target.getValueColumn());
            Result result = new Result(target.getObjectID(), target.getAttributeName(), value, dateTime);
            return result;
        } else if (target.getValueType().equalsIgnoreCase("int")) {
            int value = rs.getInt(target.getValueColumn());
            Result result = new Result(target.getObjectID(), target.getAttributeName(), value, dateTime);
            return result;
        } else if (target.getValueType().equalsIgnoreCase("string")) {
            String value = rs.getString(target.getValueColumn());
            Result result = new Result(target.getObjectID(), target.getAttributeName(), value, dateTime);
            return result;
        } else {
            logError(SQLDriverErrorNotFound.ERROR_404, "unknown value type: " + target.getValueType());
            throw new RuntimeException("Unknow value type");
        }
    }

    private void setVariable(PreparedStatement ps, JEVisObject va, JEVisObject channel) throws Exception {
        JEVisClass channelClass = channel.getJEVisClass();
        String col_ts_format = "yyyy-MM-dd HH:mm:ss";

        JEVisClass vaClass = va.getJEVisClass();
        JEVisType artType = vaClass.getType(SQLVariable.VARIABLETYPE);
        String art = DatabaseHelper.getObjectAsString(va, artType);
        int pos = Math.toIntExact(va.getAttribute(SQLVariable.POSITION).getLatestSample().getValueAsLong());
        JEVisType conditionType = vaClass.getType(SQLVariable.CONDITION);

        String condition = DatabaseHelper.getObjectAsString(va, conditionType);

        if (condition.equalsIgnoreCase("lastreadout")) {
            JEVisType last_readoutType = channelClass.getType(SQLChannel.LAST_READOUT);

            String last_readout = DatabaseHelper.getObjectAsString(channel, last_readoutType);

            DateTime lastReadout = null;
            if (channel.getAttribute(last_readoutType).hasSample()) {
                lastReadout = DatabaseHelper.getObjectAsDate(channel, last_readoutType,
                        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            }
            if (lastReadout == null || DatabaseHelper.getObjectAsString(channel, last_readoutType).isEmpty()) {
                lastReadout = new DateTime(0);
            }
            last_readout = lastReadout.toString(DateTimeFormat.forPattern(col_ts_format));
            condition = condition.replaceAll(condition, last_readout);

            setVariableInStatement(ps, art, pos, condition, channel);
        } else {
            //.... where are the other?!
        }

        //this if for?
//        if (!va.getAttribute(SQLVariable.CONDITION).hasSample()) {
//            condition = last_readout;
//        }
    }

    /**
     *
     * NOTE: FS, art should be an enum NOTE: FS, condition as String is bad,
     * Object whould be better
     *
     * @param ps
     * @param art
     * @param pos
     * @param condition
     * @param channel
     * @throws Exception
     */
    private void setVariableInStatement(PreparedStatement ps, String art, int pos, String condition, JEVisObject channel) throws Exception {
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
            DateTimeFormatter dbDateTime = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            ps.setTimestamp(pos, new Timestamp(dbDateTime.parseMillis(condition)));
        } else if (art.equalsIgnoreCase("string")) {
            System.out.println("---> Set variable in type: " + art + "Position: " + pos + "  " + condition);
            ps.setString(pos, condition);
        } else {
            logError(SQLDriverErrorNotFound.ERROR_404, "unknown varibale type: " + art);
        }
    }

}
