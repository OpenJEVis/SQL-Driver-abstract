/**
 * Copyright (C) 2015 NeroBurner
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisType;
import org.jevis.commons.DatabaseHelper;
import org.jevis.commons.driver.DataSourceHelper;
import org.jevis.commons.driver.Importer;
import org.jevis.commons.driver.ImporterFactory;
import org.jevis.commons.driver.DataCollectorTypes;
import org.jevis.commons.driver.Result;
import org.jevis.commons.driver.DataSource;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;



/**
 * This is an abstract driver to connect to a SQL Database.
 * 
 * The structure in JEVis for a single data point must be at least:
 * SQL Server
 * - SQL Channel Directory
 *   - Data Point Directory (Optional)
 *     - Data Point
 * 
 * @author NeroBurner
 */
public abstract class SQLDriverAbstract implements DataSource {
    interface SQL extends DataCollectorTypes.DataSource.DataServer {
        // from parent-class
        //public final static String NAME = "Data Server";
        //public final static String CONNECTION_TIMEOUT = "Connection Timeout";
        //public final static String READ_TIMEOUT = "Read Timeout";
        //public final static String HOST = "Host";
        //public final static String PORT = "Port";

        public final static String NAME = "SQL Server";
        public final static String SCHEMA = "Schema";
        public final static String USER = "User";
        public final static String PASSWORD = "Password";
    }

    interface SQLChannelDirectory extends DataCollectorTypes.ChannelDirectory {

        public final static String NAME = "SQL Channel Directory";
    }

    interface SQLChannel extends DataCollectorTypes.Channel {
        // from parent
        // public final static String LAST_READOUT = "Last Readout";

        public final static String NAME = "SQL Channel";
        public final static String TABLE = "Table";
        public final static String COL_ID = "Column ID"; // optional
        public final static String COL_TS = "Column Timestamp";
        public final static String COL_TS_FORMAT = "Timestamp Format";
        public final static String COL_VALUE = "Column Value";

    }

    interface SQLDataPointDirectory extends DataCollectorTypes.DataPointDirectory {

        public final static String NAME = "SQL Data Point Directory";
    }

    interface SQLDataPoint extends DataCollectorTypes.DataPoint {

        public final static String NAME = "SQL Data Point";
        public final static String ID = "ID";
        public final static String TARGET = "Target";
    }
    
    
    // Attributes
    private Long _id;
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

    // Global Variables
    private Importer _importer;
    private List<JEVisObject> _channels;
    private List<Result> _result;

    private JEVisObject _dataSource;
    protected Connection _con;

    @Override
    public void parse(List<InputStream> input) {}
    
    /**
     * Load appropriate jdbc driver and set protected SQL-connection _con
     * @param host Hostname or IP of the SQL-database to connect to
     * @param port the used TCP-port
     * @param schema Database/Schema name
     * @param dbUser User used to connect to the SQL-database
     * @param dbPW Password of the user
     * @return URL used to connect to the database, for debugging
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    abstract protected String loadJDBC(String host, int port, String schema,
            String dbUser, String dbPW)
            throws ClassNotFoundException, SQLException;
    
    /**
     * Get the name used for this driver, for example 'SQL Server'.
     * @return Name of the class used in JEVis
     */
    abstract protected String getClassName();
    
    @Override
    public void run() {
        try {
            String url = loadJDBC(_host, _port, _schema, _dbUser, _dbPW);
            
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        for (JEVisObject channel : _channels) {

            try {
                _result = new ArrayList<Result>();
                
                // Get samples from sql-database and parse into results
                this.sendSampleRequest(channel);

                // Import Results
                if (!_result.isEmpty()) {
                    this.importResult();   
                    Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Setting lastReadout to: " + _importer.getLatestDatapoint().toString());
                    DataSourceHelper.setLastReadout(channel, _importer.getLatestDatapoint());
                }
            } catch (Exception ex) {
                //TODO: remove this generic exception-catching
                Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
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

    /**
     * Get samples from SQL-database and parse into results
     *
     * @param channel defines the table to query from
     * @return
     */
    @Override
    public List<InputStream> sendSampleRequest(JEVisObject channel) {
        try {
            JEVisClass channelClass = channel.getJEVisClass();
            JEVisType tableType = channelClass.getType(SQLChannel.TABLE);
            JEVisType col_idType = channelClass.getType(SQLChannel.COL_ID);
            JEVisType col_tsType = channelClass.getType(SQLChannel.COL_TS);
            JEVisType col_tsFormatType = channelClass.getType(SQLChannel.COL_TS_FORMAT);
            JEVisType valueType = channelClass.getType(SQLChannel.COL_VALUE);
            String table = DatabaseHelper.getObjectAsString(channel, tableType);
            String col_id = DatabaseHelper.getObjectAsString(channel, col_idType);
            if (col_id == null)
                col_id = "";
            String col_ts = DatabaseHelper.getObjectAsString(channel, col_tsType);
            String col_ts_format = DatabaseHelper.getObjectAsString(channel, col_tsFormatType);
            String col_value = DatabaseHelper.getObjectAsString(channel, valueType);
            JEVisType readoutType = channelClass.getType(SQLChannel.LAST_READOUT);
            // TODO: this pattern should be in JECommons
            DateTime lastReadout = null;
            if (channel.getAttribute(readoutType).hasSample()) {
                lastReadout = DatabaseHelper.getObjectAsDate(channel, readoutType, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
            }
            // Either there is no sample or there was a sample and it is empty
            if (lastReadout == null || DatabaseHelper.getObjectAsString(channel, readoutType).isEmpty()) {
                lastReadout = new DateTime(0);
            }
            
            String sql_lastReadout;
            DateTimeFormatter dbDateTimeFormatter = DateTimeFormat.forPattern(col_ts_format);
            sql_lastReadout = lastReadout.toString(DateTimeFormat.forPattern(col_ts_format));
            
            // Prepare SQL-Statement
            // only include column if it is defined
            String col_id_sql_str = "";
            if (!col_id.isEmpty())
                col_id_sql_str = col_id + ',';
                
            String sql_query = String.format("select %s %s, %s",
                    col_id_sql_str, col_ts, col_value);
            sql_query += " from " + table;
            sql_query += String.format(" where %s > '%s'", col_ts, sql_lastReadout);
            if (!col_id.isEmpty())
                sql_query += " and " + col_id + " =?";
            sql_query += ";";
            PreparedStatement ps = _con.prepareStatement(sql_query);
            
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "SQL-Driver: Prepared querry: " + sql_query);
            
            List<JEVisObject> dataPoints;
            try {
                // Recursively get all datapoints under the current channel
                dataPoints = getDataPoints(channel);
                Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Found DataPoints:");
                for(JEVisObject dp : dataPoints) {
                    Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, dp.getName());
                }
                
            } catch (JEVisException ex) {
                java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
                return null;
            }
            // Create query for each datapoint
            for (JEVisObject dp : dataPoints) {
                JEVisClass dpClass = dp.getJEVisClass();

                JEVisType idType = dpClass.getType(SQLDataPoint.ID);
                JEVisType targetType = dpClass.getType(SQLDataPoint.TARGET);
                
                String id = DatabaseHelper.getObjectAsString(dp, idType);
                Long target = DatabaseHelper.getObjectAsLong(dp, targetType);
                
                // Querry for ID given by the datapoint
                if (!col_id.isEmpty())
                    ps.setString(1, id);
                ResultSet rs = ps.executeQuery();
                
                try {
                    // Parse the results
                    while (rs.next()) {
                        String ts_str = rs.getString(col_ts);
                        String val_str = rs.getString(col_value);
                        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, String.format("SQL-Driver: SQL-COL: %s, %s, %s", id, ts_str, val_str));    
                        
                        // Parse value and timestamp
                        double value = Double.parseDouble(val_str);
                        DateTime dateTime = dbDateTimeFormatter.parseDateTime(ts_str);
                        
                        // add to results
                        _result.add(new Result(target, value, dateTime));
                    }
                } catch (NumberFormatException nfe) {
                    java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, nfe);
                } catch (SQLException ex) {
                    java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
                }
            }
            
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        return null;
    }

    private void initializeAttributes(JEVisObject sqlObject) {
        try {
            JEVisClass sqlType = sqlObject.getDataSource().getJEVisClass(getClassName());
            JEVisType host = sqlType.getType(SQL.HOST);
            JEVisType port = sqlType.getType(SQL.PORT);
            JEVisType schema = sqlType.getType(SQL.SCHEMA);
            JEVisType user = sqlType.getType(SQL.USER);
            JEVisType password = sqlType.getType(SQL.PASSWORD);
            JEVisType connectionTimeout = sqlType.getType(SQL.CONNECTION_TIMEOUT);
            JEVisType readTimeout = sqlType.getType(SQL.READ_TIMEOUT);
            JEVisType timezoneType = sqlType.getType(SQL.TIMEZONE);
            JEVisType enableType = sqlType.getType(SQL.ENABLE);

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
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void initializeChannelObjects(JEVisObject sqlObject) {
        try {
            _channels = getChannels(sqlObject);
            Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "Found Channels:");
            for(JEVisObject channel : _channels) {
                Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, channel.getName());
            }
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(SQLDriverAbstract.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
    }
    private List<JEVisObject> getChannels(JEVisObject channelDirObject) throws JEVisException {
        ArrayList<JEVisObject> channels = new ArrayList<>();
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "ChannelDir: " + channelDirObject.getName());

        // Get Classes
        JEVisClass channelDirClass = channelDirObject.getDataSource().getJEVisClass(SQLChannelDirectory.NAME);
        JEVisClass channelClass = channelDirObject.getDataSource().getJEVisClass(SQLChannel.NAME);
        
        // Go deeper
        List<JEVisObject> channelsDirs = channelDirObject.getChildren(channelDirClass, false);
        for (JEVisObject cDir : channelsDirs) {
            channels.addAll(getChannels(cDir));
        }
        
        // Add all channels
        channels.addAll(channelDirObject.getChildren(channelClass, false));
        
        return channels;
    }
    
    private List<JEVisObject> getDataPoints(JEVisObject channelObject) throws JEVisException {
        ArrayList<JEVisObject> dataPoints = new ArrayList<>();
        
        Logger.getLogger(SQLDriverAbstract.class.getName()).log(Level.INFO, "DataPointDir: " + channelObject.getName());

        // Get Classes
        JEVisClass dpDirClass = channelObject.getDataSource().getJEVisClass(SQLDataPointDirectory.NAME);
        JEVisClass dpClass = channelObject.getDataSource().getJEVisClass(SQLDataPoint.NAME);
        
        // Go deeper
        List<JEVisObject> dpDirs = channelObject.getChildren(dpDirClass, false);
        for (JEVisObject dpDir : dpDirs) {
            dataPoints.addAll(getDataPoints(dpDir));
        }
        
        // Add all Data Points
        dataPoints.addAll(channelObject.getChildren(dpClass, false));
        
        return dataPoints;
    }

}
