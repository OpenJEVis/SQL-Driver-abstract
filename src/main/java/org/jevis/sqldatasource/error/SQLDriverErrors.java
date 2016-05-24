/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.sqldatasource.error;

/**
 *
 * @author jm
 */
public interface SQLDriverErrors {

    public static final SQLDriverError ERROR_400 = new SQLDriverError(400, "Bad Request!");
    public static final SQLDriverError ERROR_4001 = new SQLDriverError(4001, "Bad Request of Connecting Server!");
    public static final SQLDriverError ERROR_4002 = new SQLDriverError(4002, "Bad Request of Getting Query!");
    public static final SQLDriverError ERROR_4003 = new SQLDriverError(4003, "Bad Request of Importing Data!");
    public static final SQLDriverError ERROR_4004 = new SQLDriverError(4004, "Bad Request of URL!");
    public static final SQLDriverError ERROR_4005 = new SQLDriverError(4005, "Bad Request of User or Password!");
    public static final SQLDriverError ERROR_4006 = new SQLDriverError(4006, "Bad Request of Setting Variable!");
    public static final SQLDriverError ERROR_4007 = new SQLDriverError(4007, "Bad Request of Parsing Result!");

    public static final SQLDriverError ERROR_404 = new SQLDriverError(404, "Not Found!");
    public static final SQLDriverError ERROR_4040 = new SQLDriverError(4040, "SQL Server Not Found!");
    public static final SQLDriverError ERROR_4041 = new SQLDriverError(4041, "SQL Channel Directory Not Found!");
    public static final SQLDriverError ERROR_4042 = new SQLDriverError(4042, "SQL Channel Not Found!");
    public static final SQLDriverError ERROR_4043 = new SQLDriverError(4043, "SQL Variable Directory Not Found!");
    public static final SQLDriverError ERROR_4044 = new SQLDriverError(4044, "SQL Variable Not Found!");
    public static final SQLDriverError ERROR_4045 = new SQLDriverError(4045, "SQL Data Point Directory Not Found!");
    public static final SQLDriverError ERROR_4046 = new SQLDriverError(4046, "SQL Data Point Not Found!");

    public static final SQLDriverError ERROR_204 = new SQLDriverError(404, "No Content!");
    public static final SQLDriverError ERROR_2041 = new SQLDriverError(4041, "Domain No Content!");
    public static final SQLDriverError ERROR_2042 = new SQLDriverError(4042, "Host No Content!");
    public static final SQLDriverError ERROR_2043 = new SQLDriverError(4043, "Port No Content!");
    public static final SQLDriverError ERROR_2044 = new SQLDriverError(4044, "Schema No Content!");
    public static final SQLDriverError ERROR_2045 = new SQLDriverError(4045, "User No Content!");
    public static final SQLDriverError ERROR_2046 = new SQLDriverError(4046, "Password No Content!");
    public static final SQLDriverError ERROR_2047 = new SQLDriverError(4047, "Query No Content!");
    public static final SQLDriverError ERROR_2048 = new SQLDriverError(4048, "Position No Content!");
    public static final SQLDriverError ERROR_2049 = new SQLDriverError(4049, "Variable Type No Content!");
    public static final SQLDriverError ERROR_20410 = new SQLDriverError(40410, "Condition No Content!");
    public static final SQLDriverError ERROR_20411 = new SQLDriverError(40411, "Target ID No Content!");
    public static final SQLDriverError ERROR_20412 = new SQLDriverError(40412, "Target Attribute No Content!");
    public static final SQLDriverError ERROR_20413 = new SQLDriverError(40413, "Timestamp Column No Content!");
    public static final SQLDriverError ERROR_20414 = new SQLDriverError(40414, "Timeatamp Type No Content!");
    public static final SQLDriverError ERROR_20415 = new SQLDriverError(40415, "Value Column No Content!");
    public static final SQLDriverError ERROR_20416 = new SQLDriverError(40416, "Value Type No Content!");
}
