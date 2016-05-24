/**
 * Copyright (C) 2013 - 2016 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of SQLDriverAbstract.
 *
 * SQLDriverAbstract is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation in version 3.
 *
 * SQLDriverAbstract is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SQLDriverAbstract. If not, see <http://www.gnu.org/licenses/>.
 *
 * SQLDriverAbstract is part of the OpenJEVis project, further project
 * information are published at <http://www.OpenJEVis.org/>.
 */
package org.jevis.sqldatasource.error;

/**
 *
 * @author Jingxuan Man
 */
public interface SQLDriverErrorNoContent {

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
