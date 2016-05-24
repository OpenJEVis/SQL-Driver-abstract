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
public interface SQLDriverErrorBadRequest {

    public static final SQLDriverError ERROR_400 = new SQLDriverError(400, "Bad Request!");
    public static final SQLDriverError ERROR_4001 = new SQLDriverError(4001, "Bad Request of Connecting Server!");
    public static final SQLDriverError ERROR_4002 = new SQLDriverError(4002, "Bad Request of Getting Query!");
    public static final SQLDriverError ERROR_4003 = new SQLDriverError(4003, "Bad Request of Importing Data!");
    public static final SQLDriverError ERROR_4004 = new SQLDriverError(4004, "Bad Request of URL!");
    public static final SQLDriverError ERROR_4005 = new SQLDriverError(4005, "Bad Request of User or Password!");
    public static final SQLDriverError ERROR_4006 = new SQLDriverError(4006, "Bad Request of Setting Variable!");
    public static final SQLDriverError ERROR_4007 = new SQLDriverError(4007, "Bad Request of Parsing Result!");

}
