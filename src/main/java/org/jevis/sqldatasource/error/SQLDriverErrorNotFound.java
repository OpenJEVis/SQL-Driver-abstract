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
public interface SQLDriverErrorNotFound {

    public static final SQLDriverError ERROR_404 = new SQLDriverError(404, "Not Found!");
    public static final SQLDriverError ERROR_4040 = new SQLDriverError(4040, "SQL Server Not Found!");
    public static final SQLDriverError ERROR_4041 = new SQLDriverError(4041, "SQL Channel Directory Not Found!");
    public static final SQLDriverError ERROR_4042 = new SQLDriverError(4042, "SQL Channel Not Found!");
    public static final SQLDriverError ERROR_4043 = new SQLDriverError(4043, "SQL Variable Directory Not Found!");
    public static final SQLDriverError ERROR_4044 = new SQLDriverError(4044, "SQL Variable Not Found!");
    public static final SQLDriverError ERROR_4045 = new SQLDriverError(4045, "SQL Data Point Directory Not Found!");
    public static final SQLDriverError ERROR_4046 = new SQLDriverError(4046, "SQL Data Point Not Found!");
}
