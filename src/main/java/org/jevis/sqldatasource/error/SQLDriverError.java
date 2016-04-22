/**
 * Copyright (C) 2013 - 2016 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of SQLDriverAbstract.
 *
 * SQLDriverAbstract is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * SQLDriverAbstract is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * SQLDriverAbstract. If not, see <http://www.gnu.org/licenses/>.
 *
 * SQLDriverAbstract is part of the OpenJEVis project, further project information are
 * published at <http://www.OpenJEVis.org/>.
 */

package org.jevis.sqldatasource.error;

/**
 *
 * @author Jingxuan Man
 */

public class SQLDriverError {

    private int status;
    private String message;

    SQLDriverError(int status, String message) {
        this.status = status;
        this.message = message;
    }
    
    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public void setStatus(int code) {
        this.status = code;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
