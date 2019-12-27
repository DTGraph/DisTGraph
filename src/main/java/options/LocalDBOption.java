package options;

import java.io.Serializable;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:33
 * @description:
 * @modified By:
 * @version:
 */

public class LocalDBOption implements Serializable {

    private String dbPath;

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    public String getDbPath() {
        return dbPath;
    }
}

