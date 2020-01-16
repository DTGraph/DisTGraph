package options;

import UserClient.DTGSaveStore;

/**
 * @author :jinkai
 * @date :Created in 2020/1/13 15:47
 * @description：
 * @modified By:
 * @version:
 */

public class DTGMetricsRawStoreOptions {
    private String       url;
    private DTGSaveStore saveStore;

    public void setSaveStore(DTGSaveStore saveStore) {
        this.saveStore = saveStore;
    }

    public DTGSaveStore getSaveStore() {
        return saveStore;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
