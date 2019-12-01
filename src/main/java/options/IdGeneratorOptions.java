package options;

/**
 * @author :jinkai
 * @date :Created in 2019/10/20 15:23
 * @description:
 * @modified By:
 * @version:
 */

public class IdGeneratorOptions {

    private String idGeneratorPath;
    private int BatchSize;//the number of id that client request from pd
    private int grabSize;

    public void setIdGeneratorPath(String idGeneratorPath) {
        this.idGeneratorPath = idGeneratorPath;
    }

    public String getIdGeneratorPath() {
        return idGeneratorPath;
    }

    public int getBatchSize() {
        return BatchSize;
    }

    public void setBatchSize(int batchSize) {
        BatchSize = batchSize;
    }

    public int getGrabSize() {
        return grabSize;
    }

    public void setGrabSize(int grabSize) {
        this.grabSize = grabSize;
    }
}
