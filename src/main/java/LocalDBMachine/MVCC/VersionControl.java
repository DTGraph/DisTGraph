package LocalDBMachine.MVCC;

import com.alipay.sofa.jraft.util.Bits;

import java.util.ArrayList;
import java.util.List;

public class VersionControl {

    public List<Integer> temporalVersionList = new ArrayList<>();

    public VersionControl(int maxVersion){
        this.temporalVersionList.add(-maxVersion);
    }

    public synchronized int getTemporalVersion(){
        int oldVersion = temporalVersionList.get(temporalVersionList.size() - 1);
        int newVersion = oldVersion - 1;
        temporalVersionList.add(newVersion);
        return newVersion;
    }

    public void removeTemporalVersion(int version){
        temporalVersionList.remove(version);
    }

    public byte[] addVersionToObject(int version, long id){
        byte[] newId = new byte[12];
        Bits.putLong(newId,0, id);
        Bits.putInt(newId, 8, version);
        return newId;
    }
}
