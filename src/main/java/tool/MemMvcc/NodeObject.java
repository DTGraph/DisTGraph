package tool.MemMvcc;

import org.neo4j.graphdb.Node;

import java.util.Map;

import static config.MainType.NODETYPE;

public class NodeObject extends GraphObject {

    public NodeObject(long id, long version) {
        super(id, version);
        setType(NODETYPE);
    }

    @Override
    public Node getRealObjectInDB() {
        return (Node) realObjectInDB;
    }

    @Override
    public GraphObject copy() {
        NodeObject newobj = new NodeObject(getId(), -1);
        newobj.setType(getType());
        for(Map.Entry entry : properties.entrySet()){
            this.properties.put((String)entry.getKey(), entry.getValue());
        }
        for(Map.Entry entry : needUpdate.entrySet()){
            this.needUpdate.put((String)entry.getKey(), entry.getValue());
        }
        return newobj;
    }
}
