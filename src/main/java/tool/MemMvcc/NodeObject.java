package tool.MemMvcc;

import org.neo4j.graphdb.Node;

import java.util.Map;

import static config.MainType.NODETYPE;

public class NodeObject extends GraphObject {

    public NodeObject(long id) {
        super(id);
        setType(NODETYPE);
    }

    @Override
    public Node getRealObjectInDB() {
        return (Node) realObjectInDB;
    }

    @Override
    public GraphObject copy() {
        NodeObject newobj = new NodeObject(getId());
        newobj.setType(getType());
        for(Map.Entry entry : properties.entrySet()){
            this.properties.put((String)entry.getKey(), entry.getValue());
        }
        for(Map.Entry entry : needUpdate.entrySet()){
            this.needUpdate.put((String)entry.getKey(), entry.getValue());
        }
        return null;
    }
}
