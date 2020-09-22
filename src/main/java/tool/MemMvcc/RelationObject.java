package tool.MemMvcc;

import org.neo4j.graphdb.Relationship;

import java.util.Map;

import static config.MainType.RELATIONTYPE;

public class RelationObject extends GraphObject {

    private long startNode;
    private long endNode;

    public RelationObject(long id, long version, long startNode, long endNode) {
        super(id, version);
        this.startNode = startNode;
        this.endNode = endNode;
        setType(RELATIONTYPE);
    }

    public long getStartNode() {
        return startNode;
    }

    public long getEndNode() {
        return endNode;
    }

    @Override
    public Relationship getRealObjectInDB() {
        return (Relationship)realObjectInDB;
    }

    @Override
    public GraphObject copy() {
        RelationObject newobj = new RelationObject(getId(), -1, startNode, endNode);
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
