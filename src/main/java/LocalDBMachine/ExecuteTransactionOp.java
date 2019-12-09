package LocalDBMachine;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 18:42
 * @description:
 * @modified By:
 * @version:
 */

public class ExecuteTransactionOp {

//    public LocalTransaction getTransaction(GraphDatabaseService db, DTGOpreration op, Map<Integer, Object> resultMap) throws Throwable{
//        Map<Integer, Object> tempMap = new HashMap<>();
//        LocalTransaction transaction = new LocalTransaction(db);
//        List<EntityEntry> Entries = op.getEntityEntries();
//        for(EntityEntry entityEntry : Entries){
//            switch (entityEntry.getType()){
//                case EntityEntry.NODETYPE:{
//                    switch (entityEntry.getOperationType()){
//                        case EntityEntry.ADD:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Node node = transaction.addNode(entityEntry.getId());
//                                tempMap.put(entityEntry.getTransactionNum(), node);
//                                //resultMap.put(entityEntry.getTransactionNum(), node);
//                                break;
//                            }
//
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        case EntityEntry.GET:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Node node = transaction.getNodeById(entityEntry.getId());
//                                tempMap.put(entityEntry.getTransactionNum(), node);
////                                resultMap.put(entityEntry.getTransactionNum(), node);
//                            }
//
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                Object res = transaction.getNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart());
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            else {
//                                Object res = transaction.getNodeProperty(node, entityEntry.getKey());
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            break;
//                        }
//                        case EntityEntry.REMOVE:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null){
//                                transaction.deleteNode(node);
//                            }
//                            else if(entityEntry.isTemporalProperty()){
//                                transaction.deleteNodeTemporalProperty(node, entityEntry.getKey());
//                            }
//                            else transaction.deleteNodeProperty(node, entityEntry.getKey());
//                            break;
//                        }
//                        case EntityEntry.SET:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        default:{
//                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
//                        }
//                    }
//                    break;
//                }
//                case EntityEntry.RELATIONTYPE:{
//                    switch (entityEntry.getOperationType()){
//                        case EntityEntry.ADD:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Relationship relationship = transaction.addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther());
////                                resultMap.put(entityEntry.getTransactionNum(), relationship);
//                                tempMap.put(entityEntry.getTransactionNum(), relationship);
//                                break;
//                            }
//
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        case EntityEntry.GET:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Relationship relationship = transaction.getRelationshipById(entityEntry.getId());
////                                resultMap.put(entityEntry.getTransactionNum(), relationship);
//                                tempMap.put(entityEntry.getTransactionNum(), relationship);
//                            }
//
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                Object res = transaction.getRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart());
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            else {
//                                Object res = transaction.getRelationProperty(relationship, entityEntry.getKey());
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            break;
//                        }
//                        case EntityEntry.REMOVE:{
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null){
//                                transaction.deleteRelation(relationship);
//                            }
//                            else if(entityEntry.isTemporalProperty()){
//                                transaction.deleteRelationTemporalProperty(relationship, entityEntry.getKey());
//                            }
//                            else transaction.deleteRelationProperty(relationship, entityEntry.getKey());
//                            break;
//                        }
//                        case EntityEntry.SET:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        default:{
//                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
//                        }
//                    }
//                    break;
//                }
//                default:{
//                    throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
//                }
//            }
//        }
//        System.out.println("success transaction operation");
//        return transaction;
//    }
}
