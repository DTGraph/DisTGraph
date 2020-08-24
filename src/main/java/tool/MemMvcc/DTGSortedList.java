package tool.MemMvcc;

import LocalDBMachine.MemMVCC;

import java.util.LinkedList;

public class DTGSortedList {
    private class Data{
        private MVCCObject obj;
        private Data next = null;

        Data(MVCCObject obj){
            this.obj = obj;
        }
    }

    private Data first = null;
    private Data commitFirst = null;

    public void insert(MVCCObject obj) {
        Data data = new Data(obj);
        Data cur = first;
        Data pre = null;

        while(cur != null && cur.obj.compareTo(obj) > 0){
            pre = cur;
            cur = cur.next;
        }
        if(pre == null){
            if(first != null){
                first.obj.setMaxVerion(false);
            }
            first = data;
            first.obj.setMaxVerion(true);
        }
        else{
            pre.next = data;
        }
        data.next = cur;
    }

    public MVCCObject commitObject(long startVersion, long endVersion) {
        MVCCObject obj = find(startVersion);
        if(obj == null) return null;
        obj.setCommitVersion(endVersion);

        Data data = new Data(obj);
        Data cur = commitFirst;
        Data pre = null;

        while(cur != null && cur.obj.commitCompareTo(obj) > 0){
            pre = cur;
            cur = cur.next;
        }
        if(pre == null){
            if(commitFirst != null){
                commitFirst.obj.setMaxVerion(false);
            }
            commitFirst = data;
            commitFirst.obj.setMaxVerion(true);
        }
        else{
            pre.next = data;
        }
        data.next = cur;
        return obj;
    }

    public Object deleteFirst() throws Exception{
        if(first == null)
            throw new Exception("empty!");
        Data temp = first;
        first = first.next;
        return temp.obj;
    }

    public void display(){
        if(first == null)
            System.out.println("empty");
        System.out.print("first -> last : ");
        Data cur = first;
        while(cur != null){
            System.out.print(cur.obj.toString() + " -> ");
            cur = cur.next;
        }
        System.out.print("\n");
    }

    public MVCCObject getFirst(){
        return first.obj;
    }

    private MVCCObject findNear(long version){
        if(first == null)
            return null;
        Data node = first;
        while(node != null && node.obj.getVersion() > version){
            node = node.next;
        }
        if(node == null){
            return null;
        }
        return node.obj;
    }

    public MVCCObject find(long version){
        if(first == null)
            return null;
        Data node = first;
        while(node != null && node.obj.getVersion() != version){
            node = node.next;
        }
        if(node == null){
            return null;
        }
        return node.obj;
    }

    public MVCCObject findData(long version){
        if(first == null)
            return null;
        Data node = commitFirst;
        MVCCObject nearestObj = null;
        while(node != null && node.obj.getCommitVersion() > version){
            node = node.next;
        }
        while(node != null) {
            if (nearestObj == null || nearestObj.getVersion() < node.obj.getVersion()) {
                nearestObj = node.obj;
            }
            node = node.next;
        }
        return nearestObj;
    }

    public boolean clearAfter(long version){
        Data pre = null;
        Data node = first;
        while(node != null && node.obj.getVersion() > version){
            pre = node;
            node = node.next;
        }
        if(pre == null){
            first = null;
        }else{
            pre.next = null;
        }

        pre = null;
        node = commitFirst;
        while(node != null){
            if(node.obj.getVersion() < version){
                if(pre != null){
                    pre.next = node.next;
                }else{
                    commitFirst = commitFirst.next;
                }
                node = node.next;
            }else{
                pre = node;
                node = node.next;
            }
        }

        //return this list is null or not
        return first == null;
    }

    public void printList(){
        Data node = first;
        while(node != null){
            System.out.print(node.obj.getVersion() + ", ");
            node = node.next;
        }
        System.out.println();
    }

    public void printCommitList(){
        Data node = commitFirst;
        while(node != null){
            System.out.print(node.obj.getCommitVersion() + ", ");
            node = node.next;
        }
        System.out.println();
    }
}