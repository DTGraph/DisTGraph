package tool.MemMvcc;

import java.util.LinkedList;
import java.util.List;

public class TempSortedList{
    private class Data{
        private DTGSortedList obj;
        private long time;
        private Data next = null;

        Data(DTGSortedList obj, long time){
            this.obj = obj;
            this.time = time;
        }

        public long getTime() {
            return time;
        }
    }

    private Data first = null;

    public void insert(long version, Object value, long startTime, long endTime) {
        Data cur = first;
        Data pre = null;

        while(cur != null && cur.getTime() > endTime){
            pre = cur;
            cur = cur.next;
        }

        if(cur == null){
            DTGSortedList list = new DTGSortedList();
            MVCCObject o = new MVCCObject(version, value);
            list.insert(o);
            Data data = new Data(list, endTime);
            first = data;
            pre = first;
        }else{
            if(cur.getTime() == endTime){
                MVCCObject o = new MVCCObject(version, value);
                cur.obj.insert(o);
                pre = cur;
                cur = cur.next;
            }else{
                DTGSortedList list = new DTGSortedList();
                MVCCObject o = new MVCCObject(version, value);
                list.insert(o);
                Data data = new Data(list, endTime);
                if(pre == null){
                    first = data;
                }
                else{
                    pre.next = data;
                }
                data.next = cur;
                pre = data;
            }
        }

        while(cur != null && cur.getTime() > startTime){
            MVCCObject o = new MVCCObject(version, value);
            cur.obj.insert(o);
            pre = cur;
            cur = cur.next;
        }

        if(cur == null){
            addTail(pre, version, value, startTime);
        }else {
            if(cur.getTime() == startTime){
                MVCCObject o = new MVCCObject(version, value);
                cur.obj.insert(o);
            }else{
                DTGSortedList list = new DTGSortedList();
                MVCCObject o = new MVCCObject(version, value);
                list.insert(o);
                Data data = new Data(list, startTime);
                if(pre == null){
                    first = data;
                }
                else{
                    pre.next = data;
                }
                data.next = cur;
            }
        }
    }

    private void addTail(Data cur, long version, Object value, long time){
        DTGSortedList list = new DTGSortedList();
        MVCCObject o = new MVCCObject(version, value);
        list.insert(o);
        Data data = new Data(list, time);
        cur.next = data;
    }

    public Object get(long version, long time) {
        Data cur = first;
        Data pre = null;

        while(cur != null && cur.getTime() > time){
            pre = cur;
            cur = cur.next;
        }

        if(cur != null){
            return cur.obj.findData(version).getValue();
        }else{
            return null;
        }
    }

    public List<TimeMVCCObject> commitObject(long startVersion, long endVersion, long startTime, long endTime) {
        Data cur = first;
        Data pre = null;

        List<TimeMVCCObject> list = new LinkedList<>();

        while(cur != null && cur.getTime() > endTime){
            pre = cur;
            cur = cur.next;
        }
        long time1 = endTime, time2 = endTime;
        MVCCObject o = null;
        if(cur != null){
            o = cur.obj.commitObject(startVersion, endVersion);
            time1 = cur.time;
            pre = cur;
            cur = cur.next;
        }
        while(cur != null && cur.getTime() >= startTime){
            time2 = cur.time;
            if(o != null){
                TimeMVCCObject to = new TimeMVCCObject(o.getVersion(), o.getValue(), time2, time1);
                to.setMaxVerion(o.isMaxVerion());
                list.add(0, to);
            }
            time1 = cur.time;
            o = cur.obj.commitObject(startVersion, endVersion);
            pre = cur;
            cur = cur.next;
        }
        if(o != null){
            if(cur != null){
                time2 = cur.time;
            }
            TimeMVCCObject to = new TimeMVCCObject(o.getVersion(), o.getValue(), time2, time1);
            to.setMaxVerion(o.isMaxVerion());
            list.add(0, to);
        }
        return list;
    }

    public void clear(long version) {
        Data cur = first;
        Data pre = null;
        while(cur != null){
            if(cur.obj.clearAfter(version)){
                if(pre != null){
                    pre.next = cur.next;
                }else{
                    first = cur.next;
                }
                cur = cur.next;
            }else{
                if(cur == first){
                    pre = first;
                }else{
                    pre = pre.next;
                }
                cur = cur.next;
            }
        }
    }

    public void printList(){
        Data cur = first;
        while(cur != null){
            System.out.print(cur.time + ": ");
            cur.obj.printList();
            cur = cur.next;
        }
    }
}
