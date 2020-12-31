package MemMvcc;

import org.junit.Test;
import tool.MemMvcc.DTGSortedList;
import tool.MemMvcc.MVCCObject;

import java.util.HashMap;
import java.util.Map;

public class DTGSortedListTest {

    @Test
    public void insertTest() throws Exception {
        Map<String, DTGSortedList> map = new HashMap<>();
        DTGSortedList list1 = map.get("s1");
        if(list1 == null){
            list1 = new DTGSortedList();
            MVCCObject o = new MVCCObject(1, "aaa");
            list1.insert(o);
        }else {
            MVCCObject o = new MVCCObject(5, "aaa");
            list1.insert(o);
        }
        MVCCObject o1 = new MVCCObject(2, "aaa");
        list1.insert(o1);
        MVCCObject o2 = new MVCCObject(3, "aaa");
        list1.insert(o2);
        MVCCObject o3 = new MVCCObject(0, "aaa");
        list1.insert(o3);
        MVCCObject o6 = new MVCCObject(6, "aaa");
        list1.insert(o6);

        list1.commitObject(1, 3);
        list1.commitObject(2, 5);
        list1.commitObject(3, 4);
        list1.commitObject(6, 7);
        list1.commitObject(0, 3);

        list1.printList();
        list1.printCommitList();
        MVCCObject object = list1.findData(4);
        System.out.println(object.getVersion());
    }

    @Test
    public void deleteTest() throws Exception {
        Map<String, DTGSortedList> map = new HashMap<>();
        DTGSortedList list1 = map.get("s1");
        if(list1 == null){
            list1 = new DTGSortedList();
            MVCCObject o = new MVCCObject(1, "aaa");
            list1.insert(o);
        }else {
            MVCCObject o = new MVCCObject(5, "aaa");
            list1.insert(o);
        }
        MVCCObject o1 = new MVCCObject(2, "aaa");
        list1.insert(o1);
        MVCCObject o2 = new MVCCObject(3, "aaa");
        list1.insert(o2);
        MVCCObject o3 = new MVCCObject(0, "aaa");
        list1.insert(o3);
        MVCCObject o6 = new MVCCObject(6, "aaa");
        list1.insert(o6);

        list1.commitObject(1, 3);
        list1.commitObject(2, 3);
        list1.commitObject(3, 6);
        list1.commitObject(6, 7);
        list1.commitObject(0, 3);

        list1.printList();
        list1.printCommitList();
        list1.clearAfter(4);
        list1.printList();
        list1.printCommitList();
    }
}
