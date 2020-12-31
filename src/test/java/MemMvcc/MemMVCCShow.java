package MemMvcc;

import DBExceptions.ObjectExistException;
import LocalDBMachine.MemMVCC;
import org.junit.Test;
import tool.MemMvcc.DTGSortedList;
import tool.MemMvcc.MVCCObject;
import tool.MemMvcc.TempSortedList;
import tool.MemMvcc.TimeMVCCObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemMVCCShow {

    @Test
    public void findTest(){
        Map<String, DTGSortedList> map = getTopoMemMVCC();
        DTGSortedList list = map.get("name");
        MVCCObject object = list.findData(7);
        System.out.println("获取值：" + object.getValue());
    }

    private Map<String, DTGSortedList> getTopoMemMVCC(){
        Map<String, DTGSortedList> map = new HashMap<>();
        DTGSortedList list1;

        list1 = new DTGSortedList();
        MVCCObject o = new MVCCObject(3, "version-3");
        list1.insert(o);
        MVCCObject o1 = new MVCCObject(4, "version-4");
        list1.insert(o1);
        MVCCObject o2 = new MVCCObject(5, "version-5");
        list1.insert(o2);
        MVCCObject o3 = new MVCCObject(6, "version-6");
        list1.insert(o3);
        MVCCObject o6 = new MVCCObject(8, "version-8");
        list1.insert(o6);

        list1.commitObject(3, 4);
        list1.commitObject(4, 6);
        list1.commitObject(5, 5);
        list1.commitObject(6, 8);
        list1.commitObject(8, 9);

        System.out.println("按开始版本号排序：");
        list1.printList();
        System.out.println("按结束版本号排序：");
        list1.printCommitList();

        map.put("name", list1);
        return map;
    }


}
