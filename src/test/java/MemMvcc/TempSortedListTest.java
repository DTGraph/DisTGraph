package MemMvcc;

import org.junit.Test;
import tool.MemMvcc.TempSortedList;
import tool.MemMvcc.TimeMVCCObject;

import java.util.List;

public class TempSortedListTest {

    @Test
    public void insertTest(){
        TempSortedList list = new TempSortedList();
        list.insert(1, "a",4, 10);
        list.insert(2, "b",4, 10);
        list.insert(3, "c",0, 15);
        list.insert(4, "c",4, 11);
        list.insert(5, "c",3, 11);
        list.printList();
    }

    @Test
    public void commitTest(){
        TempSortedList list = new TempSortedList();
        list.insert(1, "a",4, 10);
        list.insert(2, "b",4, 10);
        list.insert(3, "c",0, 15);
        list.insert(4, "c",4, 11);
        list.insert(5, "c",3, 11);
        list.printList();

        List<TimeMVCCObject> res =  list.commitObject(4, 5, 2, 11);
        for(TimeMVCCObject o : res){
            System.out.println(o.getStartTime() + " " + o.getEndTime());
        }
    }

    @Test
    public void clearTest(){
        TempSortedList list = new TempSortedList();
        list.insert(1, "a",4, 10);
        list.insert(2, "b",4, 10);
        list.insert(3, "c",0, 15);
        list.insert(4, "c",4, 11);
        list.insert(5, "c",3, 11);
        list.printList();

        System.out.println("-------------");
        list.clear(3);
        list.printList();
    }
}
