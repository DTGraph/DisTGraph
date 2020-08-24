//package chaos;
//
//import tool.MemMvcc.DTGSortedList;
//import org.jetbrains.annotations.NotNull;
//import tool.MemMvcc.MVCCObject;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class Main {
//    public static void main(String[] args) throws Exception {
//        Map<String, DTGSortedList> map = new HashMap<>();
//        DTGSortedList list1 = map.get("s1");
//        if(list1 == null){
//            list1 = new DTGSortedList();
//            MVCCObject o = new MVCCObject(1, "aaa");
//            list1.insert(o);
//        }else {
//            MVCCObject o = new MVCCObject(5, "aaa");
//            list1.insert(o);
//        }
//        MVCCObject o1 = new MVCCObject(2, "aaa");
//        list1.insert(o1);
//        MVCCObject o2 = new MVCCObject(3, "aaa");
//        list1.insert(o2);
//        MVCCObject o3 = new MVCCObject(0, "aaa");
//        list1.insert(o3);
//        MVCCObject o6 = new MVCCObject(6, "aaa");
//        list1.insert(o6);
//        list1.printList();
//        MVCCObject object = list1.find(4);
//        System.out.println(object.getVersion());
//    }
//
//
//}
//
