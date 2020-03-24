//package chaos;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Scanner;
//public class Main {
//    public static void main(String[] args) {
//        Scanner in = new Scanner(System.in);
//        List<Integer> list = new ArrayList();
//        int i = 0;
//        while (in.hasNextInt()) {
//            int a = in.next();
//        }
//        Stir
//        System.out.println("aa");
//        int count = 0;
//        while(removeS(list)){
//            count ++;
//        }
//        while(removeDoubleS(list)){
//            count++;
//        }
//        count = count + removeleft(list);
//        System.out.println(count);
//    }
//
//    public static boolean removeS(List<Integer> list){
//        int i = 0;
//        int before = 0;
//        List<Integer> s = new ArrayList();
//        while(i < list.size()){
//            if(list.get(i) > 0 && before == i){
//                list.add(i + 1);
//                before = i + 1;
//            }else{
//                list.clear();
//                list.add(i + 1);
//                before = i + 1;
//            }
//            i++;
//        }
//        if(s.size() >= 5){
//            for(int j : s){
//                int b = list.get(j - 1);
//                list.set(j - 1, b - 1);
//            }
//            return true;
//        }else{
//            return false;
//        }
//    }
//
//    public static boolean removeDoubleS(List<Integer> list){
//        int i = 0;
//        int before = 0;
//        List<Integer> s = new ArrayList();
//        while(i < list.size()){
//            if(list.get(i) > 1 && before == i){
//                list.add(i + 1);
//                before = i + 1;
//            }else{
//                list.clear();
//                list.add(i + 1);
//                before = i + 1;
//            }
//            i++;
//        }
//        if(s.size() >= 3){
//            for(int j : s){
//                int b = list.get(j - 1);
//                list.set(j - 1, b - 2);
//            }
//            return true;
//        }else{
//            return false;
//        }
//    }
//
//    public static int removeleft(List<Integer> list){
//        int count = 0;
//        for(int i : list){
//            if(i != 0){
//                if(i%2 == 1){
//                    count = count + i / 2 + 1;
//                }
//                else{
//                    count = count + i / 2;
//                }
//            }
//        }
//        return count;
//    }
//}