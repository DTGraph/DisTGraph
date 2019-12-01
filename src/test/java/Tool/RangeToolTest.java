package Tool;

import org.junit.Test;
import tool.RangeTool;

import java.util.ArrayList;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/11/25 13:51
 * @description:
 * @modified By:
 * @version:
 */

public class RangeToolTest {

    @Test
    public void addTest(){
        List<long[]> list = new ArrayList<>();
        RangeTool.addNumberToRange(list, 0);
        RangeTool.addNumberToRange(list, 1);
        RangeTool.addNumberToRange(list, 2);
        RangeTool.addNumberToRange(list, 3);
        RangeTool.addNumberToRange(list, 5);
        RangeTool.addNumberToRange(list, 8);
        RangeTool.addNumberToRange(list, 9);
        RangeTool.addNumberToRange(list, 2);
        RangeTool.addNumberToRange(list, 4);
        RangeTool.addNumberToRange(list, 10);
        RangeTool.addNumberToRange(list, 6);
        RangeTool.addNumberToRange(list, 7);
        RangeTool.removeNumber(list,10);
        System.out.println("**************");
        printList(list);
        RangeTool.removeNumber(list,6);
        System.out.println("**************");
        printList(list);
        RangeTool.removeNumber(list,8);
        System.out.println("**************");
        printList(list);
        RangeTool.removeNumber(list,7);
        System.out.println("**************");
        printList(list);

    }

    private void printList(List<long[]> list){
        for (long[] range : list){
            System.out.println(range[0] + " - " + range[1]);
        }
    }

}
