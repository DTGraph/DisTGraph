package tool;

import DBExceptions.RangeException;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/11/25 9:56
 * @description:
 * @modified By:
 * @version:
 */

public class RangeTool {

    public static boolean inRangeOrNot(List<long[]> rangeList, long number){
        try {
            for(long[] range : rangeList){
                if(range.length != 2){
                    throw new RangeException(range.length);
                }
                if(number >= range[0] && number < range[1]){ return true; }
            }
        } catch (RangeException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void addNumberToRange(List<long[]> rangeList, long number){
        try {
            for(int i = 0; i < rangeList.size(); i++){
                long[] range = rangeList.get(i);
                if(range.length != 2){
                    throw new RangeException(range.length);
                }
                if(number >= range[0] && number < range[1]){ return; }
                else if(number == range[0] - 1){
                    if(i != 0){
                        long[] before =  rangeList.get(i-1);
                        if (before[1] == number){
                            range[0] = before[0];
                            rangeList.set(i, range);
                            rangeList.remove(i-1);
                            return;
                        }
                    }
                    range[0] = number;
                    rangeList.set(i, range);
                    return;
                }
                else if(number == range[1]){
                    if(i != rangeList.size() - 1){
                        long[] after =  rangeList.get(i+1);
                        if (after[0] == number + 1){
                            range[1] = after[1];
                            rangeList.set(i, range);
                            rangeList.remove(i+1);
                            return;
                        }
                    }
                    range[1] = number + 1;
                    rangeList.set(i, range);
                    return;
                }
                else if(i == rangeList.size() - 1){
                    long[] newRange = new long[2];
                    newRange[0] = number;
                    newRange[1] = number + 1;
                    rangeList.add(newRange);
                    return;
                }
                else{
                    long[] after = rangeList.get(i + 1);
                    if(number < after[0] - 1){
                        long[] newRange = new long[2];
                        newRange[0] = number;
                        newRange[1] = number + 1;
                        rangeList.add(i + 1, newRange);
                        return;
                    }
                }
            }
            long[] newRange = new long[2];
            newRange[0] = number;
            newRange[1] = number + 1;
            rangeList.add(newRange);
        } catch (RangeException e) {
            e.printStackTrace();
        }
    }

    public static void removeNumber(List<long[]> rangeList, long number){
        try {
            for(int i = 0; i < rangeList.size(); i++) {
                long[] range = rangeList.get(i);
                if (range.length != 2) {
                    throw new RangeException(range.length);
                }
                if(range[1] <= number)continue;
                else {
                    if(range[0] > number)throw new RangeException("number " + number + " not in range");
                    if(number == range[0]){
                        if(number == range[1] - 1){
                            rangeList.remove(i);
                            return;
                        }
                        long[] newRange = new long[2];
                        newRange[0] = number + 1;
                        newRange[1] = range[1];
                        rangeList.set(i, newRange);
                        return;
                    }
                    else if (number == range[1] - 1){
                        long[] newRange = new long[2];
                        newRange[1] = number;
                        newRange[0] = range[0];
                        rangeList.set(i, newRange);
                        return;
                    }
                    else {
                        long[] newRange1 = new long[2];
                        newRange1[0] = range[0];
                        newRange1[1] = number;
                        rangeList.set(i, newRange1);
                        long[] newRange2 = new long[2];
                        newRange2[0] = number + 1;
                        newRange2[1] = range[1];
                        if(i == rangeList.size() - 1){
                            rangeList.add(newRange2);
                        }
                        else {
                            rangeList.set(i+1, newRange2);
                        }
                        return;
                    }
                }
            }
            throw new RangeException("number " + number + " not in range");
        } catch (RangeException e) {
            e.printStackTrace();
        }
    }
}
