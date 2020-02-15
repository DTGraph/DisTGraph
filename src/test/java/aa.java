import org.junit.Test;

class aa{
    public boolean Find(int target, int [][] array) {
        int i = array[0].length - 1;
        int k = array.length/i;
        for(int j = 0; j < k && i >= 0; ){
            if(target == array[j][i]){
                return true;
            }
            if(array[j][i] < target){
                j++;
            }else{
                i--;
            }
        }
        return false;
    }
}

class bb{
    int a = 0;
}
