package chaos;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int m = sc.nextInt();
        int s = sc.nextInt();
        int t = sc.nextInt();
        int[][] map = new int[n][n];
        for(int i = 0; i < m; i++){
            int n1 =sc.nextInt();
            int n2 = sc.nextInt();
            int val = sc.nextInt();
            map[n1-1][n2-1] = val;
        }
        dfs(map, 0, 0, n, 0);
        System.out.println(minsum);
    }

    public static int max = Integer.MAX_VALUE;
    public static int minsum = Integer.MAX_VALUE;

    public static void dfs(int[][] map, int now,int sum, int n, int thiswaymax){
        if(now == n-1){
            if(thiswaymax < max){
                max = thiswaymax;
                minsum = sum;
            }
            return;
        }
        for(int i = now + 1; i < n; i++){
            if(map[now][i] != 0 && map[now][i] <= max){
                if(map[now][i] > thiswaymax){
                    thiswaymax = map[now][i];
                }
                dfs(map, i, sum + map[now][i], n, thiswaymax);
            }
        }
    }

}

