package chaos;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        String all = sc.nextLine();

    }

    public static void dfs(Node n){
        if(n == null || n.isVisit){
            return;
        }
        n.isVisit = true;
        List<Node> near = n.getNear();
        for(Node node : near){
            dfs(node);
        }
    }

}


class Node{
    List<Node> near = new LinkedList<>();
    boolean isVisit;

    public void addNear(Node n){
        near.add(n);
    }

    public List<Node> getNear() {
        return near;
    }
}

