public class Entry {
    public static void main(String args[]){
        ToolGuy toolGuy = new ToolGuy();
//        toolGuy.getNamespace(2,1);
//        toolGuy.getNamespace(2,309);
        for (int i=1;i<=311;i++){
            toolGuy.typeID=-1;
            toolGuy.getNamespace(2,i); //12Huo13
            System.out.println("dataset "+i+" end!");
        }

    }
}
