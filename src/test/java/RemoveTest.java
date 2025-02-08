import com.skylarkarms.concur.CopyOnWriteArray;

public class RemoveTest {
    private static final String TAG = "RemoveTest";
    public static void main(String[] args) {
        class Parent {

        }
        CopyOnWriteArray<Parent> copyOnWriteArray = new CopyOnWriteArray<>(Parent.class);
        record Son(){}
        Son aSon = new Son();
        copyOnWriteArray.add(
                new Parent() {
                    Parent getThis() {
                        return this;
                    }
                    {
                        System.err.println("adding..." + getThis().hashCode());
                    }
                    @Override
                    public boolean equals(Object obj) {
                        boolean same = (obj == aSon) || (obj != null && obj.equals(aSon));
                        System.err.println("same = " + same);
                        return same;
                    }
                }
        );

        System.err.println("" +
                "\n size = " + copyOnWriteArray.size() +
                "\n removing..." + aSon.hashCode());
        CopyOnWriteArray.Search<Parent> parentSearch;
        if ((parentSearch = copyOnWriteArray.nonContRemove(aSon)) == null || !parentSearch.found()) throw new IllegalStateException("This should remove...");

        System.err.println(copyOnWriteArray.size());
    }
}
