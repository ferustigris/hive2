package hive2;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

@Description(
        name = "Sequence generator",
        value = "_FUNC_(<sequence name>) - Generate next ID for the sequence",
        extended = "Example:\n" +
                "  > SELECT browser_info_parser(...) FROM data d;\n" +
                "  STEPHEN KING"
)
public class generateNextKey8 extends UDF implements Watcher {
    public Text evaluate(final Text s) throws IOException, InterruptedException {
        if (s == null) {
            return null;
        }

        ZooKeeper zk = new ZooKeeper("localhost", 2181, this);
        byte[] data = {'a', 'b', 'c'};
        Stat st = new Stat();
//        try {
//            st = zk.setData("/uid/" + s, data, -1);
//        } catch (InterruptedException e) {
//            return new Text("Fail set 1" + e.toString());
//        } catch (KeeperException e) {
//            return new Text("Fail set 2" + e.toString());
//        }
//        System.out.println("State: " + st.toString());

        byte[] uid;
        try {
            uid = zk.getData("/uid", false, st);
        } catch (InterruptedException e) {
            return new Text("Fail get 1" + e.toString());
        } catch (KeeperException e) {
            return new Text("Fail get 2" + e.toString());
        } finally {
            zk.close();
        }
        System.out.println("State: " + st.toString());

        return new Text("1" + s.toString().toUpperCase() + "_" + new String(uid));
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Event! " + watchedEvent.toString());
    }
}
