import com.epam.sid.idstore.IdZooStore;
import junit.framework.TestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IdZooStoreTest extends TestCase {

    private IdZooStore store;
    private ZooKeeper zoo;

    @Before
    public void setUp() {
        zoo = mock(ZooKeeper.class);
        store = new IdZooStore(zoo, 3);
    }

    @Test
    public void testClose() throws InterruptedException {
        store.close();
        verify(zoo).close();
    }

    @Test
    public void testClose2() throws InterruptedException, KeeperException {
        String node = "bla";
        Stat st = new Stat();
        when(zoo.getData(anyString(), anyBoolean(), any(Stat.class))).thenReturn(String.valueOf(3L).getBytes());

        assertEquals(4L, store.getNextId(node));

        verify(zoo).getData("/uid/" + node, false, st);
        verify(zoo).setData("/uid/" + node, String.valueOf(4L).getBytes(), 0);
    }
}
