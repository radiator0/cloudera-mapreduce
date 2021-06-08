import org.junit.Before;
import org.junit.Test;

public class PerformanceTest {

    @Before
    public void setUp() {
        System.setProperty("hadoop.home.dir", "/var/ufc/test");
    }

    @Test
    public void runPerformanceTests() throws Exception {
        Stage1Driver.main(new String[]{"dir1", "dir2", "dir3"});
    }
}
