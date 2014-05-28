
import static org.junit.Assert.*;
import com.temafon.bufferizator.PersistentQueue;
import java.io.IOException;
import org.junit.Test;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author A.Dmitriev
 */
public class TestPersistentQueue {
    
    @Test
    public void testCount() throws IOException {
        try (PersistentQueue<String> queue = new PersistentQueue<>("/tmp", "test-queue")) {
            queue.clear();
            int elementCount = 5000;
            for (int i = 0; i < elementCount; i++) {
                queue.push(String.valueOf(i));
            }
            assertEquals("failure - wrong queue size", elementCount, queue.size());
        }
    }
    
    @Test
    public void testDeleting() throws IOException, ClassNotFoundException {
        try (PersistentQueue<String> queue = new PersistentQueue<>("/tmp", "test-queue")) {
            queue.clear();
            int elementCount = 5000;
            for (int i = 0; i < elementCount; i++) {
                queue.push(String.valueOf(i));
            }
            for (int i = 0; i < elementCount; i++) {
                queue.poll();
            }
            assertEquals("failure - wrong queue size", 0, queue.size());
        }
    }
    
    @Test
    public void testBatchPoll() throws IOException, ClassNotFoundException {
        try (PersistentQueue<String> queue = new PersistentQueue<>("/tmp", "test-queue")) {
            queue.clear();
            int elementCount = 5000;
            for (int i = 0; i < elementCount; i++) {
                queue.push(String.valueOf(i));
            }
            for (int i = 0; i < elementCount;) {
                i+=queue.poll(100).size();
            }
            assertEquals("failure - wrong queue size", 0, queue.size());
        }
    }
    
    @Test
    public void testBatchPollWithDeffered() throws IOException, ClassNotFoundException {
        try (PersistentQueue<String> queue = new PersistentQueue<>("/tmp", "test-queue", 500)) {
            queue.clear();
            int elementCount = 5000;
            for (int i = 0; i < elementCount; i++) {
                queue.push(String.valueOf(i));
            }
            for (int i = 0; i < elementCount;) {
                i+=queue.poll(100).size();
            }
            assertEquals("failure - wrong queue size", 0, queue.size());
        }
    }
    
}