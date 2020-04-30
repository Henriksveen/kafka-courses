import java.sql.Date;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;

public class TableQueue {

    private boolean done = false;
    private final Queue<Table> queue = new LinkedList<>();

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public Table poll() {
        return queue.poll();
    }

    public void add(Table table) {
        queue.add(table);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size(){
        return queue.size();
    }

    public static class Table {

        private final Date startTime;
        private final Date endTime;
        private final String tableName;
        private Date producerStartTime;
        private Date producerEndTime;
        private long totalTime;

        public Table(Date startTime, Date endTime, String tableName) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.tableName = tableName;
        }

        public void setProducerStartTime(Date producerStartTime) {
            this.producerStartTime = producerStartTime;
        }

        public Date getStartTime() {
            return startTime;
        }

        public Date getEndTime() {
            return endTime;
        }

        public String getTableName() {
            return tableName;
        }

        public Date getProducerStartTime() {
            return producerStartTime;
        }

        public Date getProducerEndTime() {
            return producerEndTime;
        }

        public long getTotalTime() {
            return totalTime;
        }

        public void end() {
            this.producerEndTime = new Date(Instant.now().toEpochMilli());
            this.totalTime = this.producerEndTime.getTime() - this.producerStartTime.getTime();
        }

        @Override
        public String toString() {
            return "Table{" +
                    "startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", tableName='" + tableName + '\'' +
                    ", producerStartTime=" + producerStartTime +
                    ", producerEndTime=" + producerEndTime +
                    ", totalTime=" + totalTime +
                    '}';
        }
    }
}
