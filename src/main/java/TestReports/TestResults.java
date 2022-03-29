package TestReports;

import java.util.ArrayList;
import java.util.List;

public class TestResults {
    private String database;
    private List<Long> results;

    public TestResults(String database) {
        this.database = database;
        results = new ArrayList<>();
    }

    public String getDatabase() {
        return database;
    }

    public List<Long> getResults() {
        return results;
    }

    public void addResult(long time) {
        results.add(time);
    }
}
