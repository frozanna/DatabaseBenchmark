package TestReports;

import DatabaseAdapters.DatabaseAdapter;
import Log.Log;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestReport {
    private TestType testType;
    private List<TestResults> results;

    private TestReport(TestType testType) {
        this.testType = testType;
        results = new ArrayList<>();
    }

    public static TestReport createTestReportForDatabases(TestType type, List<DatabaseAdapter> databaseAdapters) {
        TestReport report = new TestReport(type);
        for (DatabaseAdapter adapter : databaseAdapters) {
            report.results.add(new TestResults(adapter.getDatabaseName()));
        }

        return report;
    }

    public void saveToCSVFile() {
        saveToCSVFile("results");
    }

    public void saveToCSVFile(String directoryPath) {
        Log.i("Saving results to file (directory: " + directoryPath + ")");

        try {
            File directory = new File(directoryPath);
            if (!directory.exists()){
                directory.mkdir();
            }
        } catch (Exception ex) {
            Log.e("Cannot get directory");
        }


        try (PrintWriter pw = new PrintWriter(Paths.get(directoryPath, testType.toString() + ".csv")
                                                                                .toAbsolutePath().toString())) {
            pw.println("database,time");

            for (TestResults result : results) {
                for (long time : result.getResults()) {
                    String line = result.getDatabase() + "," + time;
                    pw.println(line);
                }
            }
        } catch (Exception ex) {
            Log.e("Cannot save to file " + testType.toString());
            ex.printStackTrace();
        }
    }

    public void addResult(DatabaseAdapter adapter, long time) {
        TestResults testResults = results.stream()
                                  .filter(x -> x.getDatabase().equals(adapter.getDatabaseName())).findAny().orElse(null);

        if (testResults == null) {
            Log.e("Cannot find adapter in results list");
            return;
        }

        testResults.addResult(time);
    }

    public TestType getTestType() {
        return testType;
    }

    public List<TestResults> getResults() {
        return results;
    }
}
