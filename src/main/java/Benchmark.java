import DataReaders.CSVDataReader;
import DataReaders.DataReader;
import DataReaders.SocialNetworkData;
import DatabaseAdapters.*;
import Log.Log;
import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;
import TestReports.TestReport;
import TestReports.TestType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Benchmark {
    private final List<TestReport> testReports;

    private final int testRunCount;
    private final String pathToTestData;
    private final String pathToCRUDData;
    private static final DataReader dataReader = new CSVDataReader();
    private final List<DatabaseAdapter> adapters = new ArrayList<>();

    SocialNetworkData queryData;
    SocialNetworkData CRUDData;

    public Benchmark(String pathToTestData, String pathToCRUDData, int testRunCount) {
        this.pathToTestData = pathToTestData;
        this.pathToCRUDData = pathToCRUDData;
        this.testRunCount = testRunCount;
        this.testReports = new ArrayList<>();
    }

    private void setup() {
        CRUDData = dataReader.getData(pathToCRUDData);
        queryData = dataReader.getData(pathToTestData);

        List<DatabaseAdapter> createdAdapters = new ArrayList<>();
        createdAdapters.add(new MySQLAdapter());
        createdAdapters.add(new MongoDBAdapter());
        createdAdapters.add(new Neo4jAdapter());
        createdAdapters.add(new OrientDBAdapter());
        createdAdapters.add(new ObjectDBAdapter());

        // Setup - connecting to databases
        for (DatabaseAdapter adapter: createdAdapters) {
            boolean connected = adapter.connectToDatabase();
            if (connected) {
                adapters.add(adapter);
                Log.d("Connected to " + adapter.getDatabaseName() + " database");
            }
            else
                Log.e("Cannot connect to " + adapter.getDatabaseName() + " database");
        }
    }

    public void evaluate(){
        setup();

        evaluateCRUDTests();

        evaluateQueryTests();

        saveResults();

        closeConnectionsToDatabases();
    }

    private void evaluateQueryTests() {
//         Setup - cleaning databases
        cleanDatabases();

        // Query tests
        createGraphForQueryTests();

        Log.i("Running query tests");

        try {
            for (TestType testType : Arrays.stream(TestType.values()).filter(x -> !x.isCRUDTest()).collect(Collectors.toList())) {
                Log.i(testType.toString());
                TestReport report = TestReport.createTestReportForDatabases(testType, adapters);
                for (DatabaseAdapter adapter: adapters) {
                    for (int i = 0; i < testRunCount ; i ++) {
                        Method method = DatabaseAdapter.class.getMethod("run" + testType);
                        long time = (Long) method.invoke(adapter);
                        report.addResult(adapter, time);
                        Log.m(adapter.getDatabaseName() + ": " + time + " ms");
                    }
                }
                testReports.add(report);
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            Log.e("Cannot run method");
            e.printStackTrace();
        }
    }

    private void evaluateCRUDTests() {
        // Setup - cleaning databases
        cleanDatabases();

        // CRUD tests
        Log.i("Running CRUD test");

        TestReport insertReport = TestReport.createTestReportForDatabases(TestType.INSERT, adapters);
        TestReport updateReport = TestReport.createTestReportForDatabases(TestType.UPDATE, adapters);
        TestReport deleteReport = TestReport.createTestReportForDatabases(TestType.DELETE, adapters);

        for (DatabaseAdapter adapter: adapters) {
            for (int i = 0; i < testRunCount ; i ++) {
                long insertTime = adapter.runInsertTest(CRUDData);
                insertReport.addResult(adapter, insertTime);
                Log.i("Insert test");
                Log.m(adapter.getDatabaseName() + ": " + insertTime + " millis");

                long updateTime = adapter.runUpdateTest(CRUDData);
                updateReport.addResult(adapter, updateTime);
                Log.i("Update test");
                Log.m(adapter.getDatabaseName() + ": " + updateTime + " millis");

                long deleteTime = adapter.runDeleteTest(CRUDData);
                deleteReport.addResult(adapter, deleteTime);
                Log.i("Delete test");
                Log.m(adapter.getDatabaseName() + ": " + deleteTime + " millis");
            }

            adapter.deleteGraph();
        }


        testReports.add(insertReport);
        testReports.add(updateReport);
        testReports.add(deleteReport);

    }

    private void cleanDatabases() {
        Log.d("Cleaning databases");
        for (DatabaseAdapter adapter: adapters) {
            if(adapter.deleteGraph())
                Log.d(adapter.getDatabaseName() + " database cleaned");
            else
                Log.e("Cannot clean " + adapter.getDatabaseName() + " database");
        }
    }

    private void closeConnectionsToDatabases(){
        Log.d("Closing connection to databases");
        for (DatabaseAdapter adapter: adapters) {
            if( adapter.closeConnection())
                Log.d("Connection with database " + adapter.getDatabaseName() + " closed");
            else
                Log.e("Cannot close connection to " + adapter.getDatabaseName() + " database");
        }
    }

    private void createGraphForQueryTests() {
        Log.d("Creating graph");
        for (DatabaseAdapter adapter: adapters) {
            if(adapter.createGraph(queryData))
                Log.d("Graph in " + adapter.getDatabaseName() + " database created");
            else
                Log.e("Cannot create graph in " + adapter.getDatabaseName() + " database");
        }
    }

    private void saveResults() {
        for (TestReport report: testReports) {
            report.saveToCSVFile();
        }
    }
}
