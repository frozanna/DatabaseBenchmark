import DataReaders.CSVDataReader;
import DataReaders.DataReader;
import DatabaseAdapters.*;
import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

import java.util.ArrayList;
import java.util.List;

public class Benchmark {
    private final TestReport testReport;

    private final int testRunsNumber;
    private final String pathToTestData;
    private final String pathToCRUDData;
    private static final DataReader dataReader  = new CSVDataReader();
    private final List<DatabaseAdapter> adapters = new ArrayList<>();

    private List<Person> people;
    private List<FriendEdge> friends;
    private List<Webpage> webpages;
    private List<LikeEdge> likes;

    private List<Person> peopleCRUD;
    private List<FriendEdge> friendsCRUD;
    private List<Webpage> webpagesCRUD;
    private List<LikeEdge> likesCRUD;

    public Benchmark(String pathToTestData, String pathToCRUDData, int testRunsNumber) {
        this.pathToTestData = pathToTestData;
        this.pathToCRUDData = pathToCRUDData;
        this.testRunsNumber = testRunsNumber;
        testReport = new TestReport(testRunsNumber);
    }

    private void setup() {
        people = dataReader.readPersons(pathToTestData + "people.csv");
        friends = dataReader.readFriends(pathToTestData + "friends.csv");
        webpages = dataReader.readWebpages(pathToTestData + "webpages.csv");
        likes = dataReader.readLikes(pathToTestData + "likes.csv");

        peopleCRUD = dataReader.readPersons(pathToCRUDData + "people.csv");
        friendsCRUD = dataReader.readFriends(pathToCRUDData + "friends.csv");
        webpagesCRUD = dataReader.readWebpages(pathToCRUDData + "webpages.csv");
        likesCRUD = dataReader.readLikes(pathToCRUDData + "likes.csv");


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
                testReport.databasesNames.add(adapter.getDatabaseName());
                adapters.add(adapter);
                Log.d("Connected to " + adapter.getDatabaseName() + " database");
            }
            else {
                Log.e("Cannot connect to " + adapter.getDatabaseName() + " database");
            }
        }
    }

    public void evaluate(){
        setup();

        int i = 0;

//        evaluateCRUDTests(i);

        evaluateQueryTests(i);

        closeConnectionsToDatabases();
    }

    private void evaluateQueryTests(int i) {

        // Setup - cleaning databases
//        cleanDatabases();
//
//        // Query tests
//        createGraphForQueryTests();
//
//        Log.i("Select by integer test");
//        for (DatabaseAdapter adapter: adapters) {
//            long time = adapter.runSelectByIntTest();
//            testReport.selectByIntTestResults.get(i).put(adapter.getDatabaseName(), time);
//            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
//        }
//
//        Log.i("Select edges by type");
//        for (DatabaseAdapter adapter: adapters) {
//            long time = adapter.runSelectEdgesWithVertexParTest();
//            testReport.selectEdgesWithVertexParTestResults.get(i).put(adapter.getDatabaseName(), time);
//            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
//        }
//
//        Log.i("Select by like string");
//        for (DatabaseAdapter adapter: adapters) {
//            long time = adapter.runSelectByStringWithLike();
//            testReport.selectByStringWithLikeTestResult.get(i).put(adapter.getDatabaseName(), time);
//            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
//        }

        Log.i("Select by multiple parameters");
        for (DatabaseAdapter adapter: adapters) {
            long time = adapter.runSelectByMultipleParTest();
            testReport.selectByMultipleParTestResult.get(i).put(adapter.getDatabaseName(), time);
            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
        }

    }

    private void evaluateCRUDTests(int i) {
        // Setup - cleaning databases
        cleanDatabases();

        // CRUD tests
        Log.i("Running CRUD test");

        Log.i("Insert test");
        for (DatabaseAdapter adapter: adapters) {
            long time = adapter.runInsertTest(peopleCRUD, friendsCRUD, webpagesCRUD, likesCRUD);
            testReport.insertTestResults.get(i).put(adapter.getDatabaseName(), time);
            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
        }

        Log.i("Update test");
        for (DatabaseAdapter adapter: adapters) {
            long time = adapter.runUpdateTest(peopleCRUD, webpagesCRUD);
            testReport.updateTestResults.get(i).put(adapter.getDatabaseName(), time);
            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
        }

        Log.i("Delete test");
        for (DatabaseAdapter adapter: adapters) {
            long time = adapter.runDeleteTest(peopleCRUD, webpagesCRUD);
            testReport.deleteTestResults.get(i).put(adapter.getDatabaseName(), time);
            System.out.println(adapter.getDatabaseName() + ": " + time + " millis");
        }
    }

    private void cleanDatabases(){
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
            if(adapter.createGraph(people, friends, webpages, likes))
                Log.d("Graph in " + adapter.getDatabaseName() + " database created");
            else
                Log.e("Cannot create graph in " + adapter.getDatabaseName() + " database");
        }
    }
}
