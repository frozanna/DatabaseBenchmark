import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestReport {
    public final String insertTestName = "InsertTest";
    public final String batchInsertTestName = "BatchInsertTest";
    public final String updateTestName = "UpdateTest";
    public final String deleteTestName = "DeleteTest";
    public final String selectByIntParTestName = "SelectByIntegerParameterTest";
    public final String selectEdgesWithVertexParTestName = "SelectEdgesWithVertexParameters";
    public final String selectByStringWithLikeTestName = "SelectByStringWithLike";

    public final List<String> databasesNames = new ArrayList<>();
    public final List<Map<String, Long>> insertTestResults = new ArrayList<>();
    public final List<Map<String, Long>> batchInsertTestResults = new ArrayList<>();
    public final List<Map<String, Long>> updateTestResults = new ArrayList<>();
    public final List<Map<String, Long>> deleteTestResults = new ArrayList<>();
    public final List<Map<String, Long>> selectByIntParTestResults = new ArrayList<>();
    public final List<Map<String, Long>> selectEdgesWithVertexParTestResults = new ArrayList<>();
    public final List<Map<String, Long>> selectByStringWithLikeTestResult = new ArrayList<>();

    public TestReport(int testRunsNumber){
        for (int i = 0; i < testRunsNumber; i ++){
            insertTestResults.add(new HashMap<>());
            batchInsertTestResults.add(new HashMap<>());
            updateTestResults.add(new HashMap<>());
            deleteTestResults.add(new HashMap<>());
            selectByIntParTestResults.add(new HashMap<>());
            selectEdgesWithVertexParTestResults.add(new HashMap<>());
            selectByStringWithLikeTestResult.add(new HashMap<>());
        }
    }
}
