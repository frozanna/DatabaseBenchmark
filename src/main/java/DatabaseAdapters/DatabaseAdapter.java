package DatabaseAdapters;

import Data.SocialNetworkData;
import Data.FriendEdge;
import Data.LikeEdge;
import Data.Person;
import Data.Webpage;

import java.util.List;

public interface DatabaseAdapter {
    String getDatabaseName();
    
    boolean connectToDatabase();
    boolean closeConnection();

    boolean createGraph(SocialNetworkData data);
    boolean deleteGraph();

    // TESTS
    long runInsertTest(SocialNetworkData data);
    long runBatchInsertTest(List<Person> people, List<FriendEdge> friends,
                       List<Webpage> webpages, List<LikeEdge> likes);
    long runUpdateTest(SocialNetworkData data);
    long runDeleteTest(SocialNetworkData data);

    long runSelectByIntegerTest();
    long runSelectEdgesWithVertexParametersTest();
    long runSelectByStringWithLikeTest();
    long runSelectByMultipleParametersTest();

    long runCountNeighboursTest();
    long runGroupByTest();

    long runGetNeighboursTest();
    long runGetVerticesWithoutEdgesTest();
    long runGetCommonNeighboursTest();
    long runPathExistenceTest();
}
