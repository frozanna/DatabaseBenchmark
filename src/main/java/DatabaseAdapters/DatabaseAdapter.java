package DatabaseAdapters;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

import java.util.List;

public interface DatabaseAdapter {
    String getDatabaseName();
    
    boolean connectToDatabase();
    boolean closeConnection();

    boolean createGraph(List<Person> people, List<FriendEdge> friends,
                        List<Webpage> webpages, List<LikeEdge> likes);
    boolean deleteGraph();

    // TESTS
    long runInsertTest(List<Person> people, List<FriendEdge> friends,
                       List<Webpage> webpages, List<LikeEdge> likes);
    long runBatchInsertTest(List<Person> people, List<FriendEdge> friends,
                       List<Webpage> webpages, List<LikeEdge> likes);
    long runUpdateTest(List<Person> people, List<Webpage> webpages);
    long runDeleteTest(List<Person> people, List<Webpage> webpages);
    long runSelectByIntParTest();
    long runSelectEdgesWithVertexParTest();
    long runSelectByStringWithLike();
}
