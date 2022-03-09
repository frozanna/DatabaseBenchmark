package DataReaders;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

import java.util.List;

public interface DataReader {
    List<Person> readPersons(String path);
    List<FriendEdge> readFriends(String path);
    List<Webpage> readWebpages(String path);
    List<LikeEdge> readLikes(String path);
}
