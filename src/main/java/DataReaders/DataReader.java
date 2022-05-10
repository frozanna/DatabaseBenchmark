package DataReaders;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

import java.util.List;

public interface DataReader {
    SocialNetworkData getData(String path);
}
