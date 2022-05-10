package Data;

import java.util.List;

public class SocialNetworkData {
    public static String peopleFileName = "people.csv";
    public static String webpagesFileName = "webpages.csv";
    public static String friendsFileName = "friends.csv";
    public static String likesFileName = "likes.csv";

    private List<Person> people;
    private List<FriendEdge> friends;
    private List<Webpage> webpages;
    private List<LikeEdge> likes;

    public SocialNetworkData() {
    }

    public List<Person> getPeople() {
        return people;
    }

    public void setPeople(List<Person> people) {
        this.people = people;
    }

    public List<FriendEdge> getFriends() {
        return friends;
    }

    public void setFriends(List<FriendEdge> friends) {
        this.friends = friends;
    }

    public List<Webpage> getWebpages() {
        return webpages;
    }

    public void setWebpages(List<Webpage> webpages) {
        this.webpages = webpages;
    }

    public List<LikeEdge> getLikes() {
        return likes;
    }

    public void setLikes(List<LikeEdge> likes) {
        this.likes = likes;
    }
}
