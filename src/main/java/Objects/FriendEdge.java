package Objects;

public class FriendEdge {
    public long id;
    public long person1Id;
    public long person2Id;

    public FriendEdge() {
    }

    public FriendEdge(long id, long person1Id, long person2Id) {
        this.id = id;
        this.person1Id = person1Id;
        this.person2Id = person2Id;
    }

    @Override
    public String toString() {
        return "FriendConnection{" +
                "id=" + id +
                ", person1Id=" + person1Id +
                ", person21Id=" + person2Id +
                '}';
    }
}
