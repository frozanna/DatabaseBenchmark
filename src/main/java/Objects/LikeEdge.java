package Objects;

public class LikeEdge {
    public long id;
    public long personId;
    public long webpageId;

    public LikeEdge() {
    }

    public LikeEdge(long id, long personId, long webpageId) {
        this.id = id;
        this.personId = personId;
        this.webpageId = webpageId;
    }

    @Override
    public String toString() {
        return "LikeConnection{" +
                "id=" + id +
                ", personId=" + personId +
                ", webpageId=" + webpageId +
                '}';
    }
}
