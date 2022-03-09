package Objects;

import java.time.LocalDate;

public class Webpage {
    public long id;
    public String url;
    public LocalDate creationDate;

    public Webpage() {
    }

    public Webpage(long id, String url, LocalDate creationDate) {
        this.id = id;
        this.url = url;
        this.creationDate = creationDate;
    }

    @Override
    public String toString() {
        return "Webpage{" +
                "id=" + id +
                ", url='" + url + '\'' +
                ", creationDate=" + creationDate +
                '}';
    }
}
