package DatabaseAdapters.ObjectDBEntities;

import javax.persistence.*;
import java.sql.Date;
import java.util.HashSet;
import java.util.Set;

@Entity(name = "Webpage")
@Table(indexes = @Index(columnList = "id", unique = true))
public class WebpageEntity {
    @Id
    @Basic(optional = false)
    long id;

    @Basic(optional = false)
    String url;
    @Basic(optional = false)
    @Temporal(TemporalType.DATE)
    Date creationDate;
    Set<PersonEntity> likedBy = new HashSet<>();

    public WebpageEntity(long id, String url, Date creationDate) {
        this.id = id;
        this.url = url;
        this.creationDate = creationDate;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    @ManyToMany(mappedBy = "likes")
    public Set<PersonEntity> getLikedBy() {
        return likedBy;
    }

    public void setLikedBy(Set<PersonEntity> likedBy) {
        this.likedBy = likedBy;
    }
}
