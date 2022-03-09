package DatabaseAdapters.ObjectDBEntities;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

@Entity(name = "Person")
@Table(indexes = @Index(columnList = "id", unique = true))
public class PersonEntity {
    @Id
    @Basic(optional = false)
    long id;

    @Basic(optional = false)
    String name;
    @Basic(optional = false)
    String surname;
    @Basic(optional = false)
    boolean sex;
    @Basic(optional = false)
    int age;

    Set<PersonEntity> friends = new HashSet<>();
    Set<WebpageEntity> likes = new HashSet<>();

    public PersonEntity(long id, String name, String surname, boolean sex, int age) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.sex = sex;
        this.age = age;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public boolean isSex() {
        return sex;
    }

    public void setSex(boolean sex) {
        this.sex = sex;
    }

    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE})
    @JoinTable(name = "Friends",
            joinColumns = @JoinColumn(name = "person1ID", referencedColumnName="id"),
            inverseJoinColumns = @JoinColumn(name = "person2ID", referencedColumnName="id"))
    public Set<PersonEntity> getFriends() {
        return friends;
    }

    public void setFriends(Set<PersonEntity> friends) {
        this.friends = friends;
    }

    @ManyToMany(cascade = {CascadeType.PERSIST, CascadeType.MERGE})
    @JoinTable(name = "Likes",
            joinColumns = @JoinColumn(name = "webpageID", referencedColumnName="id"),
            inverseJoinColumns = @JoinColumn(name = "personID", referencedColumnName="id"))
    public Set<WebpageEntity> getLikes() {
        return likes;
    }

    public void setLikes(Set<WebpageEntity> likes) {
        this.likes = likes;
    }
    
}
