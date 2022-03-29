package DatabaseAdapters;

import DatabaseAdapters.ObjectDBEntities.PersonEntity;
import DatabaseAdapters.ObjectDBEntities.WebpageEntity;
import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;


import javax.persistence.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static DatabaseAdapters.AdapterUtils.localdateToDate;

public class ObjectDBAdapter implements DatabaseAdapter {
    private EntityManagerFactory emf;
    private EntityManager em;

    @Override
    public String getDatabaseName() {
        return "ObjectDB";
    }

    @Override
    public boolean connectToDatabase() {
        try{
            emf = Persistence.createEntityManagerFactory("objectdb://localhost:6136/socialNetwork.odb;user=admin;password=admin");
            em = emf.createEntityManager();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean closeConnection() {
        try {
            em.close();
            emf.close();
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean createGraph(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        try {
            batchInsertData(people, friends, webpages, likes, 1000);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteGraph() {
        try {
            em.getTransaction().begin();
            em.createQuery("DELETE FROM Object").executeUpdate();
            em.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public long runInsertTest(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        long start = System.currentTimeMillis();

        try{
            insertData(people, friends, webpages, likes);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runBatchInsertTest(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        long start = System.currentTimeMillis();

        try{
            batchInsertData(people, friends, webpages, likes, 100);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runUpdateTest(List<Person> people, List<Webpage> webpages) {
        long start = System.currentTimeMillis();
        try{
            for (Person p : people) {
                PersonEntity person = em.find(PersonEntity.class, p.id);
                em.getTransaction().begin();
                person.setAge(25);
                em.getTransaction().commit();
            }

            for (Webpage w : webpages) {
                WebpageEntity webpage = em.find(WebpageEntity.class, w.id);
                em.getTransaction().begin();
                webpage.setUrl("www.new.pl");
                em.getTransaction().commit();
            }

            for (Person p : people) {
                PersonEntity person = em.find(PersonEntity.class, p.id);
                em.getTransaction().begin();
                person.setAge(30);
                em.getTransaction().commit();
            }

            for (Webpage w : webpages) {
                WebpageEntity webpage = em.find(WebpageEntity.class, w.id);
                em.getTransaction().begin();
                webpage.setUrl("www.new2.pl");
                em.getTransaction().commit();
            }

            for (Person p : people) {
                PersonEntity person = em.find(PersonEntity.class, p.id);
                em.getTransaction().begin();
                person.setAge(35);
                em.getTransaction().commit();
            }

            for (Webpage w : webpages) {
                WebpageEntity webpage = em.find(WebpageEntity.class, w.id);
                em.getTransaction().begin();
                webpage.setUrl("www.new3.pl");
                em.getTransaction().commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runDeleteTest(List<Person> people, List<Webpage> webpages) {
        long start = System.currentTimeMillis();
        try{
            for (Person p : people) {
                PersonEntity person = em.find(PersonEntity.class, p.id);
                em.getTransaction().begin();
                em.remove(person);
                em.getTransaction().commit();
            }

            for (Webpage w : webpages) {
                WebpageEntity webpage = em.find(WebpageEntity.class, w.id);
                em.getTransaction().begin();
                em.remove(webpage);
                em.getTransaction().commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByIntegerTest() {
        long start = System.currentTimeMillis();
        try{
            TypedQuery<PersonEntity> query = em.createQuery("SELECT p FROM Person p WHERE p.age > 30", PersonEntity.class);
            List<PersonEntity> results = query.getResultList();

//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectEdgesWithVertexParametersTest() {
        long start = System.currentTimeMillis();
        try{
            Query query = em.createQuery("SELECT p.surname, w.url FROM Person p INNER JOIN p.likes w");
            List results = query.getResultList();
//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByStringWithLikeTest() {
        long start = System.currentTimeMillis();
        try{
            TypedQuery<PersonEntity> query = em.createQuery("SELECT p FROM Person p WHERE p.name like \"KR%\"", PersonEntity.class);
            List<PersonEntity> results = query.getResultList();

//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByMultipleParametersTest() {
        long start = System.currentTimeMillis();
        try{
            TypedQuery<WebpageEntity> query = em.createQuery("SELECT w FROM Webpage w WHERE w.id > 75000 AND w.url LIKE \"%0.html\" AND w.creationDate >= ?1", WebpageEntity.class);
            query.setParameter(1, localdateToDate(LocalDate.parse("2000-01-01")));
            List<WebpageEntity> results = query.getResultList();

//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runCountNeighboursTest() {
        long start = System.currentTimeMillis();
        try{
            Query query = em.createQuery("SELECT p.id, (p.friends.size() + p.likes.size()) AS NeighboursCount FROM Person p");
            List results = query.getResultList();

//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGroupByTest() {
        long start = System.currentTimeMillis();
        try{
            Query query = em.createQuery("SELECT p.surname, AVG(p.age) AS AvgAge FROM Person p GROUP BY p.surname");
            List results = query.getResultList();
//            System.out.println(results.size());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }


    private void insertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        Map<Long, PersonEntity> personMap = new HashMap<>();
        Map<Long, WebpageEntity> webpageMap = new HashMap<>();

        for (Person p : people) {
            PersonEntity personEntity = new PersonEntity(p.id, p.name, p.surname, p.sex, p.age);
            personMap.put(p.id, personEntity);
        }

        for (Webpage w : webpages) {
            WebpageEntity webpageEntity = new WebpageEntity(w.id, w.url, localdateToDate(w.creationDate));
            webpageMap.put(w.id, webpageEntity);
            em.getTransaction().begin();
            em.persist(webpageEntity);
            em.getTransaction().commit();
        }

        for (LikeEdge l : likes) {
            PersonEntity p = personMap.get(l.personId);
            WebpageEntity w = webpageMap.get(l.webpageId);
            p.getLikes().add(w);
        }

        for(PersonEntity personEntity : personMap.values()) {
            em.getTransaction().begin();
            em.persist(personEntity);
            em.getTransaction().commit();
        }

        for (FriendEdge f : friends) {
            PersonEntity p1 = personMap.get(f.person1Id);
            PersonEntity p2 = personMap.get(f.person2Id);
            em.getTransaction().begin();
            p1.getFriends().add(p2);
            p2.getFriends().add(p1);
            em.getTransaction().commit();
        }
    }


    private void batchInsertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes, int batch) {
        Map<Long, PersonEntity> personMap = new HashMap<>();
        Map<Long, WebpageEntity> webpageMap = new HashMap<>();

        int i = 0;

        for (Person p : people) {
            PersonEntity personEntity = new PersonEntity(p.id, p.name, p.surname, p.sex, p.age);
            personMap.put(p.id, personEntity);
        }

        em.getTransaction().begin();
        for (Webpage w : webpages) {
            WebpageEntity webpageEntity = new WebpageEntity(w.id, w.url, localdateToDate(w.creationDate));
            webpageMap.put(w.id, webpageEntity);
            em.persist(webpageEntity);

            if(i == batch) {
                em.getTransaction().commit();
                em.getTransaction().begin();
                i = 0;
            } else
                i ++;
        }

        em.getTransaction().commit();

        for (LikeEdge l : likes) {
            PersonEntity p = personMap.get(l.personId);
            WebpageEntity w = webpageMap.get(l.webpageId);
            p.getLikes().add(w);
        }

        i = 0;
        em.getTransaction().begin();

        for(PersonEntity personEntity : personMap.values()) {
            em.persist(personEntity);

            if(i == batch) {
                em.getTransaction().commit();
                em.getTransaction().begin();
                i = 0;
            } else
                i ++;
        }

        em.getTransaction().commit();
        i = 0;
        em.getTransaction().begin();

        for (FriendEdge f : friends) {
            PersonEntity p1 = personMap.get(f.person1Id);
            PersonEntity p2 = personMap.get(f.person2Id);
            p1.getFriends().add(p2);
            p2.getFriends().add(p1);

            if(i == batch) {
                em.getTransaction().commit();
                em.getTransaction().begin();
                i = 0;
            } else
                i ++;
        }

        em.getTransaction().commit();
    }
}
