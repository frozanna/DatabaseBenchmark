package DatabaseAdapters;

import Data.SocialNetworkData;
import DatabaseAdapters.ObjectDBEntities.PersonEntity;
import DatabaseAdapters.ObjectDBEntities.WebpageEntity;
import Data.FriendEdge;
import Data.LikeEdge;
import Data.Person;
import Data.Webpage;
import com.sun.org.apache.xpath.internal.operations.Bool;


import javax.persistence.*;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

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
    public boolean createGraph(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

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
    public long runInsertTest(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

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
    public long runUpdateTest(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

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
    public long runDeleteTest(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

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

    @Override
    public long runGetNeighboursTest() {
        long countPerson = countPerson();

        TypedQuery<PersonEntity> query;
        long start = System.currentTimeMillis();
        try{
            for (long i = 1; i <= countPerson; i++){

                query = em.createQuery("SELECT p FROM Person p WHERE p.id = :id", PersonEntity.class);
                PersonEntity entity = query.setParameter("id", i).getSingleResult();
                Set<PersonEntity> ps = entity.getFriends();
                Set<WebpageEntity> ws = entity.getLikes();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetVerticesWithoutEdgesTest() {
        long start = System.currentTimeMillis();
        try{
            TypedQuery<PersonEntity> queryPerson = em.createQuery("SELECT p FROM Person p WHERE p.friends.size() = 0 AND p.likes.size() = 0", PersonEntity.class);
            List<PersonEntity> resultsPeople = queryPerson.getResultList();

            List<WebpageEntity> webpages = em.createQuery("SELECT w FROM Webpage w", WebpageEntity.class).getResultList();
            List<WebpageEntity> resultsWebpages = new ArrayList<>();
            for (WebpageEntity w : webpages ) {
                if (w.getLikedBy().size() == 0)
                    resultsWebpages.add(w);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetCommonNeighboursTest() {
        long verticesToCheck[] = { 5, 10, 25, 50, 250, 500, 1000, 5000, 10000, 25000, 50000};

        long start = System.currentTimeMillis();
        try{
            for (long id : verticesToCheck) {
                PersonEntity person1 = em.find(PersonEntity.class, 1);
                PersonEntity person2 = em.find(PersonEntity.class, id);

                Set<PersonEntity> friendsPerson1 = person1.getFriends();
                Set<PersonEntity> friendsPerson2 = person2.getFriends();

                Set<WebpageEntity> likesPerson1 = person1.getLikes();
                Set<WebpageEntity> likesPerson2 = person2.getLikes();

                Set<PersonEntity> resultFriends = new HashSet<>(friendsPerson1);
                resultFriends.retainAll(friendsPerson2);
                Set<WebpageEntity> resultLikes = new HashSet<>(likesPerson1);
                resultLikes.retainAll(likesPerson2);

                if (resultFriends.size() == 0)
                    continue;

//                System.out.println(resultFriends.iterator().next().getId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runPathExistenceTest() {
        long start = System.currentTimeMillis();
        try{
            Set<PersonEntity> visited = new HashSet<>();
            LinkedList<PersonEntity> queue = new LinkedList<>();

            PersonEntity personStart = em.find(PersonEntity.class, 1);
            WebpageEntity webpageEnd = em.find(WebpageEntity.class, 90000);

            visited.add(personStart);
            queue.add(personStart);

            while (queue.size() != 0)
            {
                PersonEntity curr = queue.poll();

                if(curr.getLikes().contains(webpageEnd))
                    break;

                for (PersonEntity neighbour : curr.getFriends()) {
                    if (!visited.contains(neighbour)) {
                        visited.add(neighbour);
                        queue.add(neighbour);
                    }
                }
            }

            visited = new HashSet<>();
            queue = new LinkedList<>();

            webpageEnd = em.find(WebpageEntity.class, 100000);

            visited.add(personStart);
            queue.add(personStart);

            while (queue.size() != 0)
            {
                PersonEntity curr = queue.poll();

                if(curr.getLikes().contains(webpageEnd))
                    break;

                for (PersonEntity neighbour : curr.getFriends()) {
                    if (!visited.contains(neighbour)) {
                        visited.add(neighbour);
                        queue.add(neighbour);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    private long countPerson() {
        Query query = em.createQuery("SELECT p FROM Person p");
        List results = query.getResultList();
        return results.size();
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
