package DatabaseAdapters;

import DataReaders.SocialNetworkData;
import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;
import org.neo4j.driver.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4jAdapter implements DatabaseAdapter {
    private Driver driver;

    @Override
    public String getDatabaseName() {
        return "Neo4j";
    }

    @Override
    public boolean connectToDatabase() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "root"));
        return true;
    }

    @Override
    public boolean closeConnection() {
        try{
            driver.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteGraph() {
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("MATCH (a) DETACH DELETE a");
                tx.commit();
            }
            catch (Exception e) {
                throw e;
            }
        } catch (Exception e) {
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
            Session session = driver.session();
            Transaction tx = null;
            String personQuery = "MATCH (p:Person {ID: $id}) SET p.age = $age";
            String webpageQuery = "MATCH (p:Webpage {ID: $id}) SET p.url = $url";

            for (Person p : people) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", p.id);
                parameters.put("age", 25);
                tx.run(personQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Webpage w : webpages) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", w.id);
                parameters.put("url", "ww.new.pl");
                tx.run(webpageQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Person p : people) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", p.id);
                parameters.put("age", 30);
                tx.run(personQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Webpage w : webpages) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", w.id);
                parameters.put("url", "ww.new2.pl");
                tx.run(webpageQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Person p : people) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", p.id);
                parameters.put("age", 35);
                tx.run(personQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Webpage w : webpages) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", w.id);
                parameters.put("url", "ww.new3.pl");
                tx.run(webpageQuery, parameters);
                tx.commit();
                tx.close();
            }

            session.close();
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
            Session session = driver.session();
            Transaction tx;
            String personQuery = "MATCH (p:Person {ID: $id}) DETACH DELETE p";
            String webpageQuery = "MATCH (p:Webpage {ID: $id}) DETACH DELETE p";

            for (Person p : people) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", p.id);
                tx.run(personQuery, parameters);
                tx.commit();
                tx.close();
            }

            for (Webpage w : webpages) {
                tx = session.beginTransaction();
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("id", w.id);
                tx.run(webpageQuery, parameters);
                tx.commit();
                tx.close();
            }

            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (p:Person) WHERE p.Age > 30 RETURN p";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (p: Person)-[LIKES]->(w: Webpage) RETURN p.Surname, w.Url";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (p:Person) WHERE p.Name STARTS WITH 'KR' RETURN p";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (w:Webpage) WHERE w.ID > 75000 AND w.Url ENDS WITH '0.html' AND w.CreationDate >= date({year:2000,month:1,day:1}) RETURN w";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (p:Person)-[r]->() RETURN p.ID, count(r) AS NeighboursCount";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
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
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            int i = 0;

            String query = "MATCH (p:Person) RETURN p.Surname, AVG(toFloat(p.Age)) As AvgAge";

            Result result = tx.run(query);
            while (result.hasNext()) {
                result.next();
                i++;
            }

//            System.out.println(i);
            tx.close();
            session.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }


        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetAllNeighboursTest() {
        return 0;
    }

    private void insertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        Session session = driver.session();

        for (Person p : people) {
            Transaction tx = session.beginTransaction();

            String query = "CREATE (p:Person {ID: $id, Name: $name, Surname: $surname, Sex: $sex, Age: $age})";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", p.id);
            parameters.put("name", p.name);
            parameters.put("surname", p.surname);
            parameters.put("sex", p.sex);
            parameters.put("age", p.age);

            tx.run(query, parameters);
            tx.commit();
            tx.close();
        }

        for (Webpage w : webpages) {
            Transaction tx = session.beginTransaction();

            String query = "CREATE (w:Webpage {ID: $id, Url: $url, CreationDate: date($creation)})";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", w.id);
            parameters.put("url", w.url);
            parameters.put("creation", w.creationDate);

            tx.run(query, parameters);
            tx.commit();
            tx.close();
        }

        for (FriendEdge f : friends) {
            Transaction tx = session.beginTransaction();

            String query = "MATCH (a:Person {ID: $p1id}), (b:Person {ID: $p2id}) CREATE (a)-[r1:FRIEND_WITH]->(b)-[r2:FRIEND_WITH]->(a)";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("p1id", f.person1Id);
            parameters.put("p2id", f.person2Id);

            tx.run(query, parameters);
            tx.commit();
            tx.close();
        }

        int i = 0;
        for (LikeEdge l : likes) {
            System.out.println(l.id);
            i++;
            if(i > 75000)
                continue;
            Transaction tx = session.beginTransaction();

            String query = "MATCH (p:Person {ID: $pId}), (w:Webpage {ID: $wId}) CREATE UNIQUE (p)-[r:LIKES]->(w)";
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("pId", l.personId);
            parameters.put("wId", l.webpageId);

            tx.run(query, parameters);
            tx.commit();
            tx.close();
        }


        String query = "CREATE BTREE INDEX WebpageIndex IF NOT EXISTS FOR (w:Webpage) ON w.ID";
        Transaction tx = session.beginTransaction();
        tx.run(query);
        query = "CREATE BTREE INDEX PersonIndex IF NOT EXISTS FOR (p:Person) ON p.ID";
        tx.run(query);
        tx.commit();

        session.close();
    }

    private void batchInsertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes, int batch) {
        Session session = driver.session();

        Transaction tx = session.beginTransaction();
        int i = 0;

        String query = "CREATE (p:Person {ID: $id, Name: $name, Surname: $surname, Sex: $sex, Age: $age})";
        for (Person p : people) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", p.id);
            parameters.put("name", p.name);
            parameters.put("surname", p.surname);
            parameters.put("sex", p.sex);
            parameters.put("age", p.age);

            tx.run(query, parameters);

            if (i == batch) {
                tx.commit();
                tx.close();
                tx = session.beginTransaction();
                i = 0;
            } else
                i++;
        }

        tx.commit();
        tx.close();
        tx = session.beginTransaction();

        query = "CREATE (w:Webpage {ID: $id, Url: $url, CreationDate: date($creation)})";
        for (Webpage w : webpages) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("id", w.id);
            parameters.put("url", w.url);
            parameters.put("creation", w.creationDate);

            tx.run(query, parameters);

            if (i == batch) {
                tx.commit();
                tx.close();
                tx = session.beginTransaction();
                i = 0;
            } else
                i++;
        }

        tx.commit();
        tx.close();
        tx = session.beginTransaction();

        query = "MATCH (a:Person {ID: $p1id}), (b:Person {ID: $p2id}) CREATE (a)-[r1:FRIEND_WITH]->(b)-[r2:FRIEND_WITH]->(a)";
        for (FriendEdge f : friends) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("p1id", f.person1Id);
            parameters.put("p2id", f.person2Id);

            tx.run(query, parameters);

            if (i == batch) {
                tx.commit();
                tx.close();
                tx = session.beginTransaction();
                i = 0;
            } else
                i++;
        }

        tx.commit();
        tx.close();
        tx = session.beginTransaction();

        query = "MATCH (p:Person {ID: $pId}), (w:Webpage {ID: $wId}) CREATE (p)-[r:LIKES]->(w)";
        for (LikeEdge l : likes) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("pId", l.personId);
            parameters.put("wId", l.webpageId);

            tx.run(query, parameters);

            if (i == batch) {
                tx.commit();
                tx.close();
                tx = session.beginTransaction();
                i = 0;
            } else
                i++;
        }

        tx.commit();
        tx.close();
        session.close();
    }
}
