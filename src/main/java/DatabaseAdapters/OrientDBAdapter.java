package DatabaseAdapters;

import Data.SocialNetworkData;
import Data.FriendEdge;
import Data.LikeEdge;
import Data.Person;
import Data.Webpage;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrientDBAdapter implements DatabaseAdapter {
    private OrientDB orient;
    private ODatabaseSession db;

    @Override
    public String getDatabaseName() {
        return "OrientDB";
    }

    @Override
    public boolean connectToDatabase() {
        try {
            orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
            db = orient.open("SocialNetwork", "root", "root");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean closeConnection() {
        try {
            orient.close();
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
            insertData(people, friends, webpages, likes);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteGraph() {
        try {
            db.command("TRUNCATE CLASS Likes UNSAFE");
            db.commit();
            db.command("TRUNCATE CLASS FriendWith UNSAFE");
            db.commit();
            db.command("TRUNCATE CLASS Webpage UNSAFE");
            db.commit();
            db.command("TRUNCATE CLASS Person UNSAFE");
            db.commit();
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
                db.command("UPDATE Person SET Age = 25 WHERE ID = " + p.id);
                db.commit();
            }

            for (Webpage w : webpages) {
                db.command("UPDATE Webpage SET Url = \"www.new.pl\" WHERE ID = " + w.id);
                db.commit();
            }

            for (Person p : people) {
                db.command("UPDATE Person SET Age = 30 WHERE ID = " + p.id);
                db.commit();
            }

            for (Webpage w : webpages) {
                db.command("UPDATE Webpage SET Url = \"www.new2.pl\" WHERE ID = " + w.id);
                db.commit();
            }

            for (Person p : people) {
                db.command("UPDATE Person SET Age = 35 WHERE ID = " + p.id);
                db.commit();
            }

            for (Webpage w : webpages) {
                db.command("UPDATE Webpage SET Url = \"www.new3.pl\" WHERE ID = " + w.id);
                db.commit();
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
                db.command("DELETE VERTEX Person WHERE ID = " + p.id);
                db.commit();
            }

            for (Webpage w : webpages) {
                db.command("DELETE VERTEX Webpage WHERE ID = " + w.id);
                db.commit();
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
            String query = "SELECT * from Person where Age > 30";
            OResultSet rs = db.query(query);

            while (rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "SELECT Surname, OUT('Likes').Url FROM Person";
            OResultSet rs = db.query(query);

            while (rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "select * from Person where Name like 'KR%'";
            OResultSet rs = db.query(query);

            while (rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "SELECT * FROM Webpage WHERE ID > 75000 AND Url LIKE '%0.html' AND CreationDate >= '2000-01-01'";
            OResultSet rs = db.query(query);

            while (rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "SELECT ID, OUT().size() FROM Person";
            OResultSet rs = db.query(query);

            while (rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "SELECT Surname, AVG(Age.asFloat()) AS AvgAge FROM Person GROUP BY Surname";
            OResultSet rs = db.query(query);

            while(rs.hasNext())
                rs.next();

            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetNeighboursTest() {
        long start = System.currentTimeMillis();

        try{
            String query = "SELECT ID, out().ID as neighbourId, out().Name, out().Surname, out().Sex, out().Age, out().URL, out().CreationDate FROM Person";
            OResultSet rs = db.query(query);

            while(rs.hasNext())
                rs.next();

            rs.close();
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
            String query = "SELECT * FROM V WHERE out().size() = 0 AND in().size() = 0";
            OResultSet rs = db.query(query);

            while(rs.hasNext())
                rs.next();

            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetCommonNeighboursTest() {
        return 0;
    }

    private void insertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        Map<Long, OVertex> personMap = new HashMap<>();
        Map<Long, OVertex> webpageMap = new HashMap<>();

        for (Person p : people) {
            OVertex personVertex = createOVertex(db, p);
            personVertex.save();
            db.commit();
            personMap.put(p.id, personVertex);
        }

        for (Webpage w : webpages) {
            OVertex webpageVertex = createOVertex(db, w);
            webpageVertex.save();
            db.commit();
            webpageMap.put(w.id, webpageVertex);
        }

        for (FriendEdge f : friends) {
            OVertex p1 = personMap.get(f.person1Id);
            OVertex p2 = personMap.get(f.person2Id);

            p1.addEdge(p2, "FriendWith").save();
            p2.addEdge(p1, "FriendWith").save();
            db.commit();
        }

        for (LikeEdge l : likes) {
            OVertex p = personMap.get(l.personId);
            OVertex w = webpageMap.get(l.webpageId);

            p.addEdge(w, "Likes").save();
            db.commit();
        }
    }

    private void batchInsertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes, int batchSize) {
        Map<Long, OVertex> personMap = new HashMap<>();
        Map<Long, OVertex> webpageMap = new HashMap<>();

        int i = 0;

        for (Person p : people) {
            OVertex personVertex = createOVertex(db, p);
            personVertex.save();

            if (i == batchSize) {
                db.commit();
                i = 0;
            } else
                i++;
            personMap.put(p.id, personVertex);
        }

        for (Webpage w : webpages) {
            OVertex webpageVertex = createOVertex(db, w);
            webpageVertex.save();
            if (i == batchSize) {
                db.commit();
                i = 0;
            } else
                i++;
            webpageMap.put(w.id, webpageVertex);
        }

        for (FriendEdge f : friends) {
            OVertex p1 = personMap.get(f.person1Id);
            OVertex p2 = personMap.get(f.person2Id);

            p1.addEdge(p2, "FriendWith").save();
            p2.addEdge(p1, "FriendWith").save();
            if (i == batchSize) {
                db.commit();
                i = 0;
            } else
                i++;
        }

        for (LikeEdge l : likes) {
            OVertex p = personMap.get(l.personId);
            OVertex w = webpageMap.get(l.webpageId);

            p.addEdge(w, "Likes").save();
            if (i == batchSize) {
                db.commit();
                i = 0;
            } else
                i++;
        }


//        OGraphBatchInsert batch = new OGraphBatchInsert("remote:localhost/socialnetwork", "root", "root");
//        batch.setVertexClass("Person");
//        batch.begin();
//
//        int i = 0;
//
//        for (Person p : people) {
//            batch.createVertex(p.id);
//            batch.setVertexProperties(p.id, propertiesToMap(p));
//
//            if (i == batchSize) {
//                batch.end();
//                batch.begin();
//                i = 0;
//            } else
//                i++;
//        }
//        System.out.println("Webpage");
//
//        batch.end();
//        i = 0;
//        batch.setVertexClass("Webpage");
//        batch.begin();
//
//        for (Webpage w : webpages) {
//            batch.createVertex(w.id);
//            batch.setVertexProperties(w.id, propertiesToMap(w));
//
//            if (i == batchSize) {
//                batch.end();
//                batch.begin();
//                i = 0;
//            } else
//                i++;
//        }
//
//        batch.end();
//        i = 0;
//        batch.setVertexClass("FriendWith");
//        batch.begin();
//
//        for (FriendEdge f : friends) {
//            batch.createEdge(f.person1Id, f.person2Id, new HashMap<>());
//            batch.createEdge(f.person2Id, f.person1Id, new HashMap<>());
//
//            if (i == batchSize) {
//                batch.end();
//                batch.begin();
//                i = 0;
//            } else
//                i++;
//        }
//
//        batch.end();
//        i = 0;
//        batch.setEdgeClass("Likes");
//        batch.begin();
//
//        for (LikeEdge l : likes) {
//            batch.createEdge(l.personId, l.webpageId, new HashMap<>());
//
//            if (i == batchSize) {
//                batch.end();
//                batch.begin();
//                i = 0;
//            } else
//                i++;
//        }
//
//        batch.begin();
    }

    private static Map<String, Object> propertiesToMap(Person person) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("ID", person.id);
        properties.put("Name", person.name);
        properties.put("Surname", person.surname);
        properties.put("Sex", person.sex);
        properties.put("Age", person.age);

        return properties;
    }

    private static Map<String, Object> propertiesToMap(Webpage webpage) {
        Map<String, Object> properties = new HashMap<>();
        Date date = AdapterUtils.localdateToDate(webpage.creationDate);

        properties.put("ID", webpage.id);
        properties.put("Url", webpage.url);
        properties.put("CreationDate", date);

        return properties;
    }

    private static OVertex createOVertex(ODatabaseSession db, Person person) {
        OVertex newVertex = db.newVertex("Person");
        newVertex.setProperty("ID", person.id);
        newVertex.setProperty("Name", person.name);
        newVertex.setProperty("Surname", person.surname);
        newVertex.setProperty("Sex", person.sex);
        newVertex.setProperty("Age", person.age);
        return newVertex;
    }

    private static OVertex createOVertex(ODatabaseSession db, Webpage webpage) {
        Date date = AdapterUtils.localdateToDate(webpage.creationDate);

        OVertex newVertex = db.newVertex("Webpage");
        newVertex.setProperty("ID", webpage.id);
        newVertex.setProperty("Url", webpage.url);
        newVertex.setProperty("CreationDate", date);
        return newVertex;
    }
}
