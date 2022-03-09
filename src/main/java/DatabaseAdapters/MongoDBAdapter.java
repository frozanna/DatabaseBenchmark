package DatabaseAdapters;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;
import com.mongodb.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDBAdapter implements DatabaseAdapter {
    private MongoClient mongoClient;
    private DB database;

    @Override
    public String getDatabaseName() {
        return "MongoDB";
    }

    @Override
    public boolean connectToDatabase() {
        try {
            mongoClient = new MongoClient();
            database = mongoClient.getDB("SocialNetwork");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean closeConnection() { return true; }

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
            database.dropDatabase();
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
            DBCollection peopleCollection = database.getCollection("People");
            DBCollection webpageCollection = database.getCollection("Webpages");

            for(Person p : people) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", p.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("age", 25);

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                peopleCollection.update(query, updateObject);
            }

            for(Webpage w : webpages) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", w.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("url", "www.new.pl");

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                webpageCollection.update(query, updateObject);
            }

            for(Person p : people) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", p.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("age", 30);

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                peopleCollection.update(query, updateObject);
            }

            for(Webpage w : webpages) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", w.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("url", "www.new2.pl");

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                webpageCollection.update(query, updateObject);
            }

            for(Person p : people) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", p.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("age", 35);

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                peopleCollection.update(query, updateObject);
            }

            for(Webpage w : webpages) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", w.id);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("url", "www.new3.pl");

                BasicDBObject updateObject = new BasicDBObject();
                updateObject.put("$set", newDocument);

                webpageCollection.update(query, updateObject);
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
            DBCollection peopleCollection = database.getCollection("People");
            DBCollection webpageCollection = database.getCollection("Webpages");

            for(Person p : people) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", p.id);
                peopleCollection.remove(query);
            }

            for(Webpage w : webpages) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", w.id);
                webpageCollection.remove(query);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByIntParTest() {
        long start = System.currentTimeMillis();

        try{
            DBCollection peopleCollection = database.getCollection("People");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("age", new BasicDBObject("$gt", 30));
            DBCursor cursor = peopleCollection.find(searchQuery);

            int i = 0;

            while (cursor.hasNext()) {
                cursor.next();
                i++;
            }

//            System.out.println(i);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectEdgesWithVertexParTest() {
        long start = System.currentTimeMillis();

        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter("tmp", true));
            writer.append("=========================");
            DBCollection peopleCollection = database.getCollection("People");
            DBCollection webpagesCollection = database.getCollection("Webpages");

            DBCursor cursor = peopleCollection.find();

            Map<Long, String> webpageUrlMap = new HashMap<>();

            int i = 0;

            while (cursor.hasNext()) {
                BasicDBObject personObject = (BasicDBObject) cursor.next();
                BasicDBList likes = (BasicDBList) personObject.get("likes");
                Long id = personObject.getLong("id");
                for (Object l : likes.toArray()) {
                    Long likeWebpageId = (Long) l;
                    writer.append(id + "+" + likeWebpageId + ",");
                    if(webpageUrlMap.containsKey(likeWebpageId))
                        continue;

                    BasicDBObject searchQuery = new BasicDBObject();
                    searchQuery.put("id", likeWebpageId);
                    DBCursor cursorWebpages = webpagesCollection.find(searchQuery);
                    while(cursorWebpages.hasNext()) {
                        BasicDBObject webpageObject = (BasicDBObject)cursorWebpages.next();
                        webpageUrlMap.put(likeWebpageId, webpageObject.getString("url"));
                    }

                    i++;
                }

                cursor.next();
            }

            writer.close();
//            System.out.println(i);
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByStringWithLike() {
        return 0;
    }

    private void insertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
        DBCollection peopleCollection = database.getCollection("People");
        DBCollection webpageCollection = database.getCollection("Webpages");

        peopleCollection.createIndex(new BasicDBObject("id", 1), new BasicDBObject("unique", true));
        peopleCollection.createIndex(new BasicDBObject("likes", 1));
        peopleCollection.createIndex(new BasicDBObject("friends", 1));
        webpageCollection.createIndex(new BasicDBObject("id", 1), new BasicDBObject("unique", true));

        for (Webpage w : webpages) {
            DBObject dbWebpage = createDBObject(w);
            webpageCollection.insert(dbWebpage);
        }

        for (Person p : people) {
            List<Long> personFriends = friends.stream().filter(f -> f.person1Id == p.id || f.person2Id == p.id)
                    .map(f -> { if (f.person1Id == p.id) return f.person2Id;
                                else return f.person1Id; }).collect(Collectors.toList());
            List<Long> personLikes = likes.stream().filter(l -> l.personId == p.id).map(l -> l.webpageId).collect(Collectors.toList());

            DBObject dbPerson = createDBObject(p, personFriends, personLikes);
            peopleCollection.insert(dbPerson);
        }
    }

    private void batchInsertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes, int batch) {
        DBCollection peopleCollection = database.getCollection("People");
        DBCollection webpageCollection = database.getCollection("Webpages");

        peopleCollection.createIndex(new BasicDBObject("id", 1), new BasicDBObject("unique", true));
        peopleCollection.createIndex(new BasicDBObject("likes", 1));
        peopleCollection.createIndex(new BasicDBObject("friends", 1));
        webpageCollection.createIndex(new BasicDBObject("id", 1), new BasicDBObject("unique", true));

        int i = 0;

        BulkWriteOperation peopleBulk = peopleCollection.initializeUnorderedBulkOperation();
        BulkWriteOperation webpageBulk = webpageCollection.initializeUnorderedBulkOperation();

        for (Webpage w : webpages) {
            DBObject dbWebpage = createDBObject(w);
            webpageBulk.insert(dbWebpage);
            if (i == batch) {
                webpageBulk.execute();
                webpageBulk = webpageCollection.initializeUnorderedBulkOperation();
                i = 0;
            } else
                i++;
        }

        webpageBulk.execute();
        i = 0;

        for (Person p : people) {
            List<Long> personFriends = friends.stream().filter(f -> f.person1Id == p.id || f.person2Id == p.id)
                    .map(f -> { if (f.person1Id == p.id) return f.person2Id;
                    else return f.person1Id; }).collect(Collectors.toList());
            List<Long> personLikes = likes.stream().filter(l -> l.personId == p.id).map(l -> l.webpageId).collect(Collectors.toList());

            DBObject dbPerson = createDBObject(p, personFriends, personLikes);
            peopleBulk.insert(dbPerson);
            if (i == batch) {
                peopleBulk.execute();
                peopleBulk = peopleCollection.initializeUnorderedBulkOperation();
                i = 0;
            } else
                i++;
        }

        peopleBulk.execute();
    }

    private DBObject createDBObject(Person p, List<Long> friends, List<Long> likes) {
        DBObject person = new BasicDBObject("id", p.id)
                .append("name", p.name)
                .append("surname", p.surname)
                .append("sex", p.sex)
                .append("age", p.age)
                .append("friends", friends)
                .append("friendsWith", friends)
                .append("likes", likes);

        return person;
    }


    private DBObject createDBObject(Webpage w) {
        DBObject webpage = new BasicDBObject("id", w.id)
                .append("url", w.url)
                .append("creation_date", w.creationDate);

        return webpage;
    }
}
