package DatabaseAdapters;

import Data.SocialNetworkData;
import Data.FriendEdge;
import Data.LikeEdge;
import Data.Person;
import Data.Webpage;
import DatabaseAdapters.ObjectDBEntities.PersonEntity;
import DatabaseAdapters.ObjectDBEntities.WebpageEntity;
import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.print.Doc;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDBAdapter implements DatabaseAdapter {
    private MongoClient mongoClient;
    private DB database;
    private MongoDatabase mongoDatabase;

    @Override
    public String getDatabaseName() {
        return "MongoDB";
    }

    @Override
    public boolean connectToDatabase() {
        try {
            mongoClient = new MongoClient();
            database = mongoClient.getDB("SocialNetwork");
            mongoDatabase = mongoClient.getDatabase("SocialNetwork");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean closeConnection() { return true; }

    @Override
    public boolean createGraph(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

        try {
            batchInsertData(people, friends, webpages, likes, 5000);
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
    public long runDeleteTest(SocialNetworkData data) {
        List<Person> people = data.getPeople();
        List<FriendEdge> friends = data.getFriends();
        List<Webpage> webpages = data.getWebpages();
        List<LikeEdge> likes = data.getLikes();

        long start = System.currentTimeMillis();

        try{
            DBCollection peopleCollection = database.getCollection("People");
            DBCollection webpageCollection = database.getCollection("Webpages");

            for(Person p : people) {
                BasicDBObject query = new BasicDBObject();
                query.put("id", p.id);
                peopleCollection.remove(query);

                BasicDBObject match = new BasicDBObject("friends", p.id);
                peopleCollection.update(match, new BasicDBObject("$pull", match));
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
    public long runSelectByIntegerTest() {
        long start = System.currentTimeMillis();

        try{
            DBCollection peopleCollection = database.getCollection("People");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("age", new BasicDBObject("$gt", 30));
            DBCursor cursor = peopleCollection.find(searchQuery);

            while (cursor.hasNext())
                cursor.next();

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
            DBCollection peopleCollection = database.getCollection("People");
            DBCollection webpagesCollection = database.getCollection("Webpages");

            DBCursor cursor = peopleCollection.find();

            Map<Long, String> webpageUrlMap = new HashMap<>();

            while (cursor.hasNext()) {
                BasicDBObject personObject = (BasicDBObject) cursor.next();
                BasicDBList likes = (BasicDBList) personObject.get("likes");
                for (Object l : likes.toArray()) {
                    Long likeWebpageId = (Long) l;
                    if(webpageUrlMap.containsKey(likeWebpageId))
                        continue;

                    BasicDBObject searchQuery = new BasicDBObject();
                    searchQuery.put("id", likeWebpageId);
                    DBCursor cursorWebpages = webpagesCollection.find(searchQuery);
                    while(cursorWebpages.hasNext()) {
                        BasicDBObject webpageObject = (BasicDBObject)cursorWebpages.next();
                        webpageUrlMap.put(likeWebpageId, webpageObject.getString("url"));
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

    @Override
    public long runSelectByStringWithLikeTest() {
        long start = System.currentTimeMillis();

        try{
            DBCollection peopleCollection = database.getCollection("People");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", new BasicDBObject("$regex", "^KR.*"));
            DBCursor cursor = peopleCollection.find(searchQuery);
            while(cursor.hasNext())
                cursor.next();

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
            DBCollection webpagesCollection = database.getCollection("Webpages");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("id", new BasicDBObject("$gt", 75000));
            searchQuery.put("url", new BasicDBObject("$regex", ".*0\\.html$"));
            searchQuery.put("creation_date", new BasicDBObject("$gte", LocalDate.parse("2000-01-01")));
            DBCursor cursor = webpagesCollection.find(searchQuery);
            while(cursor.hasNext())
                cursor.next();

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
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");

            ArrayList<Document> result = peopleCollection.aggregate(Arrays.asList(new Document("$project",
                    new Document("id", "$id")
                            .append("neighbour_count",
                                    new Document("$sum", Arrays.asList(new Document("$size", "$likes"),
                                            new Document("$size", "$friends"))))))).into(new ArrayList<>());

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
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");

            ArrayList<Document> result = peopleCollection.aggregate(Arrays.asList(new Document("$group",
                    new Document("_id", "$surname").append("avgAge", new Document("$avg", "$age")))))
                    .into(new ArrayList<>());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }


//    $match
//    {
//        id: param
//    }
//    $lookup
//     {
//      from: "People",
//      localField: "friends",
//      foreignField: "id",
//      as: "friendNeighbours"
//    }
//    {
//      from: "Webpages",
//      localField: "likes",
//      foreignField: "id",
//      as: "WebpageNeighbours"
//    }

    @Override
    public long runGetNeighboursTest() {
        long personCount = countPerson();

        long start = System.currentTimeMillis();

        try{
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");

            for(long i = 1; i <= personCount; i++) {
                AggregateIterable<Document> result = peopleCollection.aggregate(Arrays.asList(new Document("$match",
                                new Document("id", i)),
                        new Document("$lookup",
                                new Document("from", "People")
                                        .append("localField", "friends")
                                        .append("foreignField", "id")
                                        .append("as", "friendNeighbours")),
                        new Document("$lookup",
                                new Document("from", "Webpages")
                                        .append("localField", "likes")
                                        .append("foreignField", "id")
                                        .append("as", "WebpageNeighbours"))));

                if(result.cursor().hasNext())
                    result.cursor().next();
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
            List<Document> results = new ArrayList<>();
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");
            MongoCollection<Document> webpagesCollection = mongoDatabase.getCollection("Webpages");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("friends", Arrays.asList());
            searchQuery.put("likes", Arrays.asList());
            ArrayList<Document> resultsPeople = peopleCollection.find(searchQuery).into(new ArrayList<>());

            ArrayList<Document> webpages = webpagesCollection.find().into(new ArrayList<>());
            for(Document d : webpages ) {
                BasicDBObject searchLikes = new BasicDBObject();
                searchLikes.put("likes", d.getLong("id"));
                long countLikes = peopleCollection.countDocuments(searchLikes);
                if(countLikes == 0)
                    results.add(d);
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

        try {
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");
            MongoCollection<Document> webpagesCollection = mongoDatabase.getCollection("Webpages");

            for (long id : verticesToCheck) {
                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("id", 1);
                Document person1 = peopleCollection.find(searchQuery).first();
                searchQuery = new BasicDBObject();
                searchQuery.put("id", id);
                Document person2 = peopleCollection.find(searchQuery).first();

                List<Long> person1Friends = person1.getList("friends", Long.class);
                List<Long> person2Friends = person2.getList("friends", Long.class);

                List<Long> person1Likes = person1.getList("likes", Long.class);
                List<Long> person2Likes = person2.getList("likes", Long.class);

                List<Long> commonFriends = new ArrayList<>(person1Friends);
                commonFriends.retainAll(person2Friends);

                List<Long> commonLikes = new ArrayList<>(person1Likes);
                commonLikes.retainAll(person2Likes);

                searchQuery = new BasicDBObject();
                searchQuery.put("id", new BasicDBObject("$in", commonFriends));
                ArrayList<Document> resultsFriends = peopleCollection.find(searchQuery).into(new ArrayList<>());

                searchQuery = new BasicDBObject();
                searchQuery.put("id", new BasicDBObject("$in", commonLikes));
                ArrayList<Document> resultsLikes = webpagesCollection.find(searchQuery).into(new ArrayList<>());
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

        try {
            MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");

            Set<Long> visited = new HashSet<>();
            LinkedList<Long> queue = new LinkedList<>();

            long personStart = 1;
            long webpageEnd = 90000;

            BasicDBObject searchLikes;
            Document curr;

            visited.add(personStart);
            queue.add(personStart);

            while (queue.size() != 0)
            {
                Long currId = queue.poll();
                searchLikes = new BasicDBObject();
                searchLikes.put("id", currId);
                curr = peopleCollection.find(searchLikes).first();

                if(curr.getList("likes", Long.class).contains(webpageEnd))
                    break;

                for (Long neighbour : curr.getList("friends", Long.class)) {
                    if (!visited.contains(neighbour)) {
                        visited.add(neighbour);
                        queue.add(neighbour);
                    }
                }
            }

            webpageEnd = 100000;

            visited = new HashSet<>();
            queue = new LinkedList<>();

            visited.add(personStart);
            queue.add(personStart);

            while (queue.size() != 0)
            {
                Long currId = queue.poll();
                searchLikes = new BasicDBObject();
                searchLikes.put("id", currId);
                curr = peopleCollection.find(searchLikes).first();

                if(curr.getList("likes", Long.class).contains(webpageEnd))
                    break;

                for (Long neighbour : curr.getList("friends", Long.class)) {
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
        MongoCollection<Document> peopleCollection = mongoDatabase.getCollection("People");
        return peopleCollection.countDocuments();
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
