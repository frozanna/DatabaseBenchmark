package DatabaseAdapters;

import Data.SocialNetworkData;
import Data.FriendEdge;
import Data.LikeEdge;
import Data.Person;
import Data.Webpage;

import java.sql.*;
import java.util.List;

public class MySQLAdapter implements DatabaseAdapter {
    private Connection conn;

    @Override
    public String getDatabaseName() {
        return "MySQL";
    }

    @Override
    public boolean connectToDatabase() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/socialnetwork?rewriteBatchedStatements=true", "Admin", "root");
        } catch (InstantiationException e) {
            e.printStackTrace();
            return false;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean closeConnection() {
        try {
            if(conn != null)
                conn.close();
        } catch (SQLException e) {
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

        try{
            batchInsertData(people, friends, webpages, likes, 1000);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean deleteGraph() {
        try{
            String queryFfCheck = "SET foreign_key_checks = ?";
            String queryTruncate = "TRUNCATE TABLE ";
            PreparedStatement preparedStmtFkCheck = conn.prepareStatement(queryFfCheck);
            PreparedStatement preparedStmtTruncate;

            preparedStmtFkCheck.setInt(1, 0);
            preparedStmtFkCheck.execute();

            preparedStmtTruncate = conn.prepareStatement(queryTruncate + "likes");
            preparedStmtTruncate.execute();

            preparedStmtTruncate = conn.prepareStatement(queryTruncate + "friends");
            preparedStmtTruncate.execute();

            preparedStmtTruncate = conn.prepareStatement(queryTruncate + "webpage");
            preparedStmtTruncate.execute();

            preparedStmtTruncate = conn.prepareStatement(queryTruncate + "person");
            preparedStmtTruncate.execute();

            preparedStmtFkCheck.setInt(1, 1);
            preparedStmtFkCheck.execute();
        } catch (SQLException e) {
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
        } catch (SQLException e) {
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
        } catch (SQLException e) {
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

        try {
            String personQuery = "UPDATE person SET Age = ? WHERE ID = ?";
            String webpageQuery = "UPDATE webpage SET Url = ? WHERE ID = ?";

            for(Person p : people) {
                PreparedStatement preparedStmt = conn.prepareStatement(personQuery);
                preparedStmt.setInt(1, 25);
                preparedStmt.setLong(2, p.id);
                preparedStmt.execute();
            }

            for(Webpage w : webpages){
                PreparedStatement preparedStmt = conn.prepareStatement(webpageQuery);
                preparedStmt.setString(1, "www.new.pl");
                preparedStmt.setLong(2, w.id);
                preparedStmt.execute();
            }

            for(Person p : people) {
                PreparedStatement preparedStmt = conn.prepareStatement(personQuery);
                preparedStmt.setInt(1, 30);
                preparedStmt.setLong(2, p.id);
                preparedStmt.execute();
            }

            for(Webpage w : webpages){
                PreparedStatement preparedStmt = conn.prepareStatement(webpageQuery);
                preparedStmt.setString(1, "www.new2.pl");
                preparedStmt.setLong(2, w.id);
                preparedStmt.execute();
            }

            for(Person p : people) {
                PreparedStatement preparedStmt = conn.prepareStatement(personQuery);
                preparedStmt.setInt(1, 35);
                preparedStmt.setLong(2, p.id);
                preparedStmt.execute();
            }

            for(Webpage w : webpages){
                PreparedStatement preparedStmt = conn.prepareStatement(webpageQuery);
                preparedStmt.setString(1, "www.new3.pl");
                preparedStmt.setLong(2, w.id);
                preparedStmt.execute();
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

        try {
            String personQuery = "DELETE FROM person WHERE ID = ?";
            String webpageQuery = "DELETE FROM webpage WHERE ID = ?";

            for(Person p : people) {
                PreparedStatement preparedStmt = conn.prepareStatement(personQuery);
                preparedStmt.setLong(1, p.id);
                preparedStmt.execute();
            }

            for(Webpage w : webpages){
                PreparedStatement preparedStmt = conn.prepareStatement(webpageQuery);
                preparedStmt.setLong(1, w.id);
                preparedStmt.execute();
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

        try {
            String query = "SELECT * FROM person WHERE Age > 30";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
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

        try {
            String query = "SELECT p.Surname, w.Url FROM likes l join person p on l.Person_ID = p.ID join webpage w on l.Webpage_ID = w.ID;";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
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

        try {
            String query = "SELECT * FROM person p WHERE p.Name like \"KR%\"";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
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

        try {
            String query = "SELECT * from Webpage w WHERE w.ID > 75000 AND w.Url like '%0.html' AND w.Creation_Date  >= '2000-01-01'";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
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

        try {
            String query = "SELECT p.ID, COUNT(distinct l.ID) + COUNT(distinct f1.ID) + COUNT(distinct f2.ID) AS NeighboursCount\n" +
                    "FROM person p\n" +
                    "LEFT JOIN friends f1\n" +
                    "ON f1.Person1_ID = p.ID\n" +
                    "LEFT JOIN friends f2\n" +
                    "ON f2.Person2_ID = p.ID\n" +
                    "LEFT JOIN likes l\n" +
                    "ON p.ID = l.Person_ID\n" +
                    "GROUP BY p.ID";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
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

        try {
            String query = "SELECT p.Surname, AVG(p.Age) AS AvgAge FROM person p GROUP BY p.Surname";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()) {}
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runGetNeighboursTest() {
        long count = countPerson();
        if (count < 0) {
            System.out.println("Cannot count vertices.");
            return -1;
        }

        String queryFriend = "SELECT p.ID, pf1.*\n" +
                "FROM person p\n" +
                "LEFT JOIN friends f1\n" +
                "ON f1.Person1_ID = p.ID\n" +
                "LEFT JOIN person pf1 on pf1.ID = f1.Person2_ID\n" +
                "WHERE p.ID = ?\n" +
                "UNION ALL\n" +
                "SELECT p.ID, pf2.*\n" +
                "FROM person p\n" +
                "LEFT JOIN friends f2\n" +
                "ON f2.Person2_ID = p.ID\n" +
                "LEFT JOIN person pf2 on pf2.ID = f2.Person1_ID\n" +
                "WHERE p.ID = ?\n";
        String queryLikes = "SELECT p.ID, w.*\n" +
                "FROM person p\n" +
                "LEFT JOIN likes l\n" +
                "ON p.ID = l.Person_ID\n" +
                "LEFT JOIN webpage w on l.Webpage_ID = w.ID\n" +
                "where p.id = ?";
        long start = System.currentTimeMillis();

        try {
            for (long i = 1; i <= count; i++) {
                PreparedStatement preparedStmt = conn.prepareStatement(queryFriend);
                preparedStmt.setLong(1, i);
                preparedStmt.setLong(2, i);
                ResultSet rs = preparedStmt.executeQuery();
                while (rs.next()) {}

                preparedStmt = conn.prepareStatement(queryLikes);
                preparedStmt.setLong(1, i);
                rs = preparedStmt.executeQuery();
                while (rs.next()) {}
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

        try {
            String query = "SELECT p.* FROM person p\n" +
                    "LEFT JOIN friends f on p.ID in (f.Person1_ID, f.Person2_ID)\n" +
                    "LEFT JOIN likes l on p.ID = l.Person_ID\n" +
                    "WHERE l.ID IS NULL\n" +
                    "AND f.ID IS NULL\n";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {}

            query = "SELECT w.* FROM webpage w\n" +
                    "LEFT JOIN likes l on w.ID = l.Webpage_ID\n" +
                    "WHERE l.ID IS NULL\n";
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
//            while (rs.next()) {}
            stmt.close();
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

    private void insertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) throws SQLException {
        String personQuery = "INSERT INTO person (ID, Name, Surname, Sex, Age)" + " VALUES (?, ?, ?, ?, ?)";
        String webpageQuery = "INSERT INTO webpage (ID, Url, Creation_Date)" + " VALUES (?, ?, ?)";
        String friendsQuery = "INSERT INTO friends (Person1_ID, Person2_ID)" + " VALUES (?, ?)";
        String likesQuery = "INSERT INTO likes (Person_ID, Webpage_ID)" + " VALUES (?, ?)";


        for (Person p : people) {
            PreparedStatement preparedStmt = conn.prepareStatement(personQuery);
            updateStatementWithValues(preparedStmt, p);
            preparedStmt.execute();
        }

        for (Webpage w : webpages) {
            PreparedStatement preparedStmt = conn.prepareStatement(webpageQuery);
            updateStatementWithValues(preparedStmt, w);
            preparedStmt.execute();
        }

        for (FriendEdge f : friends) {
            PreparedStatement preparedStmt = conn.prepareStatement(friendsQuery);
            updateStatementWithValues(preparedStmt, f);
            preparedStmt.execute();
        }

        for (LikeEdge l : likes) {
            PreparedStatement preparedStmt = conn.prepareStatement(likesQuery);
            updateStatementWithValues(preparedStmt, l);
            preparedStmt.execute();
        }
    }

    private void batchInsertData(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes, int batch) throws SQLException {
        String personQuery = "INSERT INTO person (ID, Name, Surname, Sex, Age)" + " VALUES (?, ?, ?, ?, ?)";
        String webpageQuery = "INSERT INTO webpage (ID, Url, Creation_Date)" + " VALUES (?, ?, ?)";
        String friendsQuery = "INSERT INTO friends (Person1_ID, Person2_ID)" + " VALUES (?, ?)";
        String likesQuery = "INSERT INTO likes (Person_ID, Webpage_ID)" + " VALUES (?, ?)";

        PreparedStatement preparedPeopleStmt = conn.prepareStatement(personQuery);
        PreparedStatement preparedWebpageStmt = conn.prepareStatement(webpageQuery);
        PreparedStatement preparedFriendsStmt = conn.prepareStatement(friendsQuery);
        PreparedStatement preparedLikesStmt = conn.prepareStatement(likesQuery);

        int i = 0;

        for (Person p : people) {
            updateStatementWithValues(preparedPeopleStmt, p);
            preparedPeopleStmt.addBatch();
            if (i == batch) {
                preparedPeopleStmt.executeBatch();
                i = 0;
            } else
                i++;
        }

        preparedPeopleStmt.executeBatch();
        i = 0;

        for (Webpage w : webpages) {
            updateStatementWithValues(preparedWebpageStmt, w);
            preparedWebpageStmt.addBatch();
            if (i == batch) {
                preparedWebpageStmt.executeBatch();
                i = 0;
            } else
                i++;
        }

        preparedWebpageStmt.executeBatch();
        i = 0;

        for (FriendEdge f : friends) {
            updateStatementWithValues(preparedFriendsStmt, f);
            preparedFriendsStmt.addBatch();
            if (i == batch) {
                preparedFriendsStmt.executeBatch();
                i = 0;
            } else
                i++;
        }

        preparedFriendsStmt.executeBatch();
        i = 0;

        for (LikeEdge l : likes) {
            updateStatementWithValues(preparedLikesStmt, l);
            preparedLikesStmt.addBatch();
            if (i == batch) {
                preparedLikesStmt.executeBatch();
                i = 0;
            } else
                i++;
        }

        preparedLikesStmt.executeBatch();
    }

    private void updateStatementWithValues(PreparedStatement preparedStmt, Person person) throws SQLException {
        preparedStmt.setLong(1, person.id);
        preparedStmt.setString(2, person.name);
        preparedStmt.setString(3, person.surname);
        preparedStmt.setBoolean(4, person.sex);
        preparedStmt.setInt(5, person.age);
    }

    private void updateStatementWithValues(PreparedStatement preparedStmt, Webpage webpage) throws SQLException {
        Date date = AdapterUtils.localdateToDate(webpage.creationDate);

        preparedStmt.setLong(1, webpage.id);
        preparedStmt.setString(2, webpage.url);
        preparedStmt.setDate(3, date);
    }

    private void updateStatementWithValues(PreparedStatement preparedStmt, FriendEdge friend) throws SQLException {
        preparedStmt.setLong(1, friend.person1Id);
        preparedStmt.setLong(2, friend.person2Id);
    }

    private void updateStatementWithValues(PreparedStatement preparedStmt, LikeEdge like) throws SQLException {
        preparedStmt.setLong(1, like.personId);
        preparedStmt.setLong(2, like.webpageId);
    }

    private long countPerson() {
        String query = "SELECT COUNT(p.ID) FROM Person p";
        long result = -1;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next())
                result = rs.getLong(1);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }
}
