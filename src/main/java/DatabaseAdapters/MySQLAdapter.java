package DatabaseAdapters;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

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
    public boolean createGraph(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
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
    public long runInsertTest(List<Person> people, List<FriendEdge> friends, List<Webpage> webpages, List<LikeEdge> likes) {
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
    public long runUpdateTest(List<Person> people, List<Webpage> webpages) {
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
    public long runDeleteTest(List<Person> people, List<Webpage> webpages) {
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
    public long runSelectByIntTest() {
        long start = System.currentTimeMillis();

        try {
            String query = "SELECT * FROM person WHERE Age > 30";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            int i = 0;

            while (rs.next()) {  i++; }

            System.out.println(i);

            stmt.close();
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

        try {
            String query = "SELECT p.Surname, w.Url FROM likes l join person p on l.Person_ID = p.ID join webpage w on l.Webpage_ID = w.ID;";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            int i = 0;

            while (rs.next()) {
                i++;
            }

//            System.out.println(i);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByStringWithLike() {
        long start = System.currentTimeMillis();

        try {
            String query = "SELECT * FROM person p WHERE p.Name like \"KR%\"";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            int i = 0;

            while (rs.next()) {
                i++;
            }

//            System.out.println(i);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
    }

    @Override
    public long runSelectByMultipleParTest() {
        long start = System.currentTimeMillis();

        try {
            String query = "SELECT * from Webpage w WHERE w.ID > 75000 AND w.Url like '%0.html' AND w.Creation_Date  >= '2000-01-01'";

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            int i = 0;

            while (rs.next()) {
                i++;
            }

            System.out.println(i);
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

        long finish = System.currentTimeMillis();
        return finish - start;
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
}
