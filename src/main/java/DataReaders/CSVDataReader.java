package DataReaders;

import Objects.FriendEdge;
import Objects.LikeEdge;
import Objects.Person;
import Objects.Webpage;

import java.io.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class CSVDataReader implements DataReader {
    private static final String DELIMITER = ",";

    @Override
    public SocialNetworkData getData(String path) {
        SocialNetworkData data = new SocialNetworkData();
        data.setPeople(readPersons(path + SocialNetworkData.peopleFileName));
        data.setWebpages(readWebpages(path + SocialNetworkData.webpageFileName));
        data.setFriends(readFriends(path + SocialNetworkData.friendsFileName));
        data.setLikes(readLikes(path + SocialNetworkData.likesFileName));
        return data;
    }

    private static List<Person> readPersons(String path) {
        List<Person> persons = new ArrayList<>();
        try {
            InputStream is = CSVDataReader.class.getResourceAsStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(DELIMITER);
                long id = Long.parseLong(values[0]);
                String name = values[1];
                String surname = values[2];
                boolean sex = "1".equals(values[3]);
                int age = Integer.parseInt(values[4]);
                persons.add(new Person(id, name, surname, sex, age));
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return persons;
    }

    private static List<FriendEdge> readFriends(String path) {
        List<FriendEdge> friends = new ArrayList<>();
        try {
            InputStream is = CSVDataReader.class.getResourceAsStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(DELIMITER);
                long id = Long.parseLong(values[0]);
                long p1Id = Long.parseLong(values[1]);
                long p2Id = Long.parseLong(values[2]);
                friends.add(new FriendEdge(id, p1Id, p2Id));
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return friends;
    }

    private static List<Webpage> readWebpages(String path) {
        List<Webpage> webpages = new ArrayList<>();
        try {
            InputStream is = CSVDataReader.class.getResourceAsStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(DELIMITER);
                long id = Long.parseLong(values[0]);
                String url = values[1];
                LocalDate creation = LocalDate.parse(values[2]);
                webpages.add(new Webpage(id, url, creation));
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return webpages;
    }

    private static List<LikeEdge> readLikes(String path) {
        List<LikeEdge> likes = new ArrayList<>();
        try {
            InputStream is = CSVDataReader.class.getResourceAsStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(DELIMITER);
                long id = Long.parseLong(values[0]);
                long pId = Long.parseLong(values[1]);
                long wId = Long.parseLong(values[2]);
                likes.add(new LikeEdge(id, pId, wId));
            }

            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return likes;
    }
}
