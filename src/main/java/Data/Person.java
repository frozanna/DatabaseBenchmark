package Data;

public class Person {
    public long id;
    public String name;
    public String surname;
    public boolean sex;
    public int age;

    public Person() {
    }

    public Person(long id, String name, String surname, boolean sex, int age) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.sex = sex;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", sex=" + sex +
                ", age=" + age +
                '}';
    }
}
