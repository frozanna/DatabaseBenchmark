public class Main {
    public static void main(String[] args){
        System.out.println("Welcome to benchmark");

        Benchmark benchmark = new Benchmark("/G100000/", "/G1000/", 10);
        benchmark.evaluate();
    }
}