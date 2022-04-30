public class Main {
    public static void main(String[] args){
        System.out.println("Welcome to benchmark");

        Benchmark benchmark = new Benchmark("/G500000/", "/G5000/", 10);
        benchmark.evaluate();
    }
}