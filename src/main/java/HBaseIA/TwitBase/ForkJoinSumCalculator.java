package HBaseIA.TwitBase;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static java.lang.Thread.currentThread;
import static java.util.Arrays.copyOfRange;

@SuppressWarnings("WeakerAccess")
public class ForkJoinSumCalculator extends RecursiveTask<Long> {

    private final long[] numbers;

    private final int start;
    private final int end;

    public static final long THRESHOLD = 1000;

    public ForkJoinSumCalculator(long[] numbers) {
        this(numbers, 0, numbers.length);
    }

    private ForkJoinSumCalculator(long[] numbers, int start, int end) {

        this.numbers = numbers;
        this.start = start;
        this.end = end;
    }

    private static String firstWorkerName;
    private static AtomicInteger invokeCount = new AtomicInteger();
    private static Map<String, AtomicInteger> workerCount = new ConcurrentHashMap<>();

    @Override
    protected Long compute() {


        if (end == 100_000_000 + 1 && start == 0) {
            firstWorkerName = currentThread().getName();
        }
        if (currentThread().getName().equals(firstWorkerName)) {
            invokeCount.getAndIncrement();
        }

        // 工作线程调用计次
        workerCount.computeIfAbsent(currentThread().getName(), key -> new AtomicInteger())
                .incrementAndGet();

        int length = end - start;
        if (length < THRESHOLD) {
            return computeSequentially();
        }

//        ForkJoinSumCalculator leftTask =
//                new ForkJoinSumCalculator(numbers, start, start + length/2);
//        leftTask.fork();
//        ForkJoinSumCalculator rightTask =
//                new ForkJoinSumCalculator(numbers, start + length/2, end);
//        Long rightResult =  rightTask.compute();
//        Long leftResult = leftTask.join();

        ForkJoinSumCalculator leftTask =
                new ForkJoinSumCalculator(numbers, start, start + length / 2);
        ForkJoinSumCalculator rightTask =
                new ForkJoinSumCalculator(numbers, start + length / 2, end);

        //invokeAll(leftTask, rightTask);
        leftTask.fork();
        rightTask.fork();
        Long leftResult = leftTask.join();
        Long rightResult = rightTask.join();
        return rightResult + leftResult;
    }

    private long computeSequentially() {

        return LongStream.of(numbers).sum();
    }

    private static final ForkJoinPool pool = new ForkJoinPool();

    public static void main(String[] args) {

//        long[] numbers = LongStream.rangeClosed(0, 100_000_000).toArray();
//        ForkJoinSumCalculator forkJoinSumCalculator = new ForkJoinSumCalculator(numbers);
//        pool.invoke(forkJoinSumCalculator);
//        String joiner = Joiner.on("\n").withKeyValueSeparator("=").join(workerCount);
//        System.out.println(joiner);
//
//        System.out.println("**********");
//        System.out.println(firstWorkerName);

        long[] array = new long[]{3, 60, 61, 67, 68, 1, 2, 6, 7, 8, 9, 10};

//        int lo = 0,hi = array.length - 1;
//
//        int mid = lo + hi >>> 1;
//
//        long[] buf = Arrays.copyOfRange(array, lo, mid);
//
//        for (int i = 0, j = lo, k = mid; i < buf.length; j++) {
//
//            array[j] = (k == hi || buf[i] < array[k])
//                    ? buf[i++]
//                    : array[k++];
//        }
//
//        System.out.println(Arrays.toString(array));

        Arrays.sort(array, 0, array.length - 1);

        System.out.println(Arrays.toString(array));

        System.out.println(Arrays.toString(copyOfRange(array, 1, 2)));

    }
}

interface Person<T> {
    T action();
}

class Student implements Person<String> {

    @Override
    public String action() {
        return null;
    }
}

class Farmer implements Person {

    @Override
    public Object action() {
        return null;
    }
}

class Worker implements Person<Void> {

    @Override
    public Void action() {

        return null;
    }

    static class SortTask extends RecursiveAction {
        final long[] array;
        final int lo, hi;

        static final int THRESHOLD = 1000;

        SortTask(long[] array, int lo, int hi) {
            this.array = array;
            this.lo = lo;
            this.hi = hi;
        }

        SortTask(long[] array) {
            this(array, 0, array.length);
        }

        protected void compute() {
            if (hi - lo < THRESHOLD)
                sortSequentially(lo, hi);
            else {
                int mid = (lo + hi) >>> 1;//>>> 无符号右移;
                invokeAll(new SortTask(array, lo, mid), new SortTask(array, mid, hi));

                merge(lo, mid, hi);
            }
        }

        // implementation details follow:
        void sortSequentially(int lo, int hi) {
            Arrays.sort(array, lo, hi);
        }

        void merge(int lo, int mid, int hi) {

            long[] buf = copyOfRange(array, lo, mid);

            for (int i = 0, j = lo, k = mid; i < buf.length; j++) {

                array[j] = (k == hi || buf[i] < array[k])
                        ? buf[i++]
                        : array[k++];
            }
        }
    }
}