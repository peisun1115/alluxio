package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.DeleteOptions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConcurrentIOBenchmark {
  @Parameter(names = {"--readers", "-r"}, description = "Number of concurrent readers.",
      required = true)
  private int mReaders;

  @Parameter(names = {"--read-directory", "-rd"},
      description = "Directory to read files from. Ignored if number of readers is 0.",
      required = true)
  private String mReadDirectory;

  @Parameter(names = {"--read-buffer-size", "-rbs"},
      description = "Size of the buffer to use when reading. Ignored if number of readers is 0.",
      required = true)
  private int mReadBufferSize;

  @Parameter(names = {"--writers", "-w"}, description = "Number of concurrent writers.",
      required = true)
  private int mWriters;

  @Parameter(names = {"--files-to-write", "-ftw"},
      description = "Total number of files to write. Ignored if number of writers is 0.",
      required = true)
  private int mFilesToWrite;

  @Parameter(names = {"--file-size", "-fs"},
      description = "Size of the file to write. Ignored if number of writers is 0.",
      required = true)
  private int mFileSize;

  @Parameter(names = {"--write-directory", "-wd"},
      description = "Directory to write the files to. Ignored if number of writers is 0.",
      required = true)
  private String mWriteDirectory;

  @Parameter(names = {"--write-buffer-size", "-wbs"},
      description = "How large of a buffer to use when writing. Ignored if number of writers is 0.",
      required = true)
  private int mWriteBufferSize;

  @Parameter(names = {"--results-output-directory", "-rod"},
      description = "Directory to write the results to.", required = true)
  private String mResultsDirectory;

  @Parameter(names = {"--wait-on-directory", "-wod"},
      description = "The Alluxio path to wait on for synchronized starts with other processes.")
  private String mWaitOnDirectory;

  @Parameter(names = {"--processes", "-p"}, description = "Number of processes to wait on.")
  private int mProcesses;

  @Parameter(names = {"--sleep", "-s"}, description = "Time to sleep between writes.")
  private long mSleep;

  private AlluxioURI mRegistrationPath;

  private long mId;

  private String mResultOutput = "";

  /**
   * Executes the concurrent read benchmark with the user defined properties. The benchmark runs
   * the following steps in a loop 10 times.
   *     1. Setup Read/Write benchmarks, expensive preparations such as deleting or listing files
   *     are done in this step.
   *     2. Barrier to synchronize the start times of benchmarks.
   *     3. Execution of the benchmarks.
   *     4. Removal of the metadata files used to synchronize benchmarks.
   *
   * After running the benchmarks, the results are output to the user defined output directory.
   *
   * @param args user arguments
   * @throws Exception if an error occurs during the benchmark
   */
  public static void main(String[] args) throws Exception {
    // Initialize benchmark
    ConcurrentIOBenchmark bench = new ConcurrentIOBenchmark();
    new JCommander(bench, args);

    // Run 10 times to get stable results
    for (int i = 0; i < 10; i++) {
      ReadBenchmark read = bench.setupReaders();
      WriteBenchmark write = bench.setupWriters();
      // Wait for other benchmarks if necessary
      bench.register(i);
      bench.run(read, write);
      // Clean up metadata files if necessary
      bench.unregister(i);
    }

    // Write results file
    bench.writeResults();

    System.exit(0);
  }

  public ConcurrentIOBenchmark() {

  }

  private ReadBenchmark setupReaders() throws Exception {
    long filesToRead = 0;
    long bytesToRead = 0;
    List<Reader> readers = new ArrayList<>(mReaders);

    // Setup Readers
    if (mReaders > 0) {
      FileSystem readFs = FileSystem.Factory.get();
      List<URIStatus> statuses = readFs.listStatus(new AlluxioURI(mReadDirectory));
      int filesPerThread = statuses.size() / mReaders;
      List<AlluxioURI> pathsToRead = new ArrayList<>();
      for (URIStatus status : statuses) {
        bytesToRead += status.getLength();
        pathsToRead.add(new AlluxioURI(status.getPath()));
        // Got enough files for one thread to read
        if (pathsToRead.size() == filesPerThread) {
          Reader reader = new Reader(Lists.newArrayList(pathsToRead), mReadBufferSize, readFs);
          readers.add(reader);
          pathsToRead.clear();
        }
      }
      return new ReadBenchmark(bytesToRead, filesToRead, readers);
    }

    return null;
  }

  private WriteBenchmark setupWriters() throws Exception {
    List<Writer> writers = new ArrayList<>(mWriters);

    // Setup Writers
    if (mWriters > 0) {
      FileSystem writeFs = FileSystem.Factory.get();
      mWriteDirectory = normalizeDirectory(mWriteDirectory);
      // Delete the existing files first, if any
      AlluxioURI writeDir = new AlluxioURI(mWriteDirectory);
      if (writeFs.exists(writeDir)) {
        writeFs.delete(writeDir, DeleteOptions.defaults().setRecursive(true));
        Thread.sleep(mSleep);
      }
      mWriters = Math.min(mWriters, mFilesToWrite);
      int filesPerThread = mFilesToWrite / mWriters;
      for (int i = 0; i < mWriters; i++) {
        String path = mWriteDirectory.concat("writer_" + i);
        Writer writer = new Writer(path, mFileSize, filesPerThread, mWriteBufferSize, writeFs);
        writers.add(writer);
      }
      return new WriteBenchmark(writers);
    }
    return null;
  }

  private void register(int iteration) throws Exception {
    mId = 0;
    if (mWaitOnDirectory == null || mProcesses <= 1) {
      return;
    }
    mId = new Random().nextLong();
    String waitDirectory = mWaitOnDirectory + "/" + iteration;
    FileSystem fs = FileSystem.Factory.get();
    mRegistrationPath = new AlluxioURI(waitDirectory + "/" + mId);
    fs.createFile(mRegistrationPath).close();

    for (int files = fs.listStatus(new AlluxioURI(waitDirectory)).size(); files != mProcesses;
        files = fs.listStatus(new AlluxioURI(waitDirectory)).size()) {
      if (files > mProcesses) {
        System.out.println("WARNING: Number of control files greater than expected.");
      }
      Thread.sleep(100);
    }
  }

  private void run(ReadBenchmark read, WriteBenchmark write) throws Exception {
    List<Reader> readers = new ArrayList<>(mReaders);
    List<Future> readerResults = new ArrayList<>(mReaders);
    ExecutorService readerService = null;

    List<Writer> writers = new ArrayList<>(mWriters);
    List<Future> writerResults = new ArrayList<>(mWriters);
    ExecutorService writerService = null;

    if (read != null) {
      readers = read.mReaders;
      readerService = Executors.newFixedThreadPool(mReaders);
    }

    if (write != null) {
      writers = write.mWriters;
      writerService = Executors.newFixedThreadPool(mWriters);
    }

    // Launch Readers
    for (Callable reader : readers) {
      readerResults.add(readerService.submit(reader));
    }

    // Launch Writers
    for (Callable writer : writers) {
      writerResults.add(writerService.submit(writer));
    }

    // Collect reader results
    long readerTime = 0;
    long maxReaderTime = -1;
    for (Future result : readerResults) {
      long time = (long) result.get();
      readerTime += time;
      if (time > maxReaderTime) {
        maxReaderTime = time;
      }
    }

    // Collect writer results
    long writerTime = 0;
    long maxWriterTime = -1;
    for (Future result : writerResults) {
      long time = (long) result.get();
      writerTime += time;
      if (time > maxWriterTime) {
        maxWriterTime = time;
      }
    }

    // Output results
    double c = 1f / 1000f;
    StringBuilder sb = new StringBuilder();
    // Read results
    if (read != null) {
      sb.append("----------------------\n");
      sb.append("Read Results\n");
      sb.append("----------------------\n");
      sb.append(String.format("Read %d files in %fs.\n", read.mFilesToRead, c * maxReaderTime));
      sb.append(String.format("Aggregate Throughput: %f MB/s\n",
          c * read.mBytesToRead * mReaders / readerTime));
      sb.append(String
          .format("Average Throughput per Thread: %f MB/s\n", c * read.mBytesToRead / readerTime));
    } else {
      sb.append("----------------------\n");
      sb.append("Read Results\n");
      sb.append("----------------------\n");
      sb.append("No read results.\n");
    }

    // Write results
    if (write != null) {
      sb.append("----------------------\n");
      sb.append("Write Results\n");
      sb.append("----------------------\n");
      sb.append(String.format("Wrote %d files in %fs.\n", mFilesToWrite, c * maxWriterTime));
      sb.append(String.format("Aggregate Throughput: %f MB/s\n",
          c * mFilesToWrite * mFileSize * mWriters / writerTime));
      sb.append(String.format("Average Throughput per Thread: %f MB/s\n",
          c * mFilesToWrite * mFileSize / writerTime));

    } else {
      sb.append("----------------------\n");
      sb.append("Write Results\n");
      sb.append("----------------------\n");
      sb.append("No write results.\n");
    }
    mResultOutput = mResultOutput + sb.toString();
    System.out.println(sb.toString());
  }

  private void unregister(int iteration) throws Exception {
    if (mWaitOnDirectory == null || mProcesses <= 1) {
      return;
    }
    String waitDirectory = mWaitOnDirectory + "/" + iteration;
    Thread.sleep(1000);
    FileSystem fs = FileSystem.Factory.get();
    fs.delete(mRegistrationPath, DeleteOptions.defaults().setRecursive(true));

    for (int files = fs.listStatus(new AlluxioURI(waitDirectory)).size(); files != 0;
        files = fs.listStatus(new AlluxioURI(waitDirectory)).size()) {
      Thread.sleep(1000);
    }
  }

  private void writeResults() throws Exception {
    FileSystem fs = FileSystem.Factory.get();
    if (!fs.exists(new AlluxioURI(mResultsDirectory))) {
      fs.createDirectory(new AlluxioURI(mResultsDirectory));
    }
    AlluxioURI outputFile = new AlluxioURI(mResultsDirectory + "/" + mId + ".txt");
    if (fs.exists(outputFile)) {
      fs.delete(outputFile, DeleteOptions.defaults().setRecursive(true));
    }
    DataOutputStream stream =
        new DataOutputStream(fs.createFile(new AlluxioURI(mResultsDirectory + "/" + mId + ".txt")));
    stream.writeBytes(mResultOutput);
    stream.close();
  }

  private String normalizeDirectory(String dir) {
    return dir.endsWith("/") ? dir : dir + "/";
  }

  private class Reader implements Callable<Long> {
    private final FileSystem mFileSystem;
    private byte[] mBuffer;
    private final List<AlluxioURI> mPathsToRead;

    public Reader(List<AlluxioURI> paths, int bufferSize, FileSystem fs) throws Exception {
      mFileSystem = fs;
      mBuffer = new byte[bufferSize];
      mPathsToRead = paths;
    }

    @Override
    public Long call() throws Exception {
      long start = System.currentTimeMillis();
      for (AlluxioURI toRead : mPathsToRead) {
        InputStream in = mFileSystem.openFile(toRead);
        while (in.read(mBuffer) != -1) {
          ;
        }
        in.close();
      }
      return System.currentTimeMillis() - start;
    }
  }

  private class ReadBenchmark {
    private final long mBytesToRead;
    private final long mFilesToRead;
    private final List<Reader> mReaders;

    ReadBenchmark(long bytesToRead, long filesToRead, List<Reader> readers) {
      mBytesToRead = bytesToRead;
      mFilesToRead = filesToRead;
      mReaders = readers;
    }
  }

  private class Writer implements Callable<Long> {
    private final String mPath;
    private final int mFileSize;
    private final int mNumFiles;
    private final FileSystem mFileSystem;
    private final byte[] mData;
    private int filesCreated = 0;


    public Writer(String path, int fileSize, int numFiles, int bufferSize, FileSystem fs) {
      mPath = path;
      mFileSize = fileSize;
      mNumFiles = numFiles;
      mFileSystem = fs;
      mData = new byte[bufferSize];
      new Random().nextBytes(mData);
    }

    @Override
    public Long call() throws Exception {
      long start = System.currentTimeMillis();
      for (int i = 0; i < mNumFiles; i++) {
        OutputStream out =
            mFileSystem.createFile(new AlluxioURI(mPath.concat("_" + filesCreated++)));
        int bytesToWrite = mFileSize;
        while (bytesToWrite > 0) {
          out.write(mData, 0, Math.min(mData.length, bytesToWrite));
          bytesToWrite -= mData.length;
        }
        out.close();
      }
      return System.currentTimeMillis() - start;
    }
  }

  private class WriteBenchmark {
    private final List<Writer> mWriters;

    WriteBenchmark(List<Writer> writers) {
      mWriters = writers;
    }
  }
}
