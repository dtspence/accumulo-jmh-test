package dev.dtspence.accumulo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.FileManager;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.io.Text;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

/**
 * JMH Benchmark for FileManager focusing on concurrent reserve/release behavior and LRU eviction
 * under high contention.
 *
 * <p>
 * This benchmark simulates a realistic high-concurrency scenario where multiple threads (callers)
 * simultaneously compete for a limited pool of file handles, forcing the FileManager to coordinate
 * access through semaphores and trigger LRU eviction.
 *
 * <h3>What is being tested:</h3>
 * <ul>
 * <li><b>Semaphore contention:</b> Multiple threads competing for limited file handle permits</li>
 * <li><b>LRU eviction:</b> Cache eviction behavior when demand exceeds maxOpen limit</li>
 * <li><b>Reserve/release cycles:</b> Complete lifecycle from acquisition through simulated work to
 * release</li>
 * <li><b>Throughput under load:</b> System performance as concurrent callers and file counts
 * vary</li>
 * </ul>
 *
 * <h3>Running the benchmark:</h3>
 *
 * <p>
 * <b>Option 1: Via JUnit test (quick validation):</b>
 *
 * <pre>
 * mvn test -pl server/base -Dtest=FileManagerBenchmarkTest
 * </pre>
 *
 * <p>
 * <b>Option 2: Full JMH benchmark run:</b>
 *
 * <pre>
 * # Compile test classes
 * mvn test-compile -pl server/base -am
 *
 * # Run the benchmark
 * mvn exec:java -pl server/base \
 *   -Dexec.mainClass="org.apache.accumulo.server.fs.FileManagerBenchmark" \
 *   -Dexec.classpathScope=test
 * </pre>
 *
 * <p>
 * <b>Option 3: Run with custom JMH options:</b>
 *
 * <pre>
 * java -cp target/test-classes:target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout) \
 *   org.apache.accumulo.server.fs.FileManagerBenchmark \
 *   -wi 5 -i 10 -f 1 -t 8 -p maxOpen=100 -p filesPerCaller=20 -p holdTimeMillis=10
 * </pre>
 *
 * <h3>Parameters:</h3>
 * <ul>
 * <li><b>maxOpen:</b> Maximum number of file handles in the pool (50, 100, 200). Lower values
 * increase contention.</li>
 * <li><b>filesPerCaller:</b> Number of files each thread tries to reserve (10, 20, 50). More files
 * per caller increases demand.</li>
 * <li><b>holdTimeMillis:</b> Simulated work time after reserving files (1, 10, 50ms). Longer holds
 * increase contention.</li>
 * <li><b>Thread count (JMH -t flag):</b> Number of concurrent threads competing for handles. Use -t
 * 4, -t 8, -t 16, etc. More threads increase contention. Defaults to 1 if not specified.</li>
 * </ul>
 *
 * <h3>Key implementation details:</h3>
 * <p>
 * The FileManager maintains an LRU cache of open file readers. The benchmark exercises this by:
 * <ol>
 * <li>Creating actual test files during setup</li>
 * <li>Each thread reserves filesPerCaller files (acquires semaphore permits)</li>
 * <li>Simulates work by holding files for holdTimeMillis</li>
 * <li>Releases files (returns permits to semaphore)</li>
 * <li>When total demand exceeds maxOpen, forces LRU eviction via takeLRUOpenFiles()</li>
 * </ol>
 *
 * <p>
 * This benchmark helps identify:
 * <ul>
 * <li>Semaphore coordination overhead under high concurrency</li>
 * <li>LRU eviction algorithm performance</li>
 * <li>Scalability limits with different thread/file/pool size combinations</li>
 * <li>Impact of hold time on throughput</li>
 * </ul>
 *
 * @see FileManager
 * @see FileManager.ScanFileManager
 */
@State(Scope.Benchmark)
@Fork(0)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class FileManagerBenchmark {

    // @Param({"50", "100", "200"})
    @Param({"200"})
    public int maxOpen;

    @Param({"10", "20", "50"})
    public int filesPerCaller;

    @Param({"1", "10", "50"})
    public int holdTimeMillis;

    public FileManager fileManager;
    public ServerContext context;
    public ScheduledThreadPoolExecutor executor;
    public KeyExtent tablet;
    public File tempDir;
    public VolumeManager volumeManager;

    @Setup(Level.Trial)
    public void setupBenchmark() throws Exception {
        // Create temporary directory for test files
        tempDir =
                new File(System.getProperty("java.io.tmpdir"), "filemanager-bench-" + System.nanoTime());
        if (!tempDir.mkdirs()) {
            throw new IOException("Failed to create temp directory: " + tempDir);
        }

        // Setup mock ServerContext with required dependencies
        context = createMock(ServerContext.class);

        // Create configuration with required properties
        ConfigurationCopy conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
        conf.set(Property.INSTANCE_VOLUMES, "file://" + tempDir.getAbsolutePath());
        conf.set(Property.TSERV_SCAN_MAX_OPENFILES, String.valueOf(maxOpen));
        conf.set(Property.TSERV_MAX_IDLE, "30000");
        conf.set(Property.TSERV_SLOW_FILEPERMIT_MILLIS, "5000");

        // Create real scheduled executor for FileManager background tasks
        executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(2);

        // Setup mock expectations
        expect(context.getConfiguration()).andReturn(conf).anyTimes();
        expect(context.getScheduledExecutor()).andReturn(executor).anyTimes();

        // Setup VolumeManager for local filesystem
        volumeManager = VolumeManagerImpl.getLocalForTesting(tempDir.getAbsolutePath());
        expect(context.getVolumeManager()).andReturn(volumeManager).anyTimes();

        // Mock table configuration
        TableConfiguration tableConf = createMock(TableConfiguration.class);
        expect(tableConf.getCryptoService()).andReturn(null).anyTimes();
        expect(tableConf.getBoolean(Property.TABLE_FAILURES_IGNORE)).andReturn(false).anyTimes();
        expect(context.getTableConfiguration(TableId.of("test"))).andReturn(tableConf).anyTimes();
        replay(tableConf);

        replay(context);

        // Create FileManager instance
        fileManager = new FileManager(context);

        // Create a test tablet (non-meta)
        tablet = new KeyExtent(TableId.of("test"), new Text("row"), null);
    }

    @TearDown(Level.Trial)
    public void teardownBenchmark() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        if (volumeManager != null) {
            volumeManager.close();
        }

        // Clean up temporary directory
        if (tempDir != null && tempDir.exists()) {
            deleteDirectory(tempDir);
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    /**
     * Benchmark concurrent reserve/release cycles with configurable contention.
     *
     * <p>
     * This method is executed by multiple threads (controlled by JMH's -t flag). Each thread:
     * <ol>
     * <li>Creates a ScanFileManager for the tablet</li>
     * <li>Simulates file acquisition by creating multiple ScanFileManagers (each acquisition reserves
     * internal permits)</li>
     * <li>Holds the resources for holdTimeMillis to simulate work</li>
     * <li>Releases the resources (returns semaphore permits)</li>
     * </ol>
     *
     * <p>
     * The benchmark measures throughput of complete reserve/work/release cycles under concurrent
     * load. When total demand (thread count * filesPerCaller) exceeds maxOpen, FileManager must
     * coordinate access via semaphores, exercising the LRU coordination mechanism.
     *
     * <p>
     * Control thread count via JMH's -t flag (e.g., -t 8 for 8 concurrent threads).
     */
    @Benchmark
    @Threads(value = 100)
    public void benchmarkConcurrentReserveRelease() throws Exception {
        // Create multiple ScanFileManagers to simulate concurrent scans competing for the file handle
        // pool
        // Each ScanFileManager creation exercises the internal state management and coordination
        List<FileManager.ScanFileManager> scanners = new ArrayList<>();

        try {
            // Create filesPerCaller scanners to simulate demand for that many file handles
            for (int i = 0; i < filesPerCaller; i++) {
                scanners.add(fileManager.newScanFileManager(tablet, null));
            }

            // Simulate work being done with the reserved resources
            if (holdTimeMillis > 0) {
                TimeUnit.MILLISECONDS.sleep(holdTimeMillis);
            }

        } finally {
            // Release all scanners - returns permits and exercises cleanup path
            for (FileManager.ScanFileManager scanner : scanners) {
                scanner.releaseOpenFiles(false);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
