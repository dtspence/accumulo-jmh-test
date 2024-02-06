package dev.dtspence.accumulo.file;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class DisruptorSKVWriter implements FileSKVWriter {
    private final static long NO_MEMORY_LIMIT = -1;
    private final FileSKVWriter wrappedWriter;
    private final long memoryLimit;
    private DisruptorTask task;

    public DisruptorSKVWriter(final FileSKVWriter wrappedWriter, long memoryLimit) {
        this.wrappedWriter = wrappedWriter;
        this.memoryLimit = memoryLimit;
    }

    @Override
    public boolean supportsLocalityGroups() {
        return wrappedWriter.supportsLocalityGroups();
    }

    @Override
    public void startNewLocalityGroup(final String name, final Set<ByteSequence> columnFamilies) throws IOException {
        if (task == null) {
            task = DisruptorTask.newTask(wrappedWriter, memoryLimit);
            task.start();
        }

        task.pushNewLocalityGroup(name, columnFamilies);
    }

    @Override
    public void startDefaultLocalityGroup() {
        if (task == null) {
            task = DisruptorTask.newTask(wrappedWriter, memoryLimit);
            task.start();
        }

        task.pushDefaultLocalityGroup();
    }

    @Override
    public void append(final Key key, final Value value) {
        Preconditions.checkNotNull(task);
        task.pushKv(key, value);
    }

    @Override
    public DataOutputStream createMetaStore(final String name) throws IOException {
        return wrappedWriter.createMetaStore(name);
    }

    @Override
    public void close() throws IOException {
        if (task != null) {
            task.stop();
        }
        wrappedWriter.close();
    }

    @Override
    public long getLength() throws IOException {
        return wrappedWriter.getLength();
    }

    public long getMemoryExceededCount() {
        return task.getMemoryExceededCount();
    }

    static class DisruptorTask {
        private final Disruptor<KeyValueEvent> disruptor;
        private final RingBuffer<KeyValueEvent> rb;
        private final AtomicLong ringBytes;
        private final AtomicLong ringMemoryExceeded;
        private final long ringLimitBytes;

        private DisruptorTask(final Disruptor<KeyValueEvent> disruptor, final AtomicLong ringBytes, final long memoryLimit) {
            this.disruptor = disruptor;
            this.rb = disruptor.getRingBuffer();
            this.ringBytes = ringBytes;
            this.ringMemoryExceeded = new AtomicLong();
            this.ringLimitBytes = memoryLimit;
        }

        static DisruptorTask newTask(final FileSKVWriter wrappedWriter, final long memoryLimit) {
            final var bufferSize = 1024;
            final var threadFactory = new ThreadFactoryBuilder().build();
            final var waitStrategy = new BusySpinWaitStrategy();
            final var factory = new KeyValueEventFactory();
            final var ringBytes = new AtomicLong();
            final var disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.SINGLE, waitStrategy);

            disruptor.handleEventsWith(new KeyValueEventHandler(wrappedWriter, ringBytes));

            return new DisruptorTask(disruptor, ringBytes, memoryLimit);
        }

        static class KeyValueEvent {
            Key key;
            Value value;
            boolean lgDefaultNew;
            boolean lgLocalNew;
            String lgName;
            Set<ByteSequence> lgCf;
            long keyValueSize;

            private KeyValueEvent() {
                key = new Key();
                value = new Value();
                lgCf = new HashSet<>();
            }

            void reset() {
                lgDefaultNew = false;
                lgLocalNew = false;
                lgName = null;
                lgCf.clear();
                keyValueSize = 0;
            }
        }

        static class KeyValueEventFactory implements EventFactory<KeyValueEvent> {
            @Override
            public KeyValueEvent newInstance() {
                return new KeyValueEvent();
            }
        }

        static class KeyValueEventHandler implements EventHandler<KeyValueEvent> {
            private final FileSKVWriter wrappedWriter;
            private final AtomicLong ringBytes;

            KeyValueEventHandler(final FileSKVWriter wrappedWriter, final AtomicLong ringBytes) {
                this.wrappedWriter = wrappedWriter;
                this.ringBytes = ringBytes;
            }

            @Override
            public void onEvent(final KeyValueEvent ev, final long l, final boolean b) throws Exception {
                if (ev.lgDefaultNew) {
                    wrappedWriter.startDefaultLocalityGroup();
                    return;
                } else if (ev.lgLocalNew) {
                    wrappedWriter.startNewLocalityGroup(ev.lgName, ev.lgCf);
                    return;
                }
                wrappedWriter.append(ev.key, ev.value);
                ringBytes.addAndGet(ev.keyValueSize * -1);
            }
        }

        void pushKv(final Key key, final Value value) {
            if (ringLimitBytes > 0) {
                var spins = false;
                while (ringBytes.get() > ringLimitBytes) {
                    spins = true;
                    Thread.yield();
                }
                if (spins) {
                    ringMemoryExceeded.incrementAndGet();
                }
            }
            final var seq = rb.next();
            final var ev = rb.get(seq);
            ev.reset();
            ev.key.set(key);
            ev.value.set(value.get());
            ev.keyValueSize = key.getSize() + value.getSize();
            ringBytes.addAndGet(ev.keyValueSize);
            rb.publish(seq);
        }

        void pushNewLocalityGroup(final String name, final Set<ByteSequence> columnFamilies) {
            final var seq = rb.next();
            final var ev = rb.get(seq);
            ev.reset();
            ev.lgLocalNew = true;
            ev.lgName = name;
            ev.lgCf.addAll(columnFamilies);
            rb.publish(seq);
        }

        void pushDefaultLocalityGroup() {
            final var seq = rb.next();
            final var ev = rb.get(seq);
            ev.reset();
            ev.lgDefaultNew = true;
            rb.publish(seq);
        }

        void start() {
            Preconditions.checkState(!disruptor.hasStarted());
            disruptor.start();
        }

        void stop() {
            disruptor.shutdown();
        }

        long getMemoryExceededCount() {
            return ringMemoryExceeded.get();
        }
    }
}
