package dev.dtspence.accumulo.file;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventPoller;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lmax.disruptor.EventPoller.PollState.GATING;
import static com.lmax.disruptor.EventPoller.PollState.IDLE;
import static com.lmax.disruptor.dsl.ProducerType.SINGLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DisruptorSKVIterator implements FileSKVIterator {
    private final FileSKVIterator wrappedIterator;
    private DisruptorTask disruptorTask;
    private PollContext pollContext;
    private AtomicBoolean interruptedFlag;

    private SeekCorrelation seekCorrelation;
    private KeyValueEvent kv;

    public DisruptorSKVIterator(final FileSKVIterator iterator) {
        this.wrappedIterator = iterator;
    }

    @Override
    public Key getFirstKey() throws IOException {
        return wrappedIterator.getFirstKey();
    }

    @Override
    public Key getLastKey() throws IOException {
        return wrappedIterator.getLastKey();
    }

    @Override
    public DataInputStream getMetaStore(final String name) throws IOException, NoSuchMetaStoreException {
        return wrappedIterator.getMetaStore(name);
    }

    @Override
    public FileSKVIterator getSample(final SamplerConfigurationImpl sampleConfig) {
        return wrappedIterator.getSample(sampleConfig);
    }

    @Override
    public void closeDeepCopies() throws IOException {
        wrappedIterator.closeDeepCopies();
    }

    @Override
    public void setCacheProvider(final CacheProvider cacheProvider) {
        wrappedIterator.setCacheProvider(cacheProvider);
    }

    @Override
    public void close() throws IOException {
        wrappedIterator.close();
        if (disruptorTask != null) {
            disruptorTask.stop();
            try {
                disruptorTask.awaitTermination(10000L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void setInterruptFlag(final AtomicBoolean flag) {
        wrappedIterator.setInterruptFlag(flag);
        interruptedFlag = flag;
    }

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
//        wrappedIterator.init(source, options, env);
//        disruptorTask = new DisruptorTask(wrappedIterator, interruptedFlag);
//        pollContext = new PollContext(disruptorTask.newEventPoller());
//        disruptorTask.start();
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasTop() {
        return kv.top;
    }

    @Override
    public void next() throws IOException {
        final var ev = pollContext.pollEvents(seekCorrelation);
        if (ev.exception != null) {
            throw new IOException(ev.exception);
        }
        setKeyValueEvent(ev);
    }

    @Override
    public void seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive) throws IOException {
        ensureDisruptorReady();
        seekCorrelation = disruptorTask.seek(range, columnFamilies, inclusive);
        final var ev = pollContext.pollEvents(seekCorrelation);
        if (ev.exception != null) {
            throw new IOException(ev.exception);
        }
        setKeyValueEvent(ev);
    }

    @Override
    public Key getTopKey() {
        return kv.key;
    }

    @Override
    public Value getTopValue() {
        return kv.value;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(final IteratorEnvironment env) {
        return new DisruptorSKVIterator((FileSKVIterator) wrappedIterator.deepCopy(env));
    }

    private void ensureDisruptorReady() {
        if (disruptorTask != null) {
            return;
        }
        disruptorTask = new DisruptorTask(wrappedIterator, interruptedFlag);
        disruptorTask.start();
        pollContext = new PollContext(disruptorTask.newEventPoller());
    }

    private void setKeyValueEvent(final KeyValueEvent ev) {
        kv = ev;
    }

    static class KeyValueEvent {
        SeekCorrelation seekAction;
        Exception exception;
        boolean top;
        Key key;
        Value value;

        KeyValueEvent() {
            key = new Key();
            value = new Value();
        }

        void reset() {
            exception = null;
        }

        void setEmpty(final SeekCorrelation seekAction) {
            reset();
            this.seekAction = seekAction;
            this.top = false;
        }

        void setKeyValue(final SeekCorrelation seekAction, final Key key, final Value value) {
            reset();
            this.seekAction = seekAction;
            this.top = true;
            this.key.set(key);
            this.value.set(value.get());
        }

        void setException(final SeekCorrelation seekAction, final Exception ex) {
            reset();
            this.seekAction = seekAction;
            this.exception = ex;
        }
    }

    static class PollContext {
        private final EventPoller<KeyValueEvent> eventPoller;
        private volatile KeyValueEvent event;
        private volatile SeekCorrelation seekCurrent;
        private long eventsWaitTotal;
        private long eventsWaitNs;

        PollContext(final EventPoller<KeyValueEvent> eventPoller) {
            this.eventPoller = eventPoller;
        }

        KeyValueEvent pollEvents(final SeekCorrelation correlation) throws IOException {
            seekCurrent = correlation;
            try {
                event = null;
                var pollState = eventPoller.poll(this::handleEvent);
                var ns = 0L;
                var nsTime = false;
                if (event != null) {
                    ns = System.nanoTime();
                    nsTime = true;
                }
                while (pollState == IDLE || pollState == GATING) {
                    // TODO: follow-up
                    //Thread.yield();
                    pollState = eventPoller.poll(this::handleEvent);
                }
                if (event == null) {
                    throw new IllegalStateException("Expected event to be set from poller");
                }
                if (nsTime) {
                    ns = System.nanoTime() - ns;
                    eventsWaitNs += ns;
                    eventsWaitTotal++;
                }
            } catch (final Exception e) {
                throw new IOException(e);
            }
            return event;
        }

        boolean handleEvent(final KeyValueEvent ev, final long seq, final boolean endOfBatch) {
            // seek correlation does not match
            // return and wait for correlation which matches
            if (ev.seekAction != seekCurrent) {
                return true;
            }
            event = ev;
            return false;
        }
    }

    static class KeyValueEventFactory implements EventFactory<KeyValueEvent> {

        @Override
        public KeyValueEvent newInstance() {
            return new KeyValueEvent();
        }
    }

    static class DisruptorTask {
        private final AtomicBoolean interruptedFlag;
        private final FileSKVIterator wrappedIterator;

        private ExecutorService readExecutor;
        private Disruptor<KeyValueEvent> disruptor;
        private RingBuffer<KeyValueEvent> rb;

        DisruptorTask(final FileSKVIterator iterator, final AtomicBoolean interruptedFlag) {
            this.wrappedIterator = iterator;
            this.interruptedFlag = interruptedFlag;
        }

        EventPoller<KeyValueEvent> newEventPoller() {
            throwIfNotStarted();
            return rb.newPoller();
        }

        public void start() {
            final var eventFactory = new KeyValueEventFactory();
            final var bufferSize = 1024;
            final var readThreadFactory = new ThreadFactoryBuilder().setNameFormat("FileKVIterator-Read-%d").build();
            final var rbThreadFactory = new ThreadFactoryBuilder().setNameFormat("FileKVIterator-Ring-%d").build();
            final var waitStrategy = new BusySpinWaitStrategy();
            readExecutor = Executors.newSingleThreadExecutor(readThreadFactory);
            disruptor = new Disruptor<>(eventFactory, bufferSize, rbThreadFactory, SINGLE, waitStrategy);
            rb = disruptor.getRingBuffer();
            disruptor.start();
        }

        public void stop() {
            disruptor.shutdown();
            readExecutor.shutdownNow();
        }

        public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
            return readExecutor.awaitTermination(timeoutMillis, MILLISECONDS);
        }

        SeekCorrelation seek(final Range range, final Collection<ByteSequence> columnFamilies, final boolean inclusive) {
            // create new seek action
            // the consumers will look for a specific seek
            final var seekAction = new SeekCorrelation(range, columnFamilies, inclusive);

            // enqueue the seek action
            readExecutor.submit(new KeyValueProducer(this, seekAction));

            return seekAction;
        }

        private void throwIfNotStarted() {
            if (disruptor == null) {
                throw new IllegalStateException("Disruptor has not been started");
            }
        }
    }

    static class KeyValueProducer implements Runnable {
        private final static Logger LOG = LoggerFactory.getLogger(KeyValueProducer.class);

        private final SeekCorrelation seekAction;
        private final AtomicBoolean interruptedFlag;
        private final FileSKVIterator wrappedIterator;
        private final RingBuffer<KeyValueEvent> rb;
        private boolean seeked;

        KeyValueProducer(final DisruptorTask task, final SeekCorrelation seekAction) {
            this.seekAction = seekAction;
            this.interruptedFlag = task.interruptedFlag;
            this.wrappedIterator = task.wrappedIterator;
            this.rb = task.rb;
        }

        @Override
        public void run() {
            var threadInterrupt = false;
            while (!seeked || wrappedIterator.hasTop()) {
//                if (Thread.currentThread().isInterrupted()) {
//                    threadInterrupt = true;
//                    break;
//                }
                try {
                    if (!seeked) {
                        wrappedIterator.seek(seekAction.range, seekAction.columnFamilies, seekAction.inclusive);
                        seeked = true;
                        continue;
                    }
                    final var key = wrappedIterator.getTopKey();
                    final var value = wrappedIterator.getTopValue();
                    // rb.publishEvent((ev, s, c, k, v) -> ev.setKeyValue(c, k, v), seekAction, key, value);
                    var seq = rb.next();
                    var ev = rb.get(seq);
                    ev.setKeyValue(seekAction, key, value);
                    rb.publish(seq);
                    wrappedIterator.next();
                } catch (final InterruptedIOException e) {
                    LOG.trace("Interrupted exception", e);
                    threadInterrupt = true;
                    break;
                } catch (final IOException e) {
                    LOG.debug("Exception encountered during seek/iteration", e);
                    rb.publishEvent((ev, s, c, ex) -> ev.setException(c, ex), seekAction, e);
                    break;
                }
            }
            rb.publishEvent((ev, s, c) -> ev.setEmpty(c), seekAction);
            LOG.trace("Exiting seek/iteration [interrupted: {}, thread-interrupt: {}]", interruptedFlag.get(), threadInterrupt);
        }
    }

    static class SeekCorrelation {
        final Range range;
        final Collection<ByteSequence> columnFamilies;
        final boolean inclusive;

        SeekCorrelation(final Range range, Collection<ByteSequence> columnFamilies, final boolean inclusive) {
            this.range = range;
            this.columnFamilies = columnFamilies;
            this.inclusive = inclusive;
        }
    }
}
