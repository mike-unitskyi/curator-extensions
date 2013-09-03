package com.bazaarvoice.curator.recipes.leader;

import com.bazaarvoice.curator.test.ZooKeeperTest;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.Participant;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeaderServiceTest extends ZooKeeperTest {
    private static final String PATH = "/path/leader";

    private CuratorFramework _curator;

    @Before
    @Override
    public void setup() throws Exception {
        super.setup();
        _curator = newCurator();
    }

    private <T extends Service> T register(final T service) {
        closer().register(new Closeable() {
            @Override
            public void close() {
                service.stop();
            }
        });
        return service;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Tests
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /** Test starting and stopping a single instance of LeaderService. */
    @Test
    public void testLifeCycle() throws Exception {
        final Trigger started = new Trigger();
        final Trigger stopped = new Trigger();
        LeaderService leader = register(new LeaderService(
                _curator, PATH, "test-id", 1, TimeUnit.HOURS, new Supplier<Service>() {
            @Override
            public Service get() {
                return new AbstractIdleService() {
                    @Override
                    protected void startUp() throws Exception {
                        started.fire();
                    }

                    @Override
                    protected void shutDown() throws Exception {
                        stopped.fire();
                    }
                };
            }
        }));
        assertEquals("test-id", leader.getId());
        assertFalse(leader.hasLeadership());

        // Start trying to obtain leadership
        leader.start();
        assertTrue(started.firedWithin(1, TimeUnit.MINUTES));
        assertTrue(leader.isRunning());
        assertTrue(leader.hasLeadership());
        assertEquals(new Participant("test-id", true), leader.getLeader());
        assertEquals(Collections.singletonList(new Participant("test-id", true)), leader.getParticipants());
        assertFalse(stopped.hasFired());

        // Start watching ZooKeeper directly for changes
        WatchTrigger childrenTrigger = WatchTrigger.childrenTrigger();
        _curator.getChildren().usingWatcher(childrenTrigger).forPath(PATH);

        // Stop trying to obtain leadership
        leader.stop();
        assertTrue(stopped.firedWithin(1, TimeUnit.SECONDS));
        assertFalse(leader.isRunning());
        assertFalse(leader.hasLeadership());

        // Wait for stopped state to reflect in ZooKeeper then poll ZooKeeper for leadership participants state
        assertTrue(childrenTrigger.firedWithin(1, TimeUnit.SECONDS));
        assertTrue(_curator.getChildren().forPath(PATH).isEmpty());
        assertEquals(new Participant("", false), leader.getLeader());
        assertEquals(Collections.<Participant>emptyList(), leader.getParticipants());
    }

    /** Test starting multiple instances that compete for leadership. */
    @Test
    public void testMultipleLeaders() throws Exception {
        final Trigger started = new Trigger();
        final AtomicInteger startCount = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            register(new LeaderService(_curator, PATH, "test-id", 1, TimeUnit.HOURS, new Supplier<Service>() {
                @Override
                public Service get() {
                    return new AbstractIdleService() {
                        @Override
                        protected void startUp() throws Exception {
                            started.fire();
                            startCount.incrementAndGet();
                        }

                        @Override
                        protected void shutDown() throws Exception {
                            // Do nothing
                        }
                    };
                }
            })).start();
        }
        assertTrue(started.firedWithin(1, TimeUnit.MINUTES));
        // We know one service has started.  Wait a little while and verify no more services are started.
        Thread.sleep(250);
        assertTrue(startCount.get() == 1);
    }

    /** Verify that leadership is re-acquired after the connection to ZooKeeper is lost. */
    @Test
    public void testLostZooKeeperConnection() throws Exception {
        int reacquireDelayMillis = 1500;
        List<Event> events = Collections.synchronizedList(Lists.<Event>newArrayList());
        TriggeringService service1 = new EventLoggingService("1", events);
        TriggeringService service2 = new EventLoggingService("2", events);
        Supplier<Service> services = supply(service1, service2);
        Service leader = register(
                new LeaderService(_curator, PATH, "test-id", reacquireDelayMillis, TimeUnit.MILLISECONDS, services));

        leader.start();
        assertTrue(service1.getStartupTrigger().firedWithin(1, TimeUnit.MINUTES));

        killSession(_curator);
        assertTrue(service1.getShutdownTrigger().firedWithin(1, TimeUnit.MINUTES));
        assertTrue(service2.getStartupTrigger().firedWithin(1, TimeUnit.MINUTES));

        leader.stop();
        assertTrue(service2.getShutdownTrigger().firedWithin(1, TimeUnit.MINUTES));

        // Verify sequence of events.
        assertEquals(ImmutableList.of(
                new Event(Service.State.STARTING, "1"),
                new Event(Service.State.STOPPING, "1"),
                new Event(Service.State.STARTING, "2"),
                new Event(Service.State.STOPPING, "2")
        ), events);
        Event stop1 = events.get(1), start2 = events.get(2);

        // Verify that the re-acquire delay was observed
        long reacquireDelay = start2.getTimestamp() - stop1.getTimestamp();
        assertTrue("Re-acquire delay was not observed: " + reacquireDelay, reacquireDelay >= reacquireDelayMillis);
    }

    /** Verify that the name of the thread created by LeaderSelector is set correctly. */
    @Test
    public void testThreadName() throws Exception {
        final String expectedThreadName = "TestLeaderService";
        final SettableFuture<String> actualThreadName = SettableFuture.create();
        ThreadFactory threadFactory = new ThreadFactory() {
            @Override
            @SuppressWarnings("NullableProblems")
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(expectedThreadName);
                return thread;
            }
        };
        register(new LeaderService(_curator, PATH, "id", threadFactory, 1, TimeUnit.HOURS, new Supplier<Service>() {
            @Override
            public Service get() {
                return new AbstractService() {
                    @Override
                    protected void doStart() {
                        actualThreadName.set(Thread.currentThread().getName());
                        notifyStarted();
                    }

                    @Override
                    protected void doStop() {
                        notifyStopped();
                    }
                };
            }
        })).start();
        assertEquals(expectedThreadName, actualThreadName.get(1, TimeUnit.MINUTES));
    }

    private static Supplier<Service> supply(Service... services) {
        final Iterator<Service> iter = Iterators.forArray(services);
        return new Supplier<Service>() {
            @Override
            public Service get() {
                return iter.next();
            }
        };
    }

    private static class TriggeringService extends AbstractIdleService {
        private final Trigger _startupTrigger = new Trigger();
        private final Trigger _shutdownTrigger = new Trigger();

        public Trigger getStartupTrigger() {
            return _startupTrigger;
        }

        public Trigger getShutdownTrigger() {
            return _shutdownTrigger;
        }

        @Override
        protected void startUp() throws Exception {
            _startupTrigger.fire();
        }

        @Override
        protected void shutDown() throws Exception {
            _shutdownTrigger.fire();
        }
    }

    private static class EventLoggingService extends TriggeringService {
        private final String _id;
        private final List<Event> _events;

        private EventLoggingService(String id, List<Event> events) {
            _events = events;
            _id = id;
        }

        @Override
        protected void startUp() throws Exception {
            _events.add(new Event(state(), _id));
            super.startUp();
        }

        @Override
        protected void shutDown() throws Exception {
            _events.add(new Event(state(), _id));
            super.shutDown();
        }
    }

    private static class Event {
        private final Service.State _state;
        private final String _id;
        private final long _timestamp;

        private Event(Service.State state, String id) {
            _state = checkNotNull(state, "state");
            _id = checkNotNull(id, "id");
            _timestamp = System.currentTimeMillis();
        }

        public long getTimestamp() {
            return _timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Event)) {
                return false;
            }
            Event that = (Event) o;
            return _id.equals(that._id) && _state == that._state;

        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_state, _id);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("state", _state)
                    .add("id", _id)
                    .toString();
        }
    }
}
