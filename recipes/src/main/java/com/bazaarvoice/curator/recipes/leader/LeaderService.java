package com.bazaarvoice.curator.recipes.leader;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Starts and stops a Guava service based on whether this process is elected leader using a Curator leadership
 * election algorithm.
 * <p>
 * Because Guava services are not restartable, the <code>LeaderService</code> requires a <code>Supplier<Service></code>
 * that it will call each time leadership is acquired.  The <code>LeaderService</code> will ensure that only one
 * such <code>Service</code> instance is running at a time.
 * </p>
 * <p>
 * A typical use of <code>LeaderService</code> for a task that polls an external server might look like this:
 * <pre>
 * CuratorFramework curator = ...;
 * String serverId = new HostAndPort(InetAddress.getLocalHost().getHostAddress(), "8080").toString();
 * final Duration pollInterval = ...;
 *
 * new LeaderService(curator, "/applications/my-app/leader", serverId, 1, TimeUnit.MINUTES, new Supplier<Service>() {
 *     &#64;Override
 *     public Service get() {
 *         return new AbstractScheduledService() {
 *             &#64;Override
 *             protected void runOneIteration() throws Exception {
 *                 poll(); // TODO: implement
 *             }
 *
 *             &#64;Override
 *             protected Scheduler scheduler() {
 *                 return Scheduler.newFixedDelaySchedule(0, duration.toMillis(), TimeUnit.MILLISECONDS);
 *             }
 *         };
 *     }
 * }).start();
 * </pre>
 * </p>
 */
public class LeaderService extends AbstractIdleService {
    private static final Logger _log = LoggerFactory.getLogger(LeaderService.class);

    private final Supplier<Service> _serviceSupplier;
    private final long _reacquireDelayNanos;
    private final LeaderSelector _selector;
    private final LeaderTask _task = new LeaderTask();

    public LeaderService(CuratorFramework curator, String leaderPath, String instanceId,
                         long reacquireDelay, TimeUnit reacquireDelayUnit,
                         Supplier<Service> serviceSupplier) {
        this(curator, leaderPath, instanceId, ThreadUtils.newThreadFactory("LeaderService"),
                reacquireDelay, reacquireDelayUnit, serviceSupplier);
    }

    public LeaderService(CuratorFramework curator, String leaderPath, String instanceId, ThreadFactory threadFactory,
                         long reacquireDelay, TimeUnit reacquireDelayUnit, Supplier<Service> serviceSupplier) {
        _serviceSupplier = checkNotNull(serviceSupplier, "serviceSupplier");
        _reacquireDelayNanos = checkNotNull(reacquireDelayUnit, "reacquireDelayUnit").toNanos(reacquireDelay);
        checkArgument(_reacquireDelayNanos >= 0, "reacquireDelay must be non-negative");
        _selector = new LeaderSelector(curator, leaderPath, threadFactory, MoreExecutors.sameThreadExecutor(), _task);
        _selector.setId(checkNotNull(instanceId, "instanceId"));
    }

    /**
     * Return this instance's participant id, if provided at construction time.  This will be the value returned
     * when {@link #getParticipants()} is called.
     */
    public String getId() {
        return _selector.getId();
    }

    /**
     * Returns the set of current participants in the leader selection.
     * <p>
     * <B>NOTE</B> - this method polls the ZooKeeper server. Therefore it may return a value that does not match
     * {@link #hasLeadership()} as hasLeadership returns a cached value.
     */
    public Collection<Participant> getParticipants() throws Exception {
        return _selector.getParticipants();
    }

    /**
     * Return the id for the current leader. If for some reason there is no current leader, a dummy participant
     * is returned.
     * <p>
     * <B>NOTE</B> - this method polls the ZooKeeper server. Therefore it may return a value that does not match
     * {@link #hasLeadership()} as hasLeadership returns a cached value.
     */
    public Participant getLeader() throws Exception {
        return _selector.getLeader();
    }

    /** Return true if leadership is currently held by this instance. */
    public boolean hasLeadership() {
        return _task.hasLeadership();
    }

    @Override
    protected void startUp() throws Exception {
        _selector.autoRequeue();
        _selector.start();
    }

    @Override
    protected void shutDown() throws Exception {
        _selector.close();
        _task.close();
    }

    private class LeaderTask implements LeaderSelectorListener {
        private volatile boolean _taskActive;
        private volatile boolean _lostLeadership;

        @Override
        public void takeLeadership(CuratorFramework client) throws Exception {
            _log.debug("Leadership acquired: {}", getId());

            _taskActive = true;
            _lostLeadership = false;
            try {
                if (isRunning()) {
                    Service service = _serviceSupplier.get();
                    service.startAndWait();
                    try {
                        awaitLeadershipLost();
                    } finally {
                        service.stopAndWait();
                    }
                }
            } catch (Throwable t) {
                // Start may have failed due to a network error, we'll sleep for reacquireDelay and try again.
                _log.error("Exception starting or stopping leadership-managed process: {}", getId(), t);
            } finally {
                setTaskInactive();
            }

            if (!hasLeadership()) {
                _log.debug("Leadership lost: {}", getId());
            }

            if (!isRunning()) {
                // LeaderSelector.close() has been called or will be called shortly.
                _log.debug("Leadership released: {}", getId());
                return;
            }

            // If we lost leadership, wait a while for things to settle before trying to re-acquire leadership
            // (eg. wait for a network hiccup to the ZooKeeper server to resolve).
            sleep(_reacquireDelayNanos);

            // By returning we'll attempt to re-acquire leadership via LeaderSelector.autoRequeue().
            _log.debug("Attempting to re-acquire leadership: {}", getId());
        }

        public void close() throws InterruptedException {
            setLeadershipLost();
            awaitTaskInactive();
        }

        public boolean hasLeadership() {
            return _selector.hasLeadership() && !_lostLeadership;
        }

        private synchronized void setLeadershipLost() {
            _lostLeadership = true;
            notifyAll();
        }

        private synchronized void awaitLeadershipLost() throws InterruptedException {
            while (hasLeadership() && isRunning()) {
                wait();
            }
        }

        private synchronized void setTaskInactive() {
            _taskActive = false;
            notifyAll();
        }

        /** Sleeps while the nested managed service is started. */
        private synchronized void awaitTaskInactive() throws InterruptedException {
            while (_taskActive) {
                wait();
            }
        }

        /** Sleeps for the specified amount of time or until the LeaderService is stopped, whichever comes first. */
        private synchronized void sleep(long waitNanos) throws InterruptedException {
            while (waitNanos > 0 && isRunning()) {
                long start = System.nanoTime();
                TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
                waitNanos -= System.nanoTime() - start;
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
                _log.debug("Lost leadership due to ZK state change to {}: {}", newState, getId());
                setLeadershipLost();
            }
        }
    }
}
