package com.bazaarvoice.curator.recipes.leader;

import com.google.common.base.Optional;
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
import static com.google.common.base.Preconditions.checkState;

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

    /**
     * Returns the wrapped service if this instance currently owns the leadership lock, {@link Optional#absent()}
     * otherwise.
     */
    public Optional<Service> getDelegateService() {
        return _task.getDelegate();
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
        private volatile Service _delegate;
        private volatile boolean _delegateActive;
        private volatile boolean _lostLeadership;

        @Override
        public void takeLeadership(CuratorFramework client) {
            try {
                // Check for races between leadership mutex acquisition and Curator cxn loss, service shutdown.
                if (!hasLeadership() || !isRunning()) {
                    _log.debug("Immediately abandoning leadership, state={}, hasLeadership={}: {}",
                            state(), hasLeadership(), getId());
                    return;
                }

                _log.debug("Leadership acquired: {}", getId());

                // Start the delegate service and let it do its thing until (a) we lose the leadership mutex
                // or (b) the LeaderService is stopped or (c) the delegate service is stopped.
                _delegateActive = true;
                try {
                    Service delegate = _delegate = listenTo(_serviceSupplier.get());
                    delegate.startAndWait();
                    try {
                        awaitLeadershipLostOrServicesStopped(delegate);
                    } catch (InterruptedException ie) {
                        // LeaderSelector is being closed.  We will release leadership.
                    } finally {
                        delegate.stopAndWait();
                    }
                } catch (Throwable t) {
                    // Start may have failed due to a network error, we'll sleep for reacquireDelay and try again.
                    _log.error("Exception starting or stopping leadership-managed service: {}", getId(), t);
                } finally {
                    notifyDelegateStopped();
                }

                if (!hasLeadership()) {
                    _log.debug("Leadership lost: {}", getId());
                } else if (isRunning()) {
                    _log.debug("Leadership released: {}", getId());
                }

                if (!isRunning()) {
                    // LeaderSelector.close() has been called or will be called shortly.
                    _log.debug("Leadership released, stopping: {}", getId());
                    return;
                }

                // If we lost leadership, wait a while for things to settle before trying to re-acquire leadership
                // (eg. wait for a network hiccup to the ZooKeeper server to resolve).
                try {
                    sleep(_reacquireDelayNanos);
                } catch (InterruptedException e) {
                    // LeaderSelector is being closed.  We will release leadership.
                    return;
                }

                // By returning we'll attempt to re-acquire leadership via LeaderSelector.autoRequeue().
                _log.debug("Attempting to re-acquire leadership: {}", getId());

            } finally {
                // Reset the lostLeadership flag just before Curator attempts to re-acquire the leadership mutex.
                _lostLeadership = false;
            }
        }

        public Optional<Service> getDelegate() {
            return Optional.fromNullable(_delegate);
        }

        public void close() throws InterruptedException {
            // Wake up the task if it's currently waiting for leadership to be lost, then wait for the task to
            // ack back that it has stopped the delegate service.
            checkState(!isRunning());
            notifyServiceStopped();
            awaitDelegateStopped();
        }

        public boolean hasLeadership() {
            return _selector.hasLeadership() && !_lostLeadership;
        }

        private synchronized void notifyLeadershipLost() {
            _lostLeadership = true;
            notifyAll();
        }

        /** Wake up the task thread since either the parent service or the delegate service has stopped. */
        private synchronized void notifyServiceStopped() {
            notifyAll();
        }

        /** Wait until we loose leadership or this service is stopped or the delegate service has stopped. */
        private synchronized void awaitLeadershipLostOrServicesStopped(Service delegate) throws InterruptedException {
            while (hasLeadership() && isRunning() && delegate.isRunning()) {
                wait();
            }
        }

        private synchronized void notifyDelegateStopped() {
            _delegate = null;
            _delegateActive = false;
            notifyAll();
        }

        /** Sleeps while the delegate service may be active. */
        private synchronized void awaitDelegateStopped() throws InterruptedException {
            while (_delegateActive) {
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
                notifyLeadershipLost();
            }
        }

        /** Release leadership when the service terminates (normally or abnormally). */
        private Service listenTo(Service delegate) {
            delegate.addListener(new Listener() {
                @Override
                public void starting() {
                    // Do nothing
                }

                @Override
                public void running() {
                    // Do nothing
                }

                @Override
                public void stopping(State from) {
                    // Do nothing
                }

                @Override
                public void terminated(State from) {
                    notifyDelegateStopped();
                }

                @Override
                public void failed(State from, Throwable failure) {
                    notifyDelegateStopped();
                }
            }, MoreExecutors.sameThreadExecutor());
            return delegate;
        }
    }
}
