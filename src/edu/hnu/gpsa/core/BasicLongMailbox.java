package edu.hnu.gpsa.core;


import java.util.LinkedList;
import java.util.TimerTask;

import kilim.Event;
import kilim.EventPublisher;
import kilim.EventSubscriber;
import kilim.Pausable;
import kilim.PauseReason;
import kilim.Task;

/**
 * This is a typed buffer that supports multiple producers and a single
 * consumer. It is the basic construct used for tasks to interact and
 * synchronize with each other (as opposed to direct java calls or static member
 * variables). put() and get() are the two essential functions.
 * 
 * We use the term "block" to mean thread block, and "pause" to mean
 * fiber pausing. The suffix "nb" on some methods (such as getnb())
 * stands for non-blocking. Both put() and get() have blocking and
 * non-blocking variants in the form of putb(), putnb
 */

public class BasicLongMailbox implements PauseReason, EventPublisher {
    // TODO. Give mbox a config name and id and make monitorable
    long[] msgs;
    private int iprod = 0; // producer index
    private int icons = 0; // consumer index;
    private int numMsgs = 0;
    private int maxMsgs = 300;
    EventSubscriber sink;
    
    // FIX: I don't like this event design. The only good thing is that
    // we don't create new event objects every time we signal a client
    // (subscriber) that's blocked on this mailbox.
    public static final int SPACE_AVAILABLE = 1;
    public static final int MSG_AVAILABLE = 2;
    public static final int TIMED_OUT = 3;
    public static final Event spaceAvailble = new Event(MSG_AVAILABLE);
    public static final Event messageAvailable = new Event(SPACE_AVAILABLE);
    public static final Event timedOut = new Event(TIMED_OUT);
    
    LinkedList<EventSubscriber> srcs = new LinkedList<EventSubscriber>();

    // DEBUG stuff
    // To do: move into monitorable stat object
    /*
     * public int nPut = 0; public int nGet = 0; public int nWastedPuts = 0;
     * public int nWastedGets = 0;
     */
    public BasicLongMailbox() {
        this(10);
    }

    public BasicLongMailbox(int initialSize) {
        this(initialSize, Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    public BasicLongMailbox(int initialSize, int maxSize) {
        if (initialSize > maxSize)
            throw new IllegalArgumentException("initialSize: " + initialSize
                    + " cannot exceed maxSize: " + maxSize);
        msgs =  new long[initialSize];
        maxMsgs = maxSize;
    }

    /**
     * Non-blocking, nonpausing get. 
     * @param eo. If non-null, registers this observer and calls it with a MessageAvailable event when
     *  a put() is done.
     * @return buffered message if there's one, or null 
     */
    public long get(EventSubscriber eo) {
        long msg =-1;
        EventSubscriber producer = null;
        synchronized(this) {
            int n = numMsgs;
            if (n > 0) {
            	msg = msgs[icons];
                icons = (icons + 1) % msgs.length;
                numMsgs = n - 1;
                
                if (srcs.size() > 0) {
                    producer = srcs.poll();
                }
            } else {                
                addMsgAvailableListener(eo);
            }
        }
        if (producer != null)  {
            producer.onEvent(this, spaceAvailble);
        }
        return msg;
    }
    
    /**
     * Non-blocking, nonpausing put. 
     * @param eo. If non-null, registers this observer and calls it with an SpaceAvailable event 
     * when there's space.
     * @return buffered message if there's one, or null
     * @see #putnb(Object)
     * @see #putb(Object) 
     */
    public boolean put(long msg, EventSubscriber eo) {
        boolean ret = true; // assume we will be able to enqueue
        EventSubscriber subscriber;
        synchronized(this) {     
            int ip = iprod;
            int ic = icons;
            int n = numMsgs;
            if (n == msgs.length) {
                assert ic == ip : "numElements == msgs.length && ic != ip";
                if (n < maxMsgs) {
                    long[] newmsgs =  new long[Math.min(n * 2, maxMsgs)];
                    System.arraycopy(msgs, ic, newmsgs, 0, n - ic);
                    if (ic > 0) {
                        System.arraycopy(msgs, 0, newmsgs, n - ic, ic);
                    }
                    msgs = newmsgs;
                    ip = n;
                    ic = 0;
                } else {
                    ret = false;
                }
            }
            if (ret) {
                numMsgs = n + 1;
                msgs[ip] = msg;
                iprod = (ip + 1) % msgs.length;
                icons = ic;
                subscriber = sink;
                sink = null;
            } else {
                subscriber = null;
                // unable to enqueue
                if (eo != null) {
                    srcs.add(eo);
                }
            }
        }
        // notify get's subscriber that something is available
        if (subscriber != null) {
            subscriber.onEvent(this, messageAvailable);
        }
        return ret;
    }
    
    /**
     * Get, don't pause or block.
     * 
     * @return stored message, or null if no message found.
     */
    public long getnb() {
        return get(null);
    }

    /**
     * @return non-null message.
     * @throws Pausable
     */
    public long get() throws Pausable{
        Task t = Task.getCurrentTask();
        long msg = get(t);
        while (msg == -1) {
            Task.pause(this);
            removeMsgAvailableListener(t);
            msg = get(t);
        }
        return msg;
    }

    
    /**
     * @return non-null message, or null if timed out.
     * @throws Pausable
     */
    public long get(long timeoutMillis) throws Pausable {
        final Task t = Task.getCurrentTask();
        long msg = get(t);
        long end = System.currentTimeMillis() + timeoutMillis;
        while (msg == -1) {
            TimerTask tt = new TimerTask() {
                public void run() {
                    BasicLongMailbox.this.removeMsgAvailableListener(t);
                    t.onEvent(BasicLongMailbox.this, timedOut);
                }
            };
            Task.timer.schedule(tt, timeoutMillis);
            Task.pause(this);
            tt.cancel();
            removeMsgAvailableListener(t);
            msg = get(t);
            
            timeoutMillis = end - System.currentTimeMillis();
            if (timeoutMillis <= 0) {
            	removeMsgAvailableListener(t);
                break;
            }
        }
        return msg;
    }
    
    
    /**
     * Block caller until at least one message is available.
     * @throws Pausable
     */
	public void untilHasMessage() throws Pausable {
		while (hasMessage(Task.getCurrentTask()) == false) {
			Task.pause(this);
		}
	}

	/**
	 * Block caller until <code>num</code> messages are available.
	 * @param num
	 * @throws Pausable
	 */
	public void untilHasMessages(int num) throws Pausable {
		while (hasMessages(num, Task.getCurrentTask()) == false) {
			Task.pause(this);
		}
	}


	/**
	 * Block caller (with timeout) until a message is available.
	 * @return non-null message.
	 * @throws Pausable
	 */
	public boolean untilHasMessage(long timeoutMillis) throws Pausable {
		final Task t = Task.getCurrentTask();
		boolean has_msg = hasMessage(t);
		long end = System.currentTimeMillis() + timeoutMillis;
		while (has_msg == false) {
			TimerTask tt = new TimerTask() {
				public void run() {
                    BasicLongMailbox.this.removeMsgAvailableListener(t);
	                t.onEvent(BasicLongMailbox.this, timedOut);
				}
			};
			Task.timer.schedule(tt, timeoutMillis);
			Task.pause(this);
			tt.cancel();
			has_msg = hasMessage(t);
			timeoutMillis = end - System.currentTimeMillis();
			if (timeoutMillis <= 0) {
            	removeMsgAvailableListener(t);
				break;
			}
		}
		return has_msg;
	}

	/**
	 * Block caller (with timeout) until <code>num</code> messages are available.
	 * 
	 * @param num
	 * @param timeoutMillis
	 * @return Message or <code>null</code> on timeout
	 * @throws Pausable
	 */
	public boolean untilHasMessages(int num, long timeoutMillis)
			throws Pausable {
		final Task t = Task.getCurrentTask();
		final long end = System.currentTimeMillis() + timeoutMillis;

		boolean has_msg = hasMessages(num, t);
		while (has_msg == false) {
			TimerTask tt = new TimerTask() {
				public void run() {
                    BasicLongMailbox.this.removeMsgAvailableListener(t);
	                t.onEvent(BasicLongMailbox.this, timedOut);
				}
			};
			Task.timer.schedule(tt, timeoutMillis);
			Task.pause(this);
			if (!tt.cancel()) {
            	removeMsgAvailableListener(t);
			}

			has_msg = hasMessages(num, t);
			timeoutMillis = end - System.currentTimeMillis();
			if (!has_msg && timeoutMillis <= 0) {
            	removeMsgAvailableListener(t);
				break;
			}
		}
		return has_msg;
	}

	public boolean hasMessage(Task eo) {
		boolean has_msg;
		synchronized (this) {
			int n = numMsgs;
			if (n > 0) {
				has_msg = true;
			} else {
				has_msg = false;
				addMsgAvailableListener(eo);
			}
		}
		return has_msg;
	}

	public boolean hasMessages(int num, Task eo) {
		boolean has_msg;
		synchronized (this) {
			int n = numMsgs;
			if (n >= num) {
				has_msg = true;
			} else {
				has_msg = false;
				addMsgAvailableListener(eo);
			}
		}
		return has_msg;
	}


	public long peek(int idx) {
		assert idx >= 0 : "negative index";
		long msg = -1;
		synchronized (this) {
			int n = numMsgs;
			if (idx < n) {
				int ic = icons;
				msg = msgs[(ic + idx) % msgs.length];
			} 
		}
		return msg;
	}

	

	public synchronized long[] messages() {
		synchronized (this) {
			long[] result = new long[numMsgs];
			for (int i = 0; i < numMsgs; i++) {
				result[i] = msgs[(icons + i) % msgs.length];
			}
			return result;
		}

	}


    /**
     * Takes an array of mailboxes and returns the index of the first mailbox
     * that has a message. It is possible that because of race conditions, an
     * earlier mailbox in the list may also have received a message.
     */
    // TODO: need timeout variant
    @SuppressWarnings("unchecked")
    public static int select(BasicLongMailbox... mboxes) throws Pausable {
        while (true) {
            for (int i = 0; i < mboxes.length; i++) {
                if (mboxes[i].hasMessage()) {
                    return i;
                }
            }
            Task t = Task.getCurrentTask();
            Basic_Long_EmptySet_MsgAvListener pauseReason = 
                    new Basic_Long_EmptySet_MsgAvListener(t, mboxes);
            for (int i = 0; i < mboxes.length; i++) {
                mboxes[i].addMsgAvailableListener(pauseReason);
            }
            Task.pause(pauseReason);
            for (int i = 0; i < mboxes.length; i++) {
                mboxes[i].removeMsgAvailableListener(pauseReason);
            }
        }
    }

    public synchronized void addSpaceAvailableListener(EventSubscriber spcSub) {
        srcs.add(spcSub);
    }

    public synchronized void removeSpaceAvailableListener(EventSubscriber spcSub) {
        srcs.remove(spcSub);
    }


    public synchronized void addMsgAvailableListener(EventSubscriber msgSub) {
        if (sink != null && sink != msgSub) {
            throw new AssertionError(
                    "Error: A mailbox can not be shared by two consumers.  New = "
                            + msgSub + ", Old = " + sink);
        }
        sink = msgSub;
    }

    public synchronized void removeMsgAvailableListener(EventSubscriber msgSub) {
        if (sink == msgSub) {
            sink = null;
        }
    }

    /**
     * Attempt to put a message, and return true if successful. The thread is not blocked, nor is the task
     * paused under any circumstance. 
     */
    public boolean putnb(long msg) {
        return put(msg, null);
    }

    /**
     * put a non-null message in the mailbox, and pause the calling task  until the
     * mailbox has space
     */

    public void put(long msg) throws Pausable {
        Task t = Task.getCurrentTask();
        while (!put(msg, t)) {
            Task.pause(this);
            removeSpaceAvailableListener(t);
        }
    }

    /**
     * put a non-null message in the mailbox, and pause the calling task  for timeoutMillis
     * if the mailbox is full. 
     */

    public boolean put(long msg, int timeoutMillis) throws Pausable {
        final Task t = Task.getCurrentTask();
        long begin = System.currentTimeMillis();
        while (!put(msg, t)) {
            TimerTask tt = new TimerTask() {
                public void run() {
                    BasicLongMailbox.this.removeSpaceAvailableListener(t);
                    t.onEvent(BasicLongMailbox.this, timedOut);
                }
            };
            Task.timer.schedule(tt, timeoutMillis);
            Task.pause(this);
            removeSpaceAvailableListener(t);
            if (System.currentTimeMillis() - begin >= timeoutMillis) {
                return false;
            }
        }
        return true;
    }
    
    public void putb(long msg) {
        putb(msg, 0 /* infinite wait */);
    }

    public class BlockingSubscriber implements EventSubscriber {
        public volatile boolean eventRcvd = false;
        public void onEvent(EventPublisher ep, Event e) {
            synchronized (BasicLongMailbox.this) {
                eventRcvd = true;
                BasicLongMailbox.this.notify();
            }
        }
        public void blockingWait(final long timeoutMillis) {
            long start = System.currentTimeMillis();
            long remaining = timeoutMillis;
            boolean infiniteWait = timeoutMillis == 0;
            synchronized (BasicLongMailbox.this) {
                while (!eventRcvd && (infiniteWait || remaining > 0)) {
                    try {
                        BasicLongMailbox.this.wait(infiniteWait? 0 : remaining);
                    } catch (InterruptedException ie) {}
                    long elapsed = System.currentTimeMillis() - start;
                    remaining -= elapsed;
                }
            }
        }
    }
    
    
    /**
     * put a non-null message in the mailbox, and block the calling thread  for timeoutMillis
     * if the mailbox is full. 
     */
    public void putb(long msg, final long timeoutMillis) {
        BlockingSubscriber evs = new BlockingSubscriber();
        if (!put(msg, evs)) {
            evs.blockingWait(timeoutMillis);
        }
        if (!evs.eventRcvd) {
            removeSpaceAvailableListener(evs);
        }
    }

    public synchronized int size() {
        return numMsgs;
    }
    
    public synchronized boolean hasMessage() {
        return numMsgs > 0;
    }

    public synchronized boolean hasSpace() {
        return (maxMsgs - numMsgs) > 0;
    }

    /**
     * retrieve a message, blocking the thread indefinitely. Note, this is a
     * heavyweight block, unlike #get() that pauses the Fiber but doesn't block
     * the thread.
     */

    public long getb() {
        return getb(0);
    }

    /**
     * retrieve a msg, and block the Java thread for the time given.
     * 
     * @param millis. max wait time
     * @return null if timed out.
     */
    public long getb(final long timeoutMillis) {
        BlockingSubscriber evs = new BlockingSubscriber();
        long msg = -1;
        
        if ((msg = get(evs)) == -1) {
            evs.blockingWait(timeoutMillis);
            if (evs.eventRcvd) {
                msg = get(null); // non-blocking get.
            } 
        }
        if (msg == -1) {
            removeMsgAvailableListener(evs);
        }
        return msg;
    }

    public synchronized String toString() {
        return "id:" + System.identityHashCode(this) + " " +
        // DEBUG "nGet:" + nGet + " " +
                // "nPut:" + nPut + " " +
                // "numWastedPuts:" + nWastedPuts + " " +
                // "nWastedGets:" + nWastedGets + " " +
                "numMsgs:" + numMsgs + " ipros:"+iprod+" iconsure:"+icons;
    }

    // Implementation of PauseReason
    public boolean isValid(Task t) {
        synchronized(this) {
            return (t == sink) || srcs.contains(t);
        } 
    }
}

class Basic_Long_EmptySet_MsgAvListener implements PauseReason, EventSubscriber {
    final Task task;
    final BasicLongMailbox[] mbxs;

    Basic_Long_EmptySet_MsgAvListener(Task t, BasicLongMailbox[] mbs) {
        task = t;
        mbxs = mbs;
    }

    public boolean isValid(Task t) {
        // The pauseReason is true (there is valid reason to continue
        // pausing) if none of the mboxes have any elements
        for (BasicLongMailbox mb : mbxs) {
            if (mb.hasMessage())
                return false;
        }
        return true;
    }

    public void onEvent(EventPublisher ep, Event e) {
        for (BasicLongMailbox m : mbxs) {
            if (m != ep) {
                ((BasicLongMailbox)ep).removeMsgAvailableListener(this);
            }
        }
        task.resume();
    }

    public void cancel() {
        for (BasicLongMailbox mb : mbxs) {
            mb.removeMsgAvailableListener(this);
        }
    }
}
