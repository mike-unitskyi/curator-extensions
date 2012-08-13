package com.bazaarvoice.zookeeper.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 *  Thread that holds onto an Object and never ends.
 */
public class NeverendingThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(NeverendingThread.class);

    private Semaphore _mySemaphore = new Semaphore(0);
    private List<Object> _objectReferences = null;

    /**
     * This method adds objects to the NeverendingThread to hold on to. This is to ensure that these objects are not gc'ed.
     * @param obj Object to hold on to
     */
    public void addObjectReference(Object obj){
        LOG.trace("Adding obj "+obj.toString()+ " to NeverendingThread's object references");
        if (null == _objectReferences){
            _objectReferences = new ArrayList<Object>();
        }
        _objectReferences.add(obj);
    }

    @Override
    public void run() {
        LOG.info("Starting NeverendingThread");
        while(true){
            try{
                LOG.trace("NeverendingThread "+this.toString()+" attempting to acquire semaphore to wait forever.");
                _mySemaphore.acquire();
            } catch (InterruptedException e){
                LOG.trace("NeverendingThread "+this.toString()+" caught an InterruptedException:"+e.getLocalizedMessage());
            }
        }
    }

}
