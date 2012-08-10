package com.bazaarvoice.zookeeper.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 *  Thread that holds onto an Object and never ends.
 */
public class NeverendingThread extends Thread {

    private Semaphore _mySemaphore = new Semaphore(0);
    private List<Object> _objectReferences = null;

    /**
     * This method adds objects to the NeverendingThread to hold on to. This is to ensure that these objects are not gc'ed.
     * @param obj Object to hold on to
     */
    public void addObjectReference(Object obj){
        if (null == _objectReferences){
            _objectReferences = new ArrayList<Object>();
        }
        _objectReferences.add(obj);
    }

    @Override
    public void run() {

        while(true){
            try{
                _mySemaphore.acquire();
            } catch (InterruptedException e){

            }
        }
    }

}
