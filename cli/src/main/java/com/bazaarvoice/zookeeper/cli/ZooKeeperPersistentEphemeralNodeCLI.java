package com.bazaarvoice.zookeeper.cli;


import com.bazaarvoice.zookeeper.ZooKeeperConfiguration;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.recipes.ZooKeeperPersistentEphemeralNode;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.zookeeper.CreateMode;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command-line interface for creating a @link ZooKeeperPersistentEphemeralNode
 */
public class ZooKeeperPersistentEphemeralNodeCLI implements Closeable {

    private CLIConfig _myConfig = new CLIConfig();
    private ZooKeeperConfiguration _zkConfig = null;
    private ZooKeeperConnection _zkConnection = null;
    private CreateMode _createMode = CreateMode.EPHEMERAL;
    private List<ZooKeeperPersistentEphemeralNode> _zkNodeList = new ArrayList<ZooKeeperPersistentEphemeralNode>();

    private JCommander _jCommander = new JCommander(_myConfig);

    //TODO output usage if exception

    @VisibleForTesting
    class CLIConfig{
        @Parameter(names = {"-s","--sequential"}, description = "enable sequential mode (created nodes are ephemeral and sequential")
        private boolean _isSequential = false;

        @Parameter(names = {"-z","--zookeeper-ensemble"}, description = "the zookeeper ensemble to which the node is published")
        private  String _zooKeeperEnsemble;

        @Parameter(names = {"-N", "--namespace"}, description = "namespace prepended to each node")
        private String _nameSpace;

       @Parameter(names = {"-n","--node"}, description = "description of node to be published")
        private List<String> _nodeList = new ArrayList<String>();

        boolean is_sequential() {
            return _isSequential;
        }

        String get_zooKeeperEnsemble() {
            return _zooKeeperEnsemble;
        }

        String get_nameSpace() {
            return _nameSpace;
        }

        List<String> get_nodeList() {
            return _nodeList;
        }
    }

//    public ZooKeeperPersistentEphemeralNodeCLI() {
//        _myConfig = new CLIConfig();
//        _jCommander = new JCommander(_myConfig);
//
//    }

    @VisibleForTesting
    CLIConfig get_myConfig() {
        return _myConfig;
    }

    /**
     * parses out the args and sets up the configuration
     * @param args String list of arguments (i.e., passed in on command line to main)
     */
    public void parse(String args[]) {
        _jCommander.parse(args);

        if (_myConfig.is_sequential()) {
            _createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
        }
        else {
            _createMode = CreateMode.EPHEMERAL;
        }

    }

    /**
     * opens a ZooKeeperConnection based on configuration and creates nodes according to the list of node descriptions
     */
    public void createNodes() throws IOException{
        _zkConfig = new ZooKeeperConfiguration();
        if (null != _myConfig.get_zooKeeperEnsemble()) _zkConfig.withConnectString(_myConfig.get_zooKeeperEnsemble());
        if (null != _myConfig.get_nameSpace()) _zkConfig.withNamespace(_myConfig.get_nameSpace());
        _zkConnection = _zkConfig.connect();

        for(String nodedesc : _myConfig.get_nodeList()){
            try{
                _zkNodeList.add(_createNode(nodedesc));
            } catch (IOException e){
                _jCommander.usage();
                throw e;
            }
        }
    }

    /**
     *
     * @param nodedesc string describing the node in format '/path/to/node-1=node data'
     * @return ZooKeeperPersistentEphemeralNode matching the description string
     * @throws IOException May be thrown when node data file doesn't exist when using @-syntax
     */
    private ZooKeeperPersistentEphemeralNode _createNode(String nodedesc) throws IOException {
        String path;
        byte[] data;

        //split on "=" at most one time to get the path and the data
        String[] strings = nodedesc.split("=", 2);
        path = strings[0];

        if (strings.length == 1) {
            //no data
            data = new byte[]{};
        } else if (strings[1].charAt(0) =='@'){
            //data in file
            File file = new File(strings[1].substring(1));
            data = new byte[(int)file.length()];    //TODO check cast for overflow
            DataInputStream dis = new DataInputStream(new FileInputStream(file));
            dis.readFully(data);
        } else {
            //data is inline
            data = strings[1].getBytes();
        }

        //got all the stuff, now create the node
        return new ZooKeeperPersistentEphemeralNode(_zkConnection, path, data, _createMode);
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
        if (null != _zkConnection){
            _zkConnection.close();
            _zkConnection = null;
        }
    }

    public List<ZooKeeperPersistentEphemeralNode> get_zkNodeList() {
        return Collections.unmodifiableList(_zkNodeList);
    }

    public static void main(String args[]){
        ZooKeeperPersistentEphemeralNodeCLI zkNodeCLI = new ZooKeeperPersistentEphemeralNodeCLI();
        zkNodeCLI.parse(args);
        try{
            zkNodeCLI.createNodes();
            NeverendingThread t = new NeverendingThread();
            t.addObjectReference(zkNodeCLI);
            t.start();
        } catch(IOException e){
            //caught an IOException while creating the nodes.  Since we didn't create the nodes correctly,
            //don't start the NeverendingThread
        }
        //TODO any way to clean up the zkNodeCLI? Do we need to clean anything up if we assume the JVM has crashed?
    }
}
