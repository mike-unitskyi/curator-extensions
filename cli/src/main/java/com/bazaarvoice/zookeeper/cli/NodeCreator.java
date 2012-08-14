package com.bazaarvoice.zookeeper.cli;


import com.bazaarvoice.zookeeper.ZooKeeperConfiguration;
import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.bazaarvoice.zookeeper.recipes.ZooKeeperPersistentEphemeralNode;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.String;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command-line interface for creating a @link ZooKeeperPersistentEphemeralNode
 */
public class NodeCreator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeCreator.class);

    private CLIConfig _myConfig = new CLIConfig();
    private ZooKeeperConfiguration _zkConfig = null;
    private ZooKeeperConnection _zkConnection = null;
    private CreateMode _createMode = CreateMode.EPHEMERAL;
    private List<ZooKeeperPersistentEphemeralNode> _zkNodeList = new ArrayList<ZooKeeperPersistentEphemeralNode>();

    private JCommander _jCommander = new JCommander(_myConfig);

    //TODO output usage if exception

    @VisibleForTesting
    static class CLIConfig {
        @Parameter(names = "--help", help = true)
        boolean _showHelp;

        @Parameter(names = {"-s","--sequential"}, description = "enable sequential mode (created nodes are ephemeral and sequential")
        boolean _isSequential = false;

        @Parameter(names = {"-z","--zookeeper-ensemble"}, required = true, description = "the zookeeper ensemble to which the node is published")
        String _zooKeeperEnsemble;

        @Parameter(names = {"-N", "--namespace"}, description = "namespace prepended to each node")
        String _nameSpace;

        @Parameter(names = {"-n","--node"}, required = true, description = "description of node to be published")
        List<String> _nodeList = Lists.newArrayList();
    }


    @VisibleForTesting
    CLIConfig getMyConfig() {
        return _myConfig;
    }

    /**
     * parses out the args and sets up the configuration
     * @param args String list of arguments (i.e., passed in on command line to main)
     */
    public void parse(String args[]) throws ParameterException {
        LOG.trace("Parsing arguments into CLIConfig object");

        try{
            _jCommander.parse(args);
        } catch (ParameterException e){
            LOG.error("Unable to parse parameters: {}", e);
            throw e;
        }

        if (_myConfig._showHelp){
            _jCommander.usage();
        }

        if (_myConfig._isSequential) {
            _createMode = CreateMode.EPHEMERAL_SEQUENTIAL;
        }
        else {
            _createMode = CreateMode.EPHEMERAL;
        }

    }

    /**
     * opens a ZooKeeperConnection based on configuration and creates nodes according to the list of node descriptions
     */
    public void createNodes() throws IOException {
        LOG.trace("creating nodes");
        _zkConfig = new ZooKeeperConfiguration();
        if (null != _myConfig._zooKeeperEnsemble) _zkConfig.withConnectString(_myConfig._zooKeeperEnsemble);
        if (null != _myConfig._nameSpace) _zkConfig.withNamespace(_myConfig._nameSpace);
        _zkConnection = _zkConfig.connect();

        for(String nodedesc : _myConfig._nodeList){
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

        LOG.trace("creating node {}",nodedesc);

        //split on "=" at most one time to get the path and the data
        String[] strings = nodedesc.split("=", 2);
        path = strings[0];

        if (strings.length == 1) {
            //no data
            data = new byte[]{};
        } else if (strings[1].length() > 0 && strings[1].charAt(0) =='@'){
            String filename = strings[1].substring(1);
            try{
                File file = new File(filename);
                data = Files.toByteArray(file);
            } catch (IOException e) {
                LOG.error("Error creating node with file {}",filename);
                throw e;
            }
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

        int exitCode = 0;

        NodeCreator zkNodeCreator = new NodeCreator();
        try{
            zkNodeCreator.parse(args);
            zkNodeCreator.createNodes();
            synchronized (zkNodeCreator){
                zkNodeCreator.wait();
            }
        } catch (ParameterException e){
            //caught an ParameterException while parsing parameters
            zkNodeCreator._jCommander.usage();
            exitCode = 1;
        } catch(IOException e){
            //caught an IOException while creating the nodes.
            zkNodeCreator._jCommander.usage();
            exitCode = 1;
        } catch(InterruptedException e) {
        } finally {
            Closeables.closeQuietly(zkNodeCreator);
        }
        System.exit(exitCode); //set exit code to show that there was an error
    }
}
