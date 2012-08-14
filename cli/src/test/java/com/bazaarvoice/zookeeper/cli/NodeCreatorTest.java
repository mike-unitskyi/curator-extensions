package com.bazaarvoice.zookeeper.cli;

import com.google.common.io.Closeables;
import com.netflix.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: david.han
 * Date: 8/9/12
 * Time: 2:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class NodeCreatorTest {

    static String _zkSequentialOption = "-s";
    static String _zkSequentialOption_long = "--sequential";
    static String _zkEnsembleOption = "-z";
    static String _zkEnsembleOption_long = "--zookeeper-ensemble";
    static String _zkEnsembleString; //this will be retrieved from the TestingServer during setUp
    static String _zkNamespaceOption = "-N";
    static String _zkNamespaceOption_long = "--namespace";
    static String _zkNamespaceString = "/namespace/path";
    static String _zkNodeOption = "-n";
    static String _zkNodeOption_long = "--node";
    static String _zkNodePath = "/test/path/to/node-1";
    static String _zkNodeData = "node data";
    static String _testDataFile = "/nodedata.txt";
    static String _zkNodeDataFilename; //set in the setUp
    static String _zkNodeDataFilenameString;//set in the setUp
    static byte[] _zkNodeDataFileData;
    static String _zkNodeDataFilename_nonexistent = "nodedata_nonexistent.txt";
    static String _zkNodeDataFilenameString_nonexistent = "@"+_zkNodeDataFilename_nonexistent;

    TestingServer _zooKeeperServer;
    NodeCreator _zkNodeCreator;


    @Before
    public void setUp() throws Exception {
        _zooKeeperServer = new TestingServer();
        _zkEnsembleString = _zooKeeperServer.getConnectString();

        URL url = this.getClass().getResource(_testDataFile);
        File file = new File(url.getFile());
         DataInputStream dis = new DataInputStream(new FileInputStream(file));
        _zkNodeDataFileData = new byte[(int)file.length()];
        dis.readFully(_zkNodeDataFileData);
        dis.close();

        _zkNodeDataFilename = file.getAbsolutePath();
        _zkNodeDataFilenameString = "@"+_zkNodeDataFilename;

        _zkNodeCreator = new NodeCreator();
    }

    @After
    public void tearDown() throws Exception {
        Closeables.closeQuietly((_zkNodeCreator));
        Closeables.closeQuietly(_zooKeeperServer);
    }

//    @Test
//    public void testZooKeeperPersistentEphemeralNodeCLI_empty(){
//
//        assertFalse(_zkNodeCreator.getMyConfig()._isSequential);
//        assertNull(_zkNodeCreator.getMyConfig()._zooKeeperEnsemble);
//        assertNull(_zkNodeCreator.getMyConfig()._nameSpace);
//        assertEquals(0, _zkNodeCreator.getMyConfig()._nodeList.size());
//    }

    @Test
    public void testCommandParsing_sequential(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
                _zkSequentialOption
        };

        _zkNodeCreator.parse(args);

        assertTrue(_zkNodeCreator.getMyConfig()._isSequential);
    }

    @Test
    public void testCommandParsing_sequential_long(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
                _zkSequentialOption_long
        };

        _zkNodeCreator.parse(args);

        assertTrue(_zkNodeCreator.getMyConfig()._isSequential);
    }


    @Test
    public void testCommandParsing_ensemble(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
        };

        _zkNodeCreator.parse(args);

        assertEquals(_zkEnsembleString, _zkNodeCreator.getMyConfig()._zooKeeperEnsemble);
    }

    @Test
    public void testCommandParsing_ensemble_long(){
        String[] args = {
                _zkEnsembleOption_long, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
        };

        _zkNodeCreator.parse(args);

        assertEquals(_zkEnsembleString, _zkNodeCreator.getMyConfig()._zooKeeperEnsemble);
    }

    @Test
    public void testCommandParsing_namespace(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
                _zkNamespaceOption, _zkNamespaceString
        };

        _zkNodeCreator.parse(args);

        assertEquals(_zkNamespaceString, _zkNodeCreator.getMyConfig()._nameSpace);
    }

    @Test
    public void testCommandParsing_namespace_long(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData,
                _zkNamespaceOption_long, _zkNamespaceString
        };

        _zkNodeCreator.parse(args);

        assertEquals(_zkNamespaceString, _zkNodeCreator.getMyConfig()._nameSpace);
    }

    @Test
    public void testCommandParsing_nodes(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData
        };

        _zkNodeCreator.parse(args);

        assertTrue(_zkNodeCreator.getMyConfig()._nodeList.contains(_zkNodePath+"="+_zkNodeData));
    }

    @Test
    public void testCommandParsing_nodes_long(){
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption_long, _zkNodePath+"="+_zkNodeData
        };

        _zkNodeCreator.parse(args);

        assertTrue(_zkNodeCreator.getMyConfig()._nodeList.contains(_zkNodePath+"="+_zkNodeData));
    }

    @Test
    public void testZooKeeperPersistentEphemeralNodeCLI_nodedatafromstring() throws IOException {
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeData
        };

        _zkNodeCreator.parse(args);
        _zkNodeCreator.createNodes();

        //TODO how to inspect the node to see if it was created correctly?
    }

    @Test
    public void testZooKeeperPersistentEphemeralNodeCLI_nodedatafromfile() throws IOException {
        String[] args = {_zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeDataFilenameString
        };

        _zkNodeCreator.parse(args);
        _zkNodeCreator.createNodes();

        //TODO how to inspect the node to see if it was created correctly?
        for (File file : _zooKeeperServer.getTempDirectory().listFiles()) {
            System.out.println(file.getAbsoluteFile());
        }

        _zkNodeCreator.get_zkNodeList();
        System.out.println(_zkNodeCreator.get_zkNodeList().size());
    }

    @Test(expected = FileNotFoundException.class)
    public void testZooKeeperPersistentEphemeralNodeCLI_nodedatafromfile_nonexistent() throws IOException {
        String[] args = {
                _zkEnsembleOption, _zkEnsembleString,
                _zkNodeOption, _zkNodePath+"="+_zkNodeDataFilenameString_nonexistent
        };

        _zkNodeCreator.parse(args);
        _zkNodeCreator.createNodes();
    }
}
