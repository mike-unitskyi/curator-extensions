package com.bazaarvoice.zookeeper.autonode.cli;


import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Command-line interface for creating a @link ZooKeeperPersistentEphemeralNode
 */
public class ZooKeeperPersistentEphemeralNodeCreator {

//    @Parameter(names = {"-z","--zookeeper-ensemble"}, description = "the zookeeper hosts to which the node is published", splitter = CommaParameterSplitter.class)
//    private  List<String> _zooKeeperEnsemble = new ArrayList<String>();
    @Parameter(names = {"-z","--zookeeper-ensemble"}, description = "the zookeeper ensemble to which the node is published")
    private  String _zooKeeperEnsemble;

//   @Parameter(names = {"-n","--node"}, description = "description of node to be published")
//    private List<String> _nodeList = new ArrayList<String>();
    @Parameter(names = {"-n","--node"}, description = "description of node to be published", converter = ZooKeeperNodeDescriptionConverter.class)
    private List<ZooKeeperNodeDescription> _nodeList = new ArrayList<ZooKeeperNodeDescription>();

    public String get_zooKeeperEnsemble() {
        return _zooKeeperEnsemble;
    }

//    public List<String> get_nodeList() {
//        return _nodeList;
//    }
    public List<ZooKeeperNodeDescription> get_nodeList() {
        return _nodeList;
    }

    /**
     * Helper class to hold the node descriptions as they are parsed out of the command line
     */
    public class ZooKeeperNodeDescription{
        public String _path = null;
//        private byte[] _data = null; //TODO make this immutable?

        public String get_path() {
            return _path;
        }

//        public byte[] get_data() {
//            return _data;
//        }
     }

//    private class ZooKeeperNodeData{
//        @Parameter(description = "Data for a ZooKeeperPersistentEphemeralNode")
//        private String _data;
//    }

   public class ZooKeeperNodeDescriptionConverter implements IStringConverter<ZooKeeperNodeDescription>{
        @Override
        public ZooKeeperNodeDescription convert(String value){
            ZooKeeperNodeDescription zkNodeDesc = new ZooKeeperNodeDescription();

            zkNodeDesc._path = value;

//            //first split the String on "=" one time to separate the path from the data
//            String[] strings = value.split("=",2);
//
//            zkNodeDesc._path = strings[0];



//            //check if strings[1] has quotes, @, or is empty
//            if (strings.length != 2){
//                zkNodeDesc._data = new byte[]{};
//            } else if {
//
//            } else {
//                _data = strings[1].getBytes();
//            }

            return zkNodeDesc;
        }
    }

    public static void main(String args[]){
        ZooKeeperPersistentEphemeralNodeCreator zkNodeCreator = new ZooKeeperPersistentEphemeralNodeCreator();
        JCommander jCommander = new JCommander(zkNodeCreator, args);


    }
}
