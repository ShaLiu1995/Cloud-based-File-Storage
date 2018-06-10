package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.SimpleIndex;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    // private final ManagedChannel metadataChannel;
    // private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    // private final ManagedChannel blockChannel;
    // private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    // protected List<MetadataStoreGrpc.MetadataStoreBlockingStub> followers;

    // protected MetadataStoreGrpc.MetadataStoreBlockingStub leader;

    public MetadataStore(ConfigReader config) {
    	this.config = config;

        // this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
        //         .usePlaintext(true).build();
        // this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        // this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
        //         .usePlaintext(true).build();
        // this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        // this.followers = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
	}  

	private void start(int serverNum, int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this.config, serverNum))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(c_args.getInt("number"), config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        protected Map<String, FileInfo> metaMap;
        protected List<FileInfo> logList;

        // private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

        private List<MetadataStoreGrpc.MetadataStoreBlockingStub> followerStubList;
        // private MetadataStoreGrpc.MetadataStoreBlockingStub leader;

        protected boolean isLeader;
        protected boolean isCrashed;

        protected int serverId;
        protected int leaderId;
        protected int commitedIndex;

        public MetadataStoreImpl(ConfigReader config, int serverNum) {
            super();
            this.metaMap = new HashMap<String, FileInfo>();
            this.logList = new ArrayList<FileInfo>();
            this.serverId = serverNum; 
            this.leaderId = config.getLeaderNum();
            this.isLeader = this.serverId == this.leaderId ? true : false;
            this.isCrashed = false;
            this.commitedIndex = 0;

            ManagedChannel blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

            this.followerStubList = new ArrayList<MetadataStoreGrpc.MetadataStoreBlockingStub>();
            for (int i = 1; i <= config.getNumMetadataServers(); i++) {
                if (i == serverNum) {
                    continue;
                }
                ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i))
                    .usePlaintext(true).build();
                MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
                this.followerStubList.add(metadataStub);
            }
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!
        @Override
        public void readFile(FileInfo request, StreamObserver<FileInfo> responseObserver) {

            String filename = request.getFilename();
            logger.info("Read file with name:" + filename);
            
            FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
            fileInfoBuilder.setFilename(filename);

            if (!metaMap.containsKey(filename)) {
                fileInfoBuilder.setVersion(0); 
            } else {
                fileInfoBuilder.setVersion(metaMap.get(filename).getVersion());
                fileInfoBuilder.addAllBlocklist(metaMap.get(filename).getBlocklistList());
            }

            FileInfo response = fileInfoBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {

            String filename = request.getFilename();
            logger.info("Modify file with name:" + filename);
            
            WriteResult.Builder writeResultBuilder = WriteResult.newBuilder();
            Block.Builder blockBuilder = Block.newBuilder();
            
            if (metaMap.containsKey(filename)) {
                writeResultBuilder.setCurrentVersion(metaMap.get(filename).getVersion());
            } else {
                writeResultBuilder.setCurrentVersion(0);
            }

            if (!isLeader) {
                writeResultBuilder.setResult(Result.NOT_LEADER);
            } 
            else if (request.getVersion() != writeResultBuilder.getCurrentVersion() + 1) {
                writeResultBuilder.setResult(Result.OLD_VERSION);
            } 
            else {
                List<String> missingBlocks = new ArrayList<>();
                for (String hash : request.getBlocklistList()) {
                    if (hash.equals("0"))  continue;
                    Block newBlock = Block.newBuilder().setHash(hash).build();
                    SimpleAnswer hasBlockResult = blockStub.hasBlock(newBlock);
                    if (hasBlockResult.getAnswer() == false) {
                        missingBlocks.add(hash);
                    }
                }

                if (!missingBlocks.isEmpty()) {
                    writeResultBuilder.setResult(Result.MISSING_BLOCKS);
                    writeResultBuilder.addAllMissingBlocks(missingBlocks);
                } else {
                    logList.add(FileInfo.newBuilder(request).build());
                    // metaMap.put(request.getFilename(), FileInfo.newBuilder(request).build()); 

                    int vote = 0;
                    for(MetadataStoreGrpc.MetadataStoreBlockingStub followerStub : followerStubList) {
                        SimpleAnswer isCrashedAnser = followerStub.isCrashed(Empty.newBuilder().build());
                        if (isCrashedAnser.getAnswer() == false) {
                            followerStub.appendLog(request);
                            vote++;
                        }
                    }

                    if(vote >= followerStubList.size() / 2) {
                        writeResultBuilder.setResult(Result.OK);
                        metaMap.put(request.getFilename(), FileInfo.newBuilder(request).build());                     
                        commitedIndex = logList.size();
                        logger.info("Metadata store modification successful. New version number is: " + request.getVersion());
                        for(MetadataStoreGrpc.MetadataStoreBlockingStub followerStub : followerStubList) {
                            followerStub.commit(SimpleIndex.newBuilder().setIndex(logList.size()).build());
                        }
                    }
                }
            }
                   
            
            WriteResult response = writeResultBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void deleteFile(FileInfo request, StreamObserver<WriteResult> responseObserver) {

            String filename = request.getFilename();
            logger.info("Read file with name:" + filename);
            
            WriteResult.Builder writeResultBuilder = WriteResult.newBuilder();

            if (metaMap.containsKey(filename)) {
                writeResultBuilder.setCurrentVersion(metaMap.get(filename).getVersion());
            } else {
                writeResultBuilder.setCurrentVersion(0);
            }

            if (!isLeader) {
                writeResultBuilder.setResult(Result.NOT_LEADER);
            } 
            else if (request.getVersion() != writeResultBuilder.getCurrentVersion() + 1) {
                writeResultBuilder.setResult(Result.OLD_VERSION);
            } 
            else {
                FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
                fileInfoBuilder.setFilename(filename);
                fileInfoBuilder.setVersion(request.getVersion());
                fileInfoBuilder.addAllBlocklist(Arrays.asList("0"));
                FileInfo newFileInfo = fileInfoBuilder.build();

                logList.add(newFileInfo);

                int vote = 0;
                for(MetadataStoreGrpc.MetadataStoreBlockingStub followerStub : followerStubList) {
                    SimpleAnswer isCrashedAnser = followerStub.isCrashed(Empty.newBuilder().build());
                    if (isCrashedAnser.getAnswer() == false) {
                        followerStub.appendLog(newFileInfo);
                        vote++;
                    }
                }

                if(vote >= followerStubList.size() / 2) {
                    writeResultBuilder.setResult(Result.OK);
                    writeResultBuilder.setCurrentVersion(request.getVersion());
                    metaMap.put(request.getFilename(), FileInfo.newBuilder(newFileInfo).build());                     
                    commitedIndex = logList.size();
                    logger.info("Metadata store modification successful. New version number is: " + request.getVersion());
                    for(MetadataStoreGrpc.MetadataStoreBlockingStub followerStub : followerStubList) {
                        followerStub.commit(SimpleIndex.newBuilder().setIndex(logList.size()).build());
                    }
                } 
            }

            WriteResult response = writeResultBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


        @Override
        public void isLeader(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder simpleAnserBuilder = SimpleAnswer.newBuilder();
            simpleAnserBuilder.setAnswer(isLeader);
            SimpleAnswer response = simpleAnserBuilder.build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(Empty request, StreamObserver<Empty> responseObserver) {
            if(isLeader) {
                throw new RuntimeException("crash on leader machine");
            }
            isCrashed = true;

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(Empty request, StreamObserver<Empty> responseObserver) {
            if(!isLeader) {
                isCrashed = false;
                followerStubList.get(leaderId).update(Empty.newBuilder().build());
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(Empty request, StreamObserver<SimpleAnswer> responseObserver) {
            SimpleAnswer.Builder simpleAnserBuilder = SimpleAnswer.newBuilder();
            simpleAnserBuilder.setAnswer(isCrashed);
            SimpleAnswer response = simpleAnserBuilder.build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(FileInfo request, StreamObserver<FileInfo> responseObserver) {
            String filename = request.getFilename();

            FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
            fileInfoBuilder.setFilename(filename);

            if (!metaMap.containsKey(filename)) {
                fileInfoBuilder.setVersion(0);
            } else {
                fileInfoBuilder.setVersion(metaMap.get(filename).getVersion());
            }
            
            FileInfo response = fileInfoBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override 
        public void appendLog(FileInfo request, StreamObserver<Empty> responseObserver) {
            logList.add(FileInfo.newBuilder(request).build());

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void commit(SimpleIndex request, StreamObserver<SimpleIndex> responseObserver) {
            SimpleIndex.Builder simpleIndexBuilder = SimpleIndex.newBuilder();
            if(logList.size() == request.getIndex()){
                for (int i = commitedIndex; i < logList.size(); i++){
                    FileInfo newFileInfo = logList.get(i);
                    metaMap.put(newFileInfo.getFilename(), FileInfo.newBuilder(newFileInfo).build());
                }
                commitedIndex = request.getIndex();
            }

            simpleIndexBuilder.setIndex(commitedIndex);
            SimpleIndex response = simpleIndexBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void update(Empty request, StreamObserver<Empty> responseObserver) {
            SimpleIndex.Builder simpleIndexBuilder = SimpleIndex.newBuilder();
            simpleIndexBuilder.setIndex(commitedIndex);
            SimpleIndex newSimpleIndex = simpleIndexBuilder.build();

            for(MetadataStoreGrpc.MetadataStoreBlockingStub followerStub: followerStubList){
                if (followerStub.isCrashed(Empty.newBuilder().build()).getAnswer() == true){
                    break;
                }

                SimpleIndex commitResult = followerStub.commit(newSimpleIndex);
                int followerCommitedIndex = commitResult.getIndex();
                while (followerCommitedIndex < commitedIndex){
                    for(int i = followerCommitedIndex; i < logList.size(); i++){
                        followerStub.appendLog(logList.get(i));
                    }
                    followerCommitedIndex = followerStub.commit(newSimpleIndex).getIndex();
                }
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }


    }
}