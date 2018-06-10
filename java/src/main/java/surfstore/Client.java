package surfstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

import com.google.protobuf.ByteString;

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



public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    private Map<String, byte[]> hashBlockMap;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        this.hashBlockMap = new HashMap<String, byte[]>();
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    public void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion Failed");
        }
    }

    // private static Block stringToBlock(String s) {
    //     Block.Builder blockBuilder = Block.newBuilder();

    //     try {
    //         blockBuilder.setData(ByteString.copyFrom(s, "UTF-8"));
    //     } catch (UnsupportedEncodingException e) {
    //         throw e.printStackTrace();
    //     }

    //     blockBuilder.setHash(HashUtils.sha256(s));

    //     return blockBuilder.build();
    // }

    // private static Block byteArrayToBlock(byte[] buffer, int numBytesRead) {
    //     Block.Builder blockBuilder = Block.newBuilder();

    //     try {
    //         blockBuilder.setData(ByteString.copyFrom(buffer, 0, numBytesRead));
    //     } catch (UnsupportedEncodingException e) {
    //         throw e.printStackTrace();
    //     }

    //     blockBuilder.setHash(HashUtils.sha256(buffer));

    //     return blockBuilder.build();
    // }

    private void upload(Namespace c_args) {
    	String filepath = c_args.getString("file_path");
        String filename = filepath.substring(filepath.lastIndexOf("\\") + 1);

        List<String> hashlist = new ArrayList<>();
        int numBytesRead = 0;
        byte[] buffer = new byte[4096];
        FileInputStream fin;

        try {         
            fin = new FileInputStream(filepath);
        } catch (IOException e) {
        	System.out.println("Not Found");
        	return;
        }

        try {
            numBytesRead = fin.read(buffer);
            while(numBytesRead >= 0) {
                if (numBytesRead <= 4096) {
                    byte[] lastBuffer = new byte[numBytesRead];
                    System.arraycopy(buffer, 0, lastBuffer, 0, numBytesRead);
                    String hash = HashUtils.sha256(lastBuffer);
                    hashlist.add(hash);
                    hashBlockMap.put(hash, lastBuffer.clone());
                } else {
                    String hash = HashUtils.sha256(buffer);
                    hashlist.add(hash);
                    hashBlockMap.put(hash, buffer.clone());
                }    
                numBytesRead = fin.read(buffer);
            }
        } catch (IOException e) {
            // System.out.println("Not Found");
            e.printStackTrace();
        }

        // Generate Request
        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
        fileInfoBuilder.setFilename(filename);       
        FileInfo readFileResult = metadataStub.readFile(fileInfoBuilder.build());
        fileInfoBuilder.setVersion(readFileResult.getVersion() + 1);
        fileInfoBuilder.addAllBlocklist(hashlist);
        WriteResult modifyFileResult = metadataStub.modifyFile(fileInfoBuilder.build());

        while(modifyFileResult.getResult() != Result.OK) {
            if (modifyFileResult.getResult() == Result.OLD_VERSION) {
                fileInfoBuilder.setVersion(modifyFileResult.getCurrentVersion() + 1);
            } 
            else if (modifyFileResult.getResult() == Result.MISSING_BLOCKS) {
                List<String> missingBlocksList = modifyFileResult.getMissingBlocksList();
                for (String hash : missingBlocksList) {
                    Block.Builder blockBuilder = Block.newBuilder();
                    blockBuilder.setHash(hash);
                    blockBuilder.setData(ByteString.copyFrom(hashBlockMap.get(hash)));
                    blockStub.storeBlock(blockBuilder.build());
                }
                fileInfoBuilder.setVersion(modifyFileResult.getCurrentVersion() + 1);
            }
            modifyFileResult = metadataStub.modifyFile(fileInfoBuilder.build());
        }
        System.out.println("OK");
    }

    public void download(Namespace c_args) {
        String filepath = c_args.getString("file_path");
        String filename = filepath.substring(filepath.lastIndexOf("\\") + 1);

        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
        fileInfoBuilder.setFilename(filename);
        FileInfo readFileResult = metadataStub.readFile(fileInfoBuilder.build());

        List<String> completeHashList = readFileResult.getBlocklistList();
        List<String> missingHashList = new ArrayList<>();

        if (readFileResult.getVersion() == 0) {
            System.out.println("Not Found");
            return;
        } else if (readFileResult.getBlocklistCount() == 1 && readFileResult.getBlocklist(0).equals("0")) {
            System.out.println("Not Found");
            return;
        } else {
            for (String hash : completeHashList) {
                if (!hashBlockMap.containsKey(hash)) {
                    missingHashList.add(hash);
                }
            }
            for (String missingHash : missingHashList) {
                Block.Builder blockBuilder = Block.newBuilder();
                blockBuilder.setHash(missingHash);
                SimpleAnswer hasBlockAnswer = blockStub.hasBlock(blockBuilder.build());
                if (hasBlockAnswer.getAnswer() == true) {
                    Block getBlockResult = blockStub.getBlock(blockBuilder.build());
                    hashBlockMap.put(missingHash, getBlockResult.getData().toByteArray());
                }
            }
        }

        // Form the complete file
        String destination = c_args.getString("destination");
        File output_file = new File(destination + "/" + filename);
        try {
            FileOutputStream fout = new FileOutputStream(output_file);
            for(String hash : completeHashList) {	
            	fout.write(hashBlockMap.get(hash));        	
            }
            fout.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("OK");
    }

    public void delete(Namespace c_args) {
        String filepath = c_args.getString("file_path");
        String filename = filepath.substring(filepath.lastIndexOf("\\") + 1);

        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
        fileInfoBuilder.setFilename(filename);
        FileInfo readFileResult = metadataStub.readFile(fileInfoBuilder.build());

        if (readFileResult.getVersion() == 0) {
            System.out.println("Not Found");
        } else if (readFileResult.getBlocklistCount() == 1 && readFileResult.getBlocklist(0).equals("0")) {
            System.out.println("Not Found");
        } else {
            fileInfoBuilder.setVersion(readFileResult.getVersion() + 1);
            WriteResult deleteFileResult = metadataStub.deleteFile(fileInfoBuilder.build());
            System.out.println("OK");
        }
    }

    public void getversion(Namespace c_args) {
        String filepath = c_args.getString("file_path");
        String filename = filepath.substring(filepath.lastIndexOf("\\") + 1);

        FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
        fileInfoBuilder.setFilename(filename);
        FileInfo readFileResult = metadataStub.readFile(fileInfoBuilder.build());

        // if (readFileResult.getVersion() == 0) {
        //     System.out.println("Not Found");
        // } else {
        //     System.out.println(readFileResult.getVersion());
        // }

        System.out.println(readFileResult.getVersion());
        
    }

	private void go(Namespace c_args) {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");
        
        // TODO: Implement your client here

        String command = c_args.getString("command");
        
        switch(command) {
            case "upload":
                upload(c_args);
                // System.out.println("OK");
                break;
            case "download":
                download(c_args);
                // System.out.println("OK");
                break;
            case "delete":
                delete(c_args);
                // System.out.println("OK");
                break;
            case "getversion":
                getversion(c_args);
                // System.out.println("OK");
                break;
            default:
                System.out.println("Not Found");
        }
       
	}

	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class)
                .help("Operations on files");
        parser.addArgument("file_path").type(String.class)
                .help("Path to file");
        if (args.length == 4) {
        	parser.addArgument("destination").type(String.class)
                .help("Path to where to store file");
        }

        
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

        Client client = new Client(config);
        
        try {
        	client.go(c_args);
        } finally {
            client.shutdown();
        }
    }

}
