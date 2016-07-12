package org.apache.storm.hdfs.bolt.format;

import backtype.storm.task.TopologyContext;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Created by lcy on 2016/7/11.
 * files in one day will be in one folder
 */
public class FileFolderByDateNameFormat implements FileNameFormat {
    private String componentId;
    private int taskId;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public FileFolderByDateNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public FileFolderByDateNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public FileFolderByDateNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    public String getPath(){
        DateTime dt = new DateTime();
        String dateString = dt.toString("yyyy-MM-dd");
        return this.path + "/" + dateString;
    }
}
