package xyz.kuilei.flume.formatter.output;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.formatter.output.PathManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author JiaKun Xu, 2022-06-09
 */
public class RollingPathManager implements PathManager {
    private static final Logger LOG = LoggerFactory.getLogger(RollingPathManager.class);

    private static final String FILE_PREFIX = "prefix";
    private static final String USE_CREATION_TIMESTAMP = "useCreationTimestamp";
    private static final String CREATION_TIMESTAMP_PATTERN = "creationTimestampPattern";
    private static final String FILE_EXTENSION = "extension";
    private static final String FILE_IN_USE_EXTENSION = "inUseExtension";

    private static final String DEFAULT_FILE_PREFIX = "";
    private static final boolean DEFAULT_USE_CREATION_TIMESTAMP = false;
    private static final String DEFAULT_CREATION_TIMESTAMP_PATTERN = "";
    private static final String DEFAULT_FILE_EXTENSION = "";
    private static final String DEFAULT_FILE_IN_USE_EXTENSION = "tmp";

    private final long seriesTimestamp;
    private final boolean useCreationTimestamp;
    private final String creationTimestampPattern;

    protected File currentFile;

    private File baseDirectory;
    private final String filePrefix;
    /** new added compare to #{@link org.apache.flume.formatter.output.DefaultPathManager} */
    private long creationTimestamp;
    private final AtomicInteger fileIndex;
    private final String extension;
    /** new added */
    private final String inUseExtension;

    public RollingPathManager(Context context) {
        seriesTimestamp = System.currentTimeMillis();
        useCreationTimestamp = context.getBoolean(USE_CREATION_TIMESTAMP, DEFAULT_USE_CREATION_TIMESTAMP);
        creationTimestampPattern = context.getString(CREATION_TIMESTAMP_PATTERN, DEFAULT_CREATION_TIMESTAMP_PATTERN);

        filePrefix = context.getString(FILE_PREFIX, DEFAULT_FILE_PREFIX);
        creationTimestamp = seriesTimestamp;
        fileIndex = new AtomicInteger();
        extension = context.getString(FILE_EXTENSION, DEFAULT_FILE_EXTENSION);
        inUseExtension = context.getString(FILE_IN_USE_EXTENSION, DEFAULT_FILE_IN_USE_EXTENSION);
    }

    @Override
    public File nextFile() {
        creationTimestamp = System.currentTimeMillis();

        StringBuilder sb = new StringBuilder();
        sb.append(filePrefix);
        if (useCreationTimestamp) {
            sb.append(
                creationTimestampPattern.length() > 0 ?
                DateFormatUtils.format(creationTimestamp, creationTimestampPattern) :
                creationTimestamp
            );
        } else {
            sb.append(seriesTimestamp);
        }
        sb.append("-");
        sb.append(fileIndex.incrementAndGet());
        if (extension.length() > 0) {
            sb.append(".").append(extension);
        }
        if (inUseExtension.length() > 0) {
            sb.append(".").append(inUseExtension);
        }

        File srcFile = new File(baseDirectory, sb.toString());
        currentFile = srcFile;
        return srcFile;
    }

    @Override
    public File getCurrentFile() {
        File srcFile = currentFile;

        return (srcFile == null) ? nextFile() : srcFile;
    }

    @Override
    public void rotate() {
        File srcFile = currentFile;
        currentFile = null;

        if (inUseExtension.length() > 0) {
            String srcFileName = srcFile.getName();
            String dstFileName = srcFileName.substring(0, srcFileName.lastIndexOf(inUseExtension) - 1);

            File dstFile = new File(baseDirectory, dstFileName);

            String srcPath = srcFile.getAbsolutePath();
            String dstPath = dstFile.getAbsolutePath();

            try {
                FileUtils.moveFile(srcFile, dstFile);
                LOG.info(String.format("Rename %s to %s success", srcPath, dstPath));
            } catch (IOException ioE) {
                LOG.error(String.format("Failed to rename %s to %s", srcPath, dstPath), ioE);
            }
        }
    }

    @Override
    public File getBaseDirectory() {
        return baseDirectory;
    }

    @Override
    public void setBaseDirectory(File baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    public String getPrefix() {
        return filePrefix;
    }

    public String getExtension() {
        return extension;
    }

    public String getInUseExtension() {
        return inUseExtension;
    }

    public long getSeriesTimestamp() {
        return seriesTimestamp;
    }

    public AtomicInteger getFileIndex() {
        return fileIndex;
    }

    /**
     * NO delete the builder!
     */
    public static class Builder implements PathManager.Builder {
        public Builder() {}

        @Override
        public PathManager build(Context context) {
            return new RollingPathManager(context);
        }
    }
}
