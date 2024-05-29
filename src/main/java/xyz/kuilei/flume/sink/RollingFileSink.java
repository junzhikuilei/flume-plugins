package xyz.kuilei.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.PathManagerFactory;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Based on #{@link org.apache.flume.sink.RollingFileSink}'s decompiled
 *   code, customized code is in #{@link #stop()}
 *
 * @author JiaKun Xu, 2022-06-10
 */
public class RollingFileSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(RollingFileSink.class);
    private static final long defaultRollInterval = 30L;
    private static final int defaultBatchSize = 100;
    private int batchSize = 100;
    private File directory;
    private long rollInterval;
    private OutputStream outputStream;
    private ScheduledExecutorService rollService;
    private String serializerType;
    private Context serializerContext;
    private EventSerializer serializer;
    private SinkCounter sinkCounter;
    private PathManager pathController;
    private volatile boolean shouldRotate = false;

    public RollingFileSink() {
    }

    @Override
    public void configure(Context context) {
        String pathManagerType = context.getString("sink.pathManager", "DEFAULT");
        String directory = context.getString("sink.directory");
        String rollInterval = context.getString("sink.rollInterval");
        this.serializerType = context.getString("sink.serializer", "TEXT");
        this.serializerContext = new Context(context.getSubProperties("sink.serializer."));
        Context pathManagerContext = new Context(context.getSubProperties("sink.pathManager."));
        this.pathController = PathManagerFactory.getInstance(pathManagerType, pathManagerContext);
        Preconditions.checkArgument(directory != null, "Directory may not be null");
        Preconditions.checkNotNull(this.serializerType, "Serializer type is undefined");
        if (rollInterval == null) {
            this.rollInterval = defaultRollInterval;
        } else {
            this.rollInterval = Long.parseLong(rollInterval);
        }

        this.batchSize = context.getInteger("sink.batchSize", defaultBatchSize);
        this.directory = new File(directory);
        if (this.sinkCounter == null) {
            this.sinkCounter = new SinkCounter(this.getName());
        }

    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        this.sinkCounter.start();
        super.start();
        this.pathController.setBaseDirectory(this.directory);
        if (this.rollInterval > 0L) {
            this.rollService = Executors.newScheduledThreadPool(1, (new ThreadFactoryBuilder()).setNameFormat("rollingFileSink-roller-" + Thread.currentThread().getId() + "-%d").build());
            this.rollService.scheduleAtFixedRate(() -> {
                RollingFileSink.logger.debug("Marking time to rotate file {}", RollingFileSink.this.pathController.getCurrentFile());
                RollingFileSink.this.shouldRotate = true;
            }, this.rollInterval, this.rollInterval, TimeUnit.SECONDS);
        } else {
            logger.info("RollInterval is not valid, file rolling will not happen.");
        }

        logger.info("RollingFileSink {} started.", this.getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        if (this.shouldRotate) {
            logger.debug("Time to rotate {}", this.pathController.getCurrentFile());
            if (this.outputStream != null) {
                logger.debug("Closing file {}", this.pathController.getCurrentFile());

                try {
                    this.serializer.flush();
                    this.serializer.beforeClose();
                    this.outputStream.close();
                    this.sinkCounter.incrementConnectionClosedCount();
                    this.shouldRotate = false;
                } catch (IOException var19) {
                    this.sinkCounter.incrementConnectionFailedCount();
                    throw new EventDeliveryException("Unable to rotate file " + this.pathController.getCurrentFile() + " while delivering event", var19);
                } finally {
                    this.serializer = null;
                    this.outputStream = null;
                }

                this.pathController.rotate();
            }
        }

        if (this.outputStream == null) {
            File currentFile = this.pathController.getCurrentFile();
            logger.debug("Opening output stream for file {}", currentFile);

            try {
                this.outputStream = new BufferedOutputStream(new FileOutputStream(currentFile));
                this.serializer = EventSerializerFactory.getInstance(this.serializerType, this.serializerContext, this.outputStream);
                this.serializer.afterCreate();
                this.sinkCounter.incrementConnectionCreatedCount();
            } catch (IOException var18) {
                this.sinkCounter.incrementConnectionFailedCount();
                throw new EventDeliveryException("Failed to open file " + this.pathController.getCurrentFile() + " while delivering event", var18);
            }
        }

        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        Status result = Status.READY;

        try {
            transaction.begin();
            int eventAttemptCounter = 0;
            int i = 0;

            while (true) {
                if (i < this.batchSize) {
                    event = channel.take();
                    if (event != null) {
                        this.sinkCounter.incrementEventDrainAttemptCount();
                        ++eventAttemptCounter;
                        this.serializer.write(event);
                        ++i;
                        continue;
                    }

                    result = Status.BACKOFF;
                }

                this.serializer.flush();
                this.outputStream.flush();
                transaction.commit();
                this.sinkCounter.addToEventDrainSuccessCount((long) eventAttemptCounter);
                return result;
            }
        } catch (Exception var21) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to process transaction", var21);
        } finally {
            transaction.close();
        }
    }

    /**
     * Customized method
     */
    @Override
    public void stop() {
        logger.info("RollingFile sink {} stopping...", this.getName());
        this.sinkCounter.stop();
        super.stop();
        if (this.outputStream != null) {
            logger.debug("Closing file {}", this.pathController.getCurrentFile());

            try {
                this.serializer.flush();
                this.serializer.beforeClose();
                this.outputStream.close();
                this.sinkCounter.incrementConnectionClosedCount();
            } catch (IOException var7) {
                this.sinkCounter.incrementConnectionFailedCount();
                logger.error("Unable to close output stream. Exception follows.", var7);
            } finally {
                this.outputStream = null;
                this.serializer = null;
            }

            // customized code
            //   rename 临时后缀文件 to 结果后缀文件
            this.pathController.rotate();
            // END - customized code
        }

        if (this.rollInterval > 0L) {
            this.rollService.shutdown();

            while (!this.rollService.isTerminated()) {
                try {
                    this.rollService.awaitTermination(1L, TimeUnit.SECONDS);
                } catch (InterruptedException var6) {
                    logger.debug("Interrupted while waiting for roll service to stop. Please report this.", var6);
                }
            }
        }

        logger.info("RollingFile sink {} stopped. Event metrics: {}", this.getName(), this.sinkCounter);
    }

    public File getDirectory() {
        return this.directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public long getRollInterval() {
        return this.rollInterval;
    }

    public void setRollInterval(long rollInterval) {
        this.rollInterval = rollInterval;
    }
}

