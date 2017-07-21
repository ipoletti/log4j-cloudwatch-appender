package com.github.speedwing.log4j.cloudwatch.appender;


import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CloudwatchAppender extends AppenderSkeleton {

    private final Boolean DEBUG_MODE = System.getProperty("log4j.debug") != null;

    /**
     * The queue used to buffer log entries
     */
    private LinkedBlockingQueue<LoggingEvent> loggingEventsQueue;

    /**
     * the AWS Cloudwatch Logs API client
     */
    private AWSLogsClient awsLogsClient;

    private AtomicReference<String> lastSequenceToken = new AtomicReference<>();

    /**
     * The AWS Cloudwatch Log group name
     */
    private String logGroupName;

    /**
     * The AWS Cloudwatch Log stream name
     */
    private String logStreamName;

    /**
     * The queue / buffer size
     */
    private int queueLength = 1024;

    /**
     * The maximum number of log entries to send in one go to the AWS Cloudwatch Log service
     */
    private int messagesBatchSize = 128;

    private AtomicBoolean cloudwatchAppenderInitialised = new AtomicBoolean(false);

    public CloudwatchAppender() {
        super();
    }

    public CloudwatchAppender(Layout layout, String logGroupName, String logStreamName) {
        super();
        this.setLayout(layout);
        this.setLogGroupName(logGroupName);
        this.setLogStreamName(logStreamName);
        this.activateOptions();
    }

    public void setLogGroupName(String logGroupName) {
        this.logGroupName = logGroupName;
    }

    public void setLogStreamName(String logStreamName) {
        this.logStreamName = logStreamName;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }

    public void setMessagesBatchSize(int messagesBatchSize) {
        this.messagesBatchSize = messagesBatchSize;
    }

    @Override
    protected void append(LoggingEvent event) {
        if (cloudwatchAppenderInitialised.get()) {
            loggingEventsQueue.offer(event);
        } else {
            // just do nothing
        }
    }

    private synchronized void sendMessages() {
        LoggingEvent polledLoggingEvent;

        List<LoggingEvent> loggingEvents = new ArrayList<>();

        try {

            while ((polledLoggingEvent = loggingEventsQueue.poll()) != null && loggingEvents.size() <= messagesBatchSize) {
                loggingEvents.add(polledLoggingEvent);
            }

            List<InputLogEvent> inputLogEvents = new ArrayList<InputLogEvent>();
            for (LoggingEvent loggingEvent : loggingEvents) {
            	InputLogEvent awsLogEvent = new InputLogEvent().withTimestamp(loggingEvent.getTimeStamp()).withMessage(layout.format(loggingEvent));
            	inputLogEvents.add(awsLogEvent);
			}
            
            if (!inputLogEvents.isEmpty()) {

                PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(
                        logGroupName,
                        logStreamName,
                        inputLogEvents);

                try {
                    putLogEventsRequest.setSequenceToken(lastSequenceToken.get());
                    PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
                    lastSequenceToken.set(result.getNextSequenceToken());
                } catch (InvalidSequenceTokenException invalidSequenceTokenException) {
                    putLogEventsRequest.setSequenceToken(invalidSequenceTokenException.getExpectedSequenceToken());
                    PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
                    lastSequenceToken.set(result.getNextSequenceToken());
                    if (DEBUG_MODE) {
                        invalidSequenceTokenException.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            if (DEBUG_MODE) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() {
        while (loggingEventsQueue != null && !loggingEventsQueue.isEmpty()) {
            this.sendMessages();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        if (isBlank(logGroupName) || isBlank(logStreamName)) {
            Logger.getRootLogger().error("Could not initialise CloudwatchAppender because either or both LogGroupName(" + logGroupName + ") and LogStreamName(" + logStreamName + ") are null or empty");
            this.close();
        } else {
            this.awsLogsClient = new AWSLogsClient();
            loggingEventsQueue = new LinkedBlockingQueue<>(queueLength);
            try {
                initializeCloudwatchResources();
                initCloudwatchDaemon();
                cloudwatchAppenderInitialised.set(true);
            } catch (Exception e) {
                Logger.getRootLogger().error("Could not initialise Cloudwatch Logs for LogGroupName: " + logGroupName + " and LogStreamName: " + logStreamName, e);
                if (DEBUG_MODE) {
                    System.err.println("Could not initialise Cloudwatch Logs for LogGroupName: " + logGroupName + " and LogStreamName: " + logStreamName);
                    e.printStackTrace();
                }
            }
        }
    }

    private void initCloudwatchDaemon() {
        
        Runnable r = new Runnable() {
			public void run() {
	            while (true) {
	                try {
	                    if (loggingEventsQueue.size() > 0) {
	                        sendMessages();
	                    }
	                    Thread.sleep(20L);
	                } catch (InterruptedException e) {
	                    if (DEBUG_MODE) {
	                        e.printStackTrace();
	                    }
	                }
	            }
			}
		};
		new Thread(r).start();
    }

    private void initializeCloudwatchResources() {

        DescribeLogGroupsRequest describeLogGroupsRequest = new DescribeLogGroupsRequest();
        describeLogGroupsRequest.setLogGroupNamePrefix(logGroupName);

        List<LogGroup> logGroups = awsLogsClient
	        .describeLogGroups(describeLogGroupsRequest)
	        .getLogGroups();
	        
        
        boolean exists = false;
        for (LogGroup logGroup : logGroups) {
			if (logGroup.getLogGroupName().equals(logGroupName)) {
				exists = true;
				break;
			}
		}

        if (!exists) {
            CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest().withLogGroupName(logGroupName);
            awsLogsClient.createLogGroup(createLogGroupRequest);
        }

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(logGroupName).withLogStreamNamePrefix(logStreamName);

        List<LogStream> logStreams = awsLogsClient
        .describeLogStreams(describeLogStreamsRequest)
        .getLogStreams();
       
        boolean logStreamExists = false;
        for (LogStream logStream : logStreams) {
			if (logStream.getLogStreamName().equals(logStreamName)) {
				logStreamExists = true;
				break;
			}
		}

        if (!logStreamExists) {
            Logger.getLogger(this.getClass()).info("About to create LogStream: " + logStreamName + "in LogGroup: " + logGroupName);
            CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName);
            awsLogsClient.createLogStream(createLogStreamRequest);
        }

    }

    private boolean isBlank(String string) {
        return null == string || string.trim().length() == 0;
    }

}
