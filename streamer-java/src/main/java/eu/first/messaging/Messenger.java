package eu.first.messaging;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;


/**
 * @author mkalender
 * 
 *         The messenger class sends and receives messages
 */
public class Messenger {

	// used for load balancing
	private static int ID = 1;
	private static int producerNum = 1;
	private static int receiverNum = 1;
	private static float ratio = 1;
	private HashSet<Integer> lbSet = new HashSet<Integer>();

	private BlockingQueue<String> waitIndicator = new LinkedBlockingQueue<String>(1);

	
	// configuration variables
	private static Logger logger = Logger.getLogger(Messenger.class.getName());
	private static XMLConfiguration configuration;
	private static String CONFIG_FILE_NAME = "resources" + File.separator + "config" + File.separator + "config.xml";
	private static String LOG_CONF_FILE_NAME = "resources" + File.separator + "config" + File.separator + "log4j.xml";


	// keeps received messages
	private BlockingQueue<String> incomingMessageQueue;
	private BlockingQueue<String> outgoingMessageQueue;
	private BlockingQueue<String> fileQueue; // keeps file names

	// extra queue for logging facilities
	private BlockingQueue<String> loggerQueue;
	// zeromq connection sockets
	private ZMQ.Context context;
	private ZMQ.Socket receiver;
	private ZMQ.Socket sender;
	private ZMQ.Socket lbPublisher;
	private ZMQ.Socket lbReceiver;
	private ZMQ.Socket finishSender;
	private ZMQ.Socket finishReceiver;

	// channel for logging output (DB_LOGGING)
	private ZMQ.Socket loggerEmitter;
	private ZMQ.Socket monitorSocket;
	
	// activemq variables
	// private MessageConsumer consumer;
	// private Connection activeMQconnection;

	// messaging threads
	private MessageListener messageListener;
	private MessageSender messageSender;
	private MonitoringThread monitoringThread;

	private FileThread fileThread;
	private FinishThread finishThread;
	private Thread loggerThread;

	// messaging variables
	private static int MESSAGING_TYPE = 1;
	private static final int PIPELINE = 0;
	private static final int REQ_REP = 1;
	private static int BROKER = 0;
	private static final int NONE = 0;
	private static final int ACTIVEMQ = 1;
	private static final int RABBITMQ = 2;
	private static String MESSAGE_REQUEST = "R";
	private static String WAIT_COMMAND = "WAIT";
	public static String FINISH_COMMAND = "FINISH";
	private static String CONTINUE_COMMAND = "CONTINUE";
	private static boolean BLOCKING_QUEUE = false;
	private static boolean IGNORE_QUEUE_OVERFLOW = false;

	private static int BLOCK_ON_SEND = 0;

	private int MAX_QUEUE_SIZE = 100; // maximum queue size before sending wait
										// message
	private int MIN_QUEUE_SIZE = 10; // minimum queue size to send continue
										// message

	private int outFileNum = 1; // keeps file counter
	private int inFileNum = 1; // keeps file counter
	private String outFileStorageAddress; // file storage folder
	private String inFileStorageAddress; // file storage folder
	private int maxFileStorageNum; // max number of messages can be stored in
									// files

	private static boolean DB_Logging = false;
	private static boolean DB_Logging_LocalWrite = false;
	private static boolean messagingFinished = false;

	private long recvCounter = 0L;
	private long recvQueueCounter = 0L;
	private long sendCounter = 0L;
	private long sendQueueCounter = 0L;
	
	// for testing
	public static void main(String[] args) {
		try {

			double messageNum = 0;
			int messageTest = 3;
			int delay = 0;

			if (args.length > 0 && args[0] != null) {
				messageTest = Integer.valueOf(args[0]);
			}

			if (args.length > 1 && args[1] != null) {
				delay = Integer.valueOf(args[1]);
			}

			if (args.length > 2 && args[2] != null) {
				CONFIG_FILE_NAME = "resources" + File.separator + "config" + File.separator + args[2];

			}
			// Creates messaging thread
			Messenger messenger = new Messenger();
			FileWriter outFile = new FileWriter("message.txt");
			PrintWriter out = new PrintWriter(outFile);

			switch (messageTest) {
			// Receives messages until the finish command
			case 1:
				while (true) {
					String message = messenger.getMessage();
					if (message != null) {
						// FileWriter doc = new
						// FileWriter("D:\\streamer\\received\\message"+messageNum+".xml");
						// doc.write(message);
						// doc.close();
						Thread.sleep(delay);
						messageNum++;
						logger.info("Gets message number:" + messageNum);
					}
					if (Messenger.isMessagingFinished()) {
						break;
					}

				}
				out.close();
				logger.info("Total received messages:" + messageNum);
				break;

			// Sends test messages
			case 2:
				for (int i = 0; i < 10000; i++) {
					messageNum++;
					messenger.sendMessage(String.valueOf(messageNum));
					logger.info("Sends message number:" + messageNum);
					Thread.sleep(delay);
				}
				Thread.sleep(4000);
				messenger.stopMessaging();
				out.close();
				logger.info("Total sent messages:" + messageNum);
				break;

			case 3:
				// Sends files in the dataset to the message listener
				File inDir = new File("Dataset");
				if (inDir.exists() == false) {
					inDir.mkdir();
				}
				String[] children = inDir.list();
				if (children == null) {
					// Either dir does not exist or is not a directory
				} else {
					for (int i = 0; i < children.length; i++) {
						// Get filename of file or directory
						String filename = children[i];
						messageNum++;
						messenger.sendMessage(messenger.readFileAsString("Dataset" + File.separator + filename));
						logger.info("Sends message number:" + messageNum);
						Thread.sleep(delay);
					}
				}
				messenger.stopMessaging();
				out.close();
				logger.info("Total sent messages:" + messageNum);
				break;

			// Sends received messages
			case 4:
				while (true) {
					// Gets sent messages from the WP3, waits until a message is
					// received
					String message = messenger.getMessage();
					if (message != null) {
						messageNum++;
						logger.info("Gets message number:" + messageNum);
						messenger.sendMessage(message);
						logger.info("Sends message number:" + messageNum);
						Thread.sleep(delay);

					}
					if (Messenger.isMessagingFinished()) {
						break;
					}

				}
				out.close();
				logger.info("Total received messages:" + messageNum);
				break;

			// Sends files in the dataset to the message listener in a loop
			case 5:
				while (true) {
					File inDir2 = new File("Dataset");
					if (inDir2.exists() == false) {
						inDir2.mkdir();
					}
					String[] children2 = inDir2.list();
					if (children2 == null) {
						// Either dir does not exist or is not a directory
					} else {
						for (int i = 0; i < children2.length; i++) {
							// Get filename of file or directory
							String filename = children2[i];
							messenger.sendMessage(messenger.readFileAsString("Dataset" + File.separator + filename));
							logger.info("Sends message number:" + messageNum++);
							Thread.sleep(delay);
						}
					}
				}
			default:
				break;
			}

		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (Exception e) {
			logger.error(e);
		}
	}

	public Messenger() {

		init();
	}

	/**
	 * Initialize messaging settings
	 */
	public void init() {
		initLogger();
		initConfiguration();
		initMessaging();
	}

	/**
	 * Initialize Zeromq socket types
	 */
	public void initMessaging() {
		incomingMessageQueue = new LinkedBlockingQueue<String>();
		outgoingMessageQueue = new LinkedBlockingQueue<String>();
		fileQueue = new LinkedBlockingQueue<String>();

		loggerQueue = new LinkedBlockingQueue<String>();
		context = ZMQ.context(1);

		// reading variables from the configuration file
		MESSAGING_TYPE = configuration.getInt("messaging.MessagingType");
		MIN_QUEUE_SIZE = configuration.getInt("messaging.MIN_QUEUE_SIZE");
		MAX_QUEUE_SIZE = configuration.getInt("messaging.MAX_QUEUE_SIZE");
		BLOCKING_QUEUE = configuration.getBoolean("messaging.BLOCKING_QUEUE");
		MESSAGE_REQUEST = configuration.getString("messaging.MESSAGE_REQUEST");
		WAIT_COMMAND = configuration.getString("messaging.WAIT_COMMAND");
		FINISH_COMMAND = configuration.getString("messaging.FINISH_COMMAND");
		CONTINUE_COMMAND = configuration.getString("messaging.CONTINUE_COMMAND");
		IGNORE_QUEUE_OVERFLOW = configuration.getBoolean("messaging.IGNORE_QUEUE_OVERFLOW");
		// blocking on send (for stream simulation)
		BLOCK_ON_SEND = configuration.getInt("messaging.BLOCK_ON_SEND");

		if (BLOCK_ON_SEND == 1) {
			outgoingMessageQueue = new LinkedBlockingQueue<String>(3);
		} else {
			//we need to limit the max size of the queue to enable messages dropping
			incomingMessageQueue = new LinkedBlockingQueue<String>(MAX_QUEUE_SIZE);
		}

		outFileStorageAddress = configuration.getString("messaging.outFileStorageAddress");
		inFileStorageAddress = configuration.getString("messaging.inFileStorageAddress");
		maxFileStorageNum = configuration.getInt("messaging.MAX_FILE_STORAGE_SIZE");

		ID = configuration.getInt("messaging.ID");
		receiverNum = configuration.getInt("messaging.ReceiverNumber");
		producerNum = configuration.getInt("messaging.ProducerNumber");
		ratio = (float) producerNum / receiverNum;

		DB_Logging = configuration.getBoolean("messaging.logging.DBLogging");
		DB_Logging_LocalWrite = configuration.getBoolean("messaging.logging.DBLoggingLocalWrite");

		if (DB_Logging == true) {
			String loggingSendAddress = configuration.getString("messaging.logging.DBLoggingReceiver");
			loggerEmitter = context.socket(ZMQ.PUSH);
			loggerEmitter.bind(loggingSendAddress);
			loggerThread = new LoggerThread();
			loggerThread.start();
		}
		
		//monitoring socket (server mode)
		String monitoringSocketAddress = configuration.getString("messaging.connection.MonitoringSocket");
		if (monitoringSocketAddress != null) {
			monitorSocket = context.socket(ZMQ.REP);
			monitorSocket.bind(monitoringSocketAddress);
			monitoringThread = new MonitoringThread();
			monitoringThread.start();
			System.out.println("Monitoring starts ");			
		}
		
		// zeromq opening connections
		switch (MESSAGING_TYPE) {
		case PIPELINE:
			// Receive messages
			String receiveAddress = configuration.getString("messaging.connection.MessageReceiveAddress");
			if (receiveAddress != null) {
				receiver = context.socket(ZMQ.PULL);
				String[] addresses = receiveAddress.split(" ");
				for (String address : addresses) {
					receiver.connect(address);
				}
				// Publish overflow messages to message producer
				lbPublisher = context.socket(ZMQ.PUB);
				lbPublisher.bind(configuration.getString("messaging.connection.SendLoadBalancingAddress"));

				messageListener = new MessageListener();
				messageListener.start();

				readOldIncomingMessageFiles();
			}

			String sendAddress = configuration.getString("messaging.connection.MessageSendAddress");
			if (sendAddress != null) {
				sender = context.socket(ZMQ.PUSH);
				sender.bind(sendAddress);
				// Publish overflow messages
				lbReceiver = context.socket(ZMQ.SUB);
				String[] addresses = configuration.getString("messaging.connection.ReceiveLoadBalancingAddress").split(
						" ");
				for (String address : addresses) {
					lbReceiver.connect(address);
				}
				lbReceiver.subscribe(configuration.getString("messaging.connection.RECEIVE_COMMAND_FILTER").getBytes());

				messageSender = new MessageSender();
				messageSender.start();

				if (IGNORE_QUEUE_OVERFLOW == false) {
					// Enables file storage for handling data peaks
					fileThread = new FileThread();
					// Reads previously written messages if they are not sent to
					// WP4
					readOldOutgoingdMessageFiles();
					fileThread.start();
				}
			}

			break;

		case REQ_REP:
			String receiveAddress2 = configuration.getString("messaging.connection.MessageReceiveAddress");
			if (receiveAddress2 != null) {
				receiver = context.socket(ZMQ.REQ);
				String[] addresses = receiveAddress2.split(" ");
				for (String address : addresses) {
					receiver.connect(address);
				}
				messageListener = new MessageListener();
				messageListener.start();

				readOldIncomingMessageFiles();
			}

			String sendAddress2 = configuration.getString("messaging.connection.MessageSendAddress");
			if (sendAddress2 != null) {
				sender = context.socket(ZMQ.REP);
				sender.bind(sendAddress2);
				messageSender = new MessageSender();
				messageSender.start();

				if (IGNORE_QUEUE_OVERFLOW == false) {
					// Enables file storage for handling data peaks
					fileThread = new FileThread();
					// Reads previously written messages if they are not sent to
					// WP4
					readOldOutgoingdMessageFiles();
					fileThread.start();
				}
			}
			break;
		}

		String finishPublishAddress = configuration.getString("messaging.connection.FinishPublish");
		String finishReceiveAddress = configuration.getString("messaging.connection.FinishReceive");
		if (finishPublishAddress != null) {
			finishSender = context.socket(ZMQ.PUB);
			finishSender.bind(finishPublishAddress);
		} else {
			finishReceiver = context.socket(ZMQ.SUB);
			finishReceiver.connect(finishReceiveAddress);
			finishReceiver.subscribe(configuration.getString("messaging.connection.RECEIVE_COMMAND_FILTER").getBytes());
			finishThread = new FinishThread();
			finishThread.start();
		}

		// Message broker configuration
		// BROKER = configuration.getInt("messaging.Broker");
		// if ( BROKER == ACTIVEMQ ) {
		// ActiveMQConnectionFactory connectionFactory = new
		// ActiveMQConnectionFactory(
		// configuration.getString("messaging.connection.ACTIVEMQ"));
		// try {
		// activeMQconnection = connectionFactory.createConnection();
		// activeMQconnection.start();
		// Session session =
		// activeMQconnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
		// Destination destination =
		// session.createQueue(configuration.getString("messaging.connection.QueueName"));
		// consumer = session.createConsumer(destination);
		// } catch (JMSException e) {
		// logger.error(e);
		// }
		// //disables broker based messaging if connection cannot be established
		// if (consumer == null) {
		// BROKER = NONE;
		// }
		// }
	}

	/**
	 * Initiliaze config file
	 */
	public void initConfiguration() {
		getConfiguration();
	}

	/**
	 * Loads config.xml file
	 * 
	 * @return XML configuration setting
	 */
	private static XMLConfiguration getConfiguration() {
		if (configuration == null) {
			try {
				configuration = new XMLConfiguration(CONFIG_FILE_NAME);
			} catch (ConfigurationException e) {
				logger.error(e.toString());
			}
		}
		return configuration;
	}

	/**
	 * Initialize logging using log4j.xml file
	 */
	public void initLogger() {
		DOMConfigurator.configure(LOG_CONF_FILE_NAME);
	}

	/**
	 * @return the first message from the queue
	 */
	public String getMessage() {
		try {
			if (BLOCKING_QUEUE == true) {
				String message = incomingMessageQueue.take();
				Thread.sleep(1);
				return message;
			} else {
				String message = incomingMessageQueue.poll();
				Thread.sleep(1);
				return message;
			}
		} catch (Exception e) {
			logger.error(e.toString());
		}
		return null;
	}

	/**
	 * @param message
	 *            - message content Sends messages to message consumer
	 */
	public void sendMessage(String message) {
		try {
			sendQueueCounter++;
			// sends message with zeromq if queue doesn't exceed the max
			if (outgoingMessageQueue.size() < MAX_QUEUE_SIZE) {
				if (BLOCK_ON_SEND == 1) {
					outgoingMessageQueue.put(message);
				} else {
					outgoingMessageQueue.add(message);

				}

			}
			// writes message to file for later consumption
			else if (IGNORE_QUEUE_OVERFLOW == false) {
				writeOutgoingMessageToFile(message);

			}
			// ignore the message
			else {
				logger.debug("Message ignored");
			}

			Thread.sleep(1);
		} catch (Exception e) {
			logger.error(e.toString());
		}

	}

	/**
	 * Publishes stop command and messaging terminates
	 */
	public void stopMessaging() {
		finishSender.send((FINISH_COMMAND).getBytes(), 0);
		setMessagingFinished(true);
	}

	/**
	 * @param message
	 *            - message content When messaging terminates, messages in the
	 *            incoming queue are written to files
	 */
	private void writeIncomingMessageToFile(String message) {
		writeFile(message, inFileStorageAddress + "\\" + inFileNum++);
	}

	/**
	 * @param message
	 *            - message content When messaging terminates or outgoing queue
	 *            gets overflow, messages in the outgoing queue are written to
	 *            files
	 */
	private void writeOutgoingMessageToFile(String message) {
		// keeps old message file and ignores the new message if file queue
		// limit is exceeded
		logger.debug(ID + " writeOutgoingMessageToFile ");
		if (!fileQueue.contains(String.valueOf(outFileNum))) {
			writeFile(message, outFileStorageAddress + "\\" + outFileNum);
			fileQueue.add(String.valueOf(outFileNum));
			// reset counter
			if (outFileNum >= maxFileStorageNum) {
				outFileNum = 1;
			} else {
				outFileNum++;
			}
			logger.debug(ID + " messagewritten ");
		}

	}

	class MessageSender2 extends Thread {
		public MessageSender2() {
			
		}

		public void run() {
			
//			switch (MESSAGING_TYPE) {
//			case PIPELINE:
//				while (!isMessagingFinished()) {
//					try {
//						waitIndicator.poll(100, TimeUnit.MILLISECONDS);
//						String value = (String) outgoingMessageQueue.poll();
//						// Sends the message
//						sender.send(value.getBytes(), 0);
//					
//					
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					
//				}
//				break;
//			}
		}
	}
	
	/**
	 * @author mkalender Message sending thread
	 */
	class MessageSender extends Thread {
		/**
		 * Messaging constructor
		 */
		public MessageSender() {

		}

		public void run() {

			logger.debug("sender messaging type: " + MESSAGING_TYPE);
			double messageNum = 0;
			boolean wait = false;
			double check = 0;
			switch (MESSAGING_TYPE) {
			case PIPELINE:
				while (!isMessagingFinished()) {
					try {
						// gets load balancing commands
						byte[] command = lbReceiver.recv(1);
//						check++;
						if (command != null) {
							String commandString = new String(command);
							if (commandString.startsWith(WAIT_COMMAND)) {
								int clientNum = Integer.parseInt(commandString.split(" ")[1]);
								lbSet.add(new Integer(clientNum));
								logger.debug(ID + " wait message is received from : " + clientNum);
							} else if (commandString.startsWith(CONTINUE_COMMAND)) {
								int clientNum = Integer.parseInt(commandString.split(" ")[1]);
								lbSet.remove(new Integer(clientNum));
								logger.debug(ID + " continue message is received from : " + clientNum);
								
							}

							/*
							 * Message consumers publishes overflow message to
							 * all message producers when they can't consume all
							 * messages. When there are multiple consumers and
							 * producers in the pipeline, we need a scheduling
							 * algorithm. We assume that message consumers are
							 * identical and consume similar amount of messages
							 * at a time. All overflow messages are sent to each
							 * message producers therefore every producer knows
							 * how many of the consumers get overflow. Based on
							 * the ID values of the message producers they stop
							 * sending messages. For instance if there are 4
							 * producers and 2 consumers, when 1 consumer sends
							 * overflow message, corresponding 2 message
							 * producers with ID values 1 and 2 would stop
							 * sending messages. Similarly for the 2 producer
							 * and 4 consumer situation, when 1 consumer sends
							 * overflow message all producers would continue
							 * messaging. When 2nd overflow message is sent,
							 * producer with ID value 1 would stop sending
							 * messages.
							 */
							if (lbSet.size() * ratio >= ID) {
								wait = true;
								//System.out.println("     WAIT! (" +lbSet.size() +")");
							} else {
								wait = false;
								logger.debug(ID + " running mode : ");
								//System.out.println("     GO! (" +lbSet.size() +")");
							}

						}
						if (wait == true) {
							Thread.sleep(outgoingMessageQueue.size() * 10);
							logger.debug(ID + " wait mode, size : " + lbSet.size());
						}
						// Gets message from the queue added by the WP3
						else if (outgoingMessageQueue.size() > 0) {
							String value = (String) outgoingMessageQueue.poll();
							// Sends the message
							sender.send(value.getBytes(), 0);
							logger.debug("message is sent over network: " + messageNum++);
							sendCounter++;
						}
						
						if (outgoingMessageQueue.size() == 0) {
							Thread.sleep(100);
						}
//						if ((check % 100) == 0) {
//							System.out.println("lbset: " + lbSet.size());
//						}
					} catch (Exception e) {
						logger.error(e);
					}
				}
				while (outgoingMessageQueue.size() > 0) {
					writeOutgoingMessageToFile(outgoingMessageQueue.poll());
				}
				break;
			case REQ_REP:
				while (!isMessagingFinished()) {
					try {
						// Sends request message
						logger.debug("Before Request message");
						String message = "";
						try {
							receiver.send(MESSAGE_REQUEST.getBytes(), 0);
							logger.debug("Request message is sent");
							// Gets message from WP3
							logger.debug("Before receive message");
							byte[] mes2 = receiver.recv(1);
							if (mes2 != null) {
								message = new String(mes2);
								incomingMessageQueue.put(message);
								logger.debug("message received: " + messageNum++);
							}
						} catch (ZMQException e) {
							byte[] mes2 = receiver.recv(1);
							if (mes2 != null) {
								message = new String(mes2);
								incomingMessageQueue.put(message);
								logger.debug("message received: " + messageNum++);
							}
						}
						// to give context to the main thread
						Thread.sleep(1);
					} catch (InterruptedException e) {
						logger.error(e.toString());
					}

				}
				break;
			}
			sender.close();
			lbReceiver.close();
			finishSender.close();
			context.term();
		}

	}

	/**
	 * @author mkalender Reads message files written to files for later
	 *         consumption
	 */
	class FileThread extends Thread {
		/**
		 * Messaging constructor
		 */
		public FileThread() {

		}

		public void run() {
			while (!isMessagingFinished()) {
				try {
					// if there are no new messages in the queue, reads messages
					// from the file storage
					if (outgoingMessageQueue.size() < 5 && fileQueue.size() > 0) {
						String message = readOutGoingMessageFile();
						outgoingMessageQueue.add(message);
						Thread.sleep(1);
					} else {
						Thread.sleep(outgoingMessageQueue.size() * 10);
					}

					if (isMessagingFinished() && fileQueue.size() == 0) {
						return;
					}

				} catch (Exception e) {
					logger.error(e);
					fileQueue.clear();
					return;
				}
			}
		}
	}

	/*
	 * Logger thread
	 */
	class LoggerThread extends Thread {

		public LoggerThread() {

		}

		public void run() {
			while (true) {
				try {
					// if (loggerQueue == null) {
					// System.out.println("lol wut?");
					// }
					// if (loggerQueue.size() != 0) {
					// System.out.println("queue OK!");
					String value = (String) loggerQueue.poll();
					loggerEmitter.send(value.getBytes(), 0);
					// logger.Debug("Message is sent for logging");
					Thread.sleep(1);
					if (messagingFinished && loggerQueue.size() == 0) {
						loggerEmitter.send(FINISH_COMMAND.getBytes(), 0);
						return;
					}

					// } else {
					// //System.out.println("queue empty!");
					// }
				} catch (Exception e) {
					// disables logging type messaging
					// DB_Logging = false;
					// loggerQueue.clear();
					// logger.error(e);
					// IGNORE_QUEUE_OVERFLOW = true;
					// return;
				}
			}
		}
	}

	/**
	 * 
	 * Monitoring thread
	 * 
	 * @author A182637
	 *
	 */
	class MonitoringThread extends Thread {
		
		public MonitoringThread() {

		}
		
		public void run() {
			while (!isMessagingFinished()) {
				try {
					byte[] reqMsg = monitorSocket.recv(0);
					if (reqMsg != null) {
						String req = new String(reqMsg);
						if (req.equalsIgnoreCase("stats")) {
							//System.out.println("Received: " + req);	
							String response = recvCounter + " " + recvQueueCounter + " " + sendCounter + " " + sendQueueCounter;
							monitorSocket.send(response.getBytes(), 0);							
						}
					} else {
						System.out.println("nothing....");
					}
					Thread.sleep(200);
				} catch (InterruptedException e) {
					System.out.println(e);
				} catch (ZMQException e) {
					System.out.println(e);
				}
			}
		}
		
	}
	
	
	/**
	 * @author mkalender Thread listens for the finish command
	 */
	class FinishThread extends Thread {
		/**
		 * Messaging constructor
		 */
		public FinishThread() {

		}

		public void run() {
			byte[] command = finishReceiver.recv(0);
			setMessagingFinished(true);
		}
	}

	/**
	 * @author mkalender Message receiving thread
	 */
	class MessageListener extends Thread {

		/**
		 * Messaging constructor
		 */
		public MessageListener() {

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run() Messaging thread main method
		 */
		public void run() {

			logger.debug("Messaging type: " + MESSAGING_TYPE);

			double messageNum = 0;// keeps number of retrieved messages

			switch (MESSAGING_TYPE) {
			case PIPELINE:

				boolean wait = false;
				String message = null;

				logger.debug("starts receiving messages");

				// continue message is sent just in case message producer is in
				// a wait mode
				lbPublisher.send((CONTINUE_COMMAND + " " + ID).getBytes(), 0);

				while (!isMessagingFinished()) {
					try {
						// load balancing, sends wait and continue messages to
						// message producers
						if (incomingMessageQueue.size() >= MAX_QUEUE_SIZE && BLOCK_ON_SEND == 1) {
							logger.debug(ID + "Wait message is sent");
							lbPublisher.send((WAIT_COMMAND + " " + ID).getBytes(), 0);
							// waits until messages are consumed
							Thread.sleep(incomingMessageQueue.size());
							wait = true;
							// logger.info("(1)");
						} else if (wait == true && incomingMessageQueue.size() == 0) {
							wait = false;
							logger.debug(ID + " Continue message is sent");
							lbPublisher.send((CONTINUE_COMMAND + " " + ID).getBytes(), 0);
							Thread.sleep(10);
							// logger.info("(2)");
						} else if (wait == false && incomingMessageQueue.size() == 0) {
							logger.debug(ID + " Continue message is sent");
							lbPublisher.send((CONTINUE_COMMAND + " " + ID).getBytes(), 0);
							Thread.sleep(10);
							// logger.info("(3)");
						}
						
						// gets all sent messages by WP3.
						// Returns null when there is no message. When input is
						// 0, it blocks the application
						// until a message is retrieved. We don't use it because
						// it cause problems when
						// application failures occur.
						byte[] mes = receiver.recv(1);
						if (mes != null) {
							recvCounter++;
							message = new String(mes);
							if (BLOCK_ON_SEND == 1) {
								incomingMessageQueue.put(message);
								logger.debug("message received: " + messageNum++);								
							} else {
								//in the true pipeline mode we don't care if message is delivered or not. 
								//In case the queue is full, we can drop it.
								if (incomingMessageQueue.offer(message) == true) {
									logger.debug("message received: " + messageNum++);	
									recvQueueCounter++;
								} else {
									logger.debug("message dropped: " + messageNum++);
								}
							}

						}



						// to give context to the main thread
						Thread.sleep(1);
					} catch (InterruptedException e) {
						logger.error(e.toString());
						System.out.println("ERROR " + e);
					}

				}
				// after receiving the finish command incoming messages are
				// written to files
				while (incomingMessageQueue.size() > 0) {
					writeIncomingMessageToFile(incomingMessageQueue.poll());
				}
				break;

			case REQ_REP:
				while (!isMessagingFinished()) {
					try {
						// Sends request message
						logger.debug("Before Request message");
						message = "";
						try {
							receiver.send(MESSAGE_REQUEST.getBytes(), 0);
							logger.debug("Request message is sent");
							// Gets message from WP3
							logger.debug("Before receive message");
							byte[] mes2 = receiver.recv(1);
							if (mes2 != null) {
								message = new String(mes2);
								incomingMessageQueue.put(message);
								logger.debug("message received: " + messageNum++);
							}
						} catch (ZMQException e) {
							receiver.close();
							String receiveAddress2 = configuration
									.getString("messaging.connection.MessageReceiveAddress");
							if (receiveAddress2 != null) {
								receiver = context.socket(ZMQ.REQ);
								String[] addresses = receiveAddress2.split(" ");
								for (String address : addresses) {
									receiver.connect(address);
								}
							}
						}
						// to give context to the main thread
						Thread.sleep(1);
					} catch (InterruptedException e) {
						logger.error(e.toString());
					}

				}
				break;
			}
			receiver.close();
			lbPublisher.close();
			finishReceiver.close();
			context.term();
		}

	}

	/**
	 * @param message
	 * @param filename
	 *            Writes a message to a file
	 */
	public void writeFile(String message, String filename) {
		try {
			// Create file
			FileWriter fstream = new FileWriter(filename);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(message);
			// Close the output stream
			out.close();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
	}

	/**
	 * @return Reads a message file
	 */
	private String readOutGoingMessageFile() {
		String fileName = (String) fileQueue.peek();
		String content = readFileAsString(outFileStorageAddress + "\\" + fileName);
		File f = new File(outFileStorageAddress + "\\" + fileName);
		f.delete();
		fileQueue.remove();
		return content;
	}

	/**
	 * @return Reads a message file
	 * 
	 */
	private String readIncomingMessageFile(String fileName) {
		String content = readFileAsString(inFileStorageAddress + "\\" + fileName);
		File f = new File(inFileStorageAddress + "\\" + fileName);
		f.delete();
		return content;
	}

	/**
	 * @return Reads a message file
	 * 
	 */
	public String readFileAsString(String filePath) {
		// filePath = "messages" + File.separator + filePath;
		File file = new File(filePath);
		byte[] buffer = new byte[(int) file.length()];
		BufferedInputStream f = null;
		try {
			f = new BufferedInputStream(new FileInputStream(filePath));
			f.read(buffer);
			f.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new String(buffer);
	}

	/*
	 * Reads previously written messages
	 */
	private void readOldOutgoingdMessageFiles() {
		File outDir = new File(outFileStorageAddress);
		if (outDir.exists() == false) {
			outDir.mkdir();
		}

		String[] children = outDir.list();
		if (children == null) {
			// Either dir does not exist or is not a directory
		} else {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];
				fileQueue.add(filename);
				int num = Integer.parseInt(filename);
				if (num >= outFileNum) {
					outFileNum = num + 1;
				} else {
					outFileNum++;
				}
			}
		}

	}

	private void readOldIncomingMessageFiles() {

		File inDir = new File(inFileStorageAddress);
		if (inDir.exists() == false) {
			inDir.mkdir();
		}
		String[] children = inDir.list();
		if (children == null) {
			// Either dir does not exist or is not a directory
		} else {
			for (int i = 0; i < children.length; i++) {
				// Get filename of file or directory
				String filename = children[i];
				incomingMessageQueue.add(readIncomingMessageFile(filename));
			}
		}

	}

	public static boolean isMessagingFinished() {
		return messagingFinished;
	}

	private static void setMessagingFinished(boolean messagingFinished) {
		Messenger.messagingFinished = messagingFinished;
	}

}
