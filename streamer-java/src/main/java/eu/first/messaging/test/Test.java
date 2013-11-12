package eu.first.messaging.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;
import eu.first.messaging.Messenger;

public class Test {
	private static Logger logger = Logger.getLogger(Test.class.getName());

	public static void main(String[] args) {
		try {
			// Creates messaging thread
			Messenger messenger = new Messenger();
			FileWriter outFile = new FileWriter("message.txt");
			PrintWriter out = new PrintWriter(outFile);
			double messageNum = 0;
			int messageTest = 3;
			int delay = 10;
			int times = 1000;
			
			int secondParam = 0;
			int thirdParam = 0;
			if (args.length > 0 && args[0] != null) {
				messageTest = Integer.valueOf(args[0]);
			}

			if (args.length > 1 && args[1] != null) {
				secondParam = Integer.valueOf(args[1]);
			}

			if (args.length > 2 && args[2] != null) {
				thirdParam = Integer.valueOf(args[2]);
			}

			switch (messageTest) {
			// Receives messages until the finish command
			case 1:
				double diff = 0;
				delay = secondParam;
				while (true) {

					String message = messenger.getMessage();
					if (message != null) {
						Thread.sleep(delay);
						messageNum++;
						logger.info("Gets message number:" + messageNum);
						logger.debug("   message content: " + message);

						// debug: detect dropped messages
						try {
							double current_diff = Double.parseDouble(message) - messageNum;
							if (current_diff > diff) {
								logger.debug(" * (" + (current_diff - diff) + ")");
								diff = current_diff;
							}	
						} catch (Exception e) {
							
						}
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
				times = secondParam;
				delay = thirdParam;
				for (int i = 0; i < times; i++) {
					messageNum++;
					logger.info("Sends message number:" + messageNum);
					messenger.sendMessage(String.valueOf(messageNum));
					Thread.sleep(thirdParam);
				}
				logger.info("Total sent messages:" + messageNum);
				logger.info("Pres any key to terminate...");
				System.in.read();
				messenger.stopMessaging();
				out.close();
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
						logger.info("Gets message number:" + messageNum++);
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
				delay = secondParam;
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
			//send message in "bursts"
			case 6:
				times = secondParam;
				delay = thirdParam;
				for (int i = 0; i < times; i++) {
					messageNum++;
					logger.info("Sends message number:" + messageNum);
					messenger.sendMessage(String.valueOf(messageNum));
					Thread.sleep(5);
				}
				Thread.sleep(10000);
				for (int i = 0; i < times; i++) {
					messageNum++;
					logger.info("Sends message number:" + messageNum);
					messenger.sendMessage(String.valueOf(messageNum));
					Thread.sleep(5);
				}
				
				logger.info("Total sent messages:" + messageNum);
				logger.info("Press any key to terminate...");
				System.in.read();
				messenger.stopMessaging();
				out.close();
				break;
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
}
