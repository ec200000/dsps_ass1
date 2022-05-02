Submitted by: Tal Skopas 322593070, Itay Cohen 211896261

The name of the local application JAR is: LocalApplication.jar
The JAR for the manager (Manager.jar) and the worker (Worker.jar) are in an S3 bucket named dspsass1.
We have a single queue from the local applications to the manager, and queues from the manager to each local application.
Moreover, we have a single queue between the manager and the workers and a single queue from the workers to the manager.
The AMI we used is: ami-00e95a9222311e8ed.
The type of instance we used for the workers is t2.micro and for the manager t2.medium.

It took our program 6:30 minutes to finish working on the first input with n=250
and 3:30 minutes to finish working on the second input with n=100. 
We ran both programs without terminate.

How our program works?
First, the local application uploads a file with the list of PDF files and operations.
It sends a message to a "LocalToManager" queue stating the location of the input file on a bucket on the S3, and starts the manager if it isn't active.
The manager creates a new thread for the process, downloads the PDF file and creates a message to a "ManagerToWorker" queue for each URL and the operation that needs to be done.
After that, the manager creates the workers according to the 'n' given, and the workers start working on their assignments.
The workers reads the messages from the "ManagerToWorker" queue and perform the requested task on the PDF.
When a worker finish his job, it uploads the resulted file to the S3 and puts a message in a "WorkerToManager" queue.
The manager reads all the messages from the "WorkerToManager" queue, it creates a summary text file and uploads it to the S3.
The manager sends a message to a costum queue from him to the specific local application about the lcoation of the summary file.
The local application reads the message from the queue, downloads the summary file from the S3 and creates an html file.
At the end, if terminate is one of the arguments, the local application sends a terminate message to the manager, which closes his queue to new local
applciations and closes the queue from him to the workers when they finish, signaling them to terminate.


▪ Did you think for more than 2 minutes about security? Do not send your credentials in plain text!
Yes, we used the following attribute in order to "save" the credentials:
.iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::788820258204:instance-profile/LabInstanceProfile").build())

▪ Did you think about scalability? Will your program work properly when 1 million clients
connected at the same time? How about 2 million? 1 billion? Scalability is very important
aspect of the system, be sure it is scalable!
Yes, our program should work properly on any amount of clients.

▪ What about persistence? What if a node dies? What if a node stalls for a while? Have you
taken care of all possible outcomes in the system? Think of more possible issues that might
arise from failures. What did you do to solve it? What about broken communications? Be
sure to handle all fail-cases!
The system is persistent.
We use visibilityTimeout, so if a worker dies in a middle of a job, another worker will take it.
In other cases, we used try-catch in order to catch all the possible outcomes and treated them accordingly.

▪ Threads in your application, when is it a good idea? When is it bad? Invest time to think
about threads in your application!
It's a good idea when we have multiple local applications. The manager should work in parallel.

▪ Did you run more than one client at the same time? Be sure they work properly, and finish
properly, and your results are correct.
Yes, we run the program with more than one client and it works as expected.

▪ Do you understand how the system works? Do a full run using pen and paper, draw the
different parts and the communication that happens between them.
Yes.

▪ Did you manage the termination process? Be sure all is closed once requested!
Yes, the termination process works as expected.

▪ Did you take in mind the system limitations that we are using? Be sure to use it to its fullest!
Yes, we restricted the number of workers to 18 at the same time.

▪ Are all your workers working hard? Or some are slacking? Why?
Yes, all the workers are working hard and supposed to work equally.

▪ Is your manager doing more work than he's supposed to? Have you made sure each part of
your system has properly defined tasks? Did you mix their tasks? Don't!
No, the manager only does the works that it supposed to do. Each part of the system is properly defined and has its own target.

▪ Lastly, are you sure you understand what distributed means? Is there anything in your
system awaiting another?
Yes, we understood the meaning of distributed.
No, every entity in the system works separately without relation to the other entities.