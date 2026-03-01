the server: code that the user can send api request to from various applications 
the server has a few functions:
- receive api requests from the user
- create rs jobs 
    - rs jobs are a sequence of commands that are executed on the client the job will invlove pulling files from s3 into a staging location on the client and then connecting the user on the rsnode server residing on the client after screate the session then create a project with a new scene and then based on the dataroot and the session code you can find the folder where project will look for files when you run the add folder command then you can run the arbitrary command called align task = client.project.command("align")
    - the server will communicate with the client machines over redis
    
- the client
    - the client will be set up and started with a single script the script should first check if the client is already running and if it is then it should ask if i want to shut down curretnly running client and monitoring scripts and rsnode.exe after that it should check git and pull the latest code from main branch no matter what after that it can run a python script this python script being client.py will handle various tasks it should listen to the redis channel for new commands from the server and execute them on the client machine the server will hold the job and handle the communcation back and forth to track what part of the stage the rs node is at so it can issue new commands to the client as needed the client must also monitor rsnode.exe and if it detects that it has stopped it should report back to the server stop the job and attempt to re start rs node the server should wait for the node to re start at which point it start the job from scratch it will need to copy the images from staging again the client should regularly report its status over redis the rsnode status and health and the client status and health 
 


machines

host machine
- this machine the the machine runniing the server the redis and the db 
- client machines these machines runs the client script and the rsnode.exe