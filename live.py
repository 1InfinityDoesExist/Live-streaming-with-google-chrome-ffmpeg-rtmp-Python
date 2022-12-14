import threading
import webbrowser
from kafka import KafkaConsumer
import sys
import json
import json.decoder
import subprocess as sp

bootstrap_servers = ['localhost:9092']

topicName = 'youtube_live_1'

consumer = KafkaConsumer (topicName, group_id ='group1',bootstrap_servers =
   bootstrap_servers, value_deserializer=lambda m: json.loads(m.decode('utf-8')))


webbrowser.register("xdg-open", None, webbrowser.BackgroundBrowser("xdg-open"), preferred=True)
print(webbrowser.get())

for msg in consumer:
	print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
	jsonData = msg.value

	rtmpUrl = jsonData["rtmpUrl"]
	baUrl = jsonData["baUrl"]
	rtmpKey = jsonData["rtmpKey"]
	print (rtmpUrl)
	print (baUrl)
	print (rtmpKey)

	try:
		webbrowser.open(baUrl)
		command =  ['ffmpeg',
	        	 '-f' ,'x11grab',
        		 '-i' ,':0',
         		 '-f' ,'pulse',
         		 '-i' ,'default',
         		 '-c:v' ,'libx264',
         		 '-c:a' ,'aac',
         		 '-vf' ,'format=yuv420p',
         		 '-g' ,'50',
         		 '-b:v','4000k',
         		 '-maxrate' ,'4000k',
         		 '-bufsize' ,'8000k',
         		 '-f', 'flv',
			 'rtmp://a.rtmp.youtube.com/live2/3q91-5ays-kxjh-da69-ed7h']



		p = sp.Popen(command, stdin=sp.PIPE)
		print(p)


		p.stdin.close()  # Close stdin pipe
		p.wait()

	except:
		print("----Something went wrong, Could not open browser/ or ffmpeg crashed")
	print("Success")


