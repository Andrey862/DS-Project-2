# Project 2
## How to launch
### Naming server
>sudo docker run --env NN=\<ip of your NS here\> -p 8800-8810:8800-8810 andrey2/datanode:1.0 
### Data node
>sudo docker run -p 8800-8810:8800-8810 andrey2/namingserver:1.0
### Clinet
git clone https://github.com/Andrey862/DS-Project-2 <br/>
cd Client
<br/>
python client.py
## Client commands
NS_ip 8800 ls .
NS_ip 8800 mkdir Foder_name
NS_ip 8800 rm File_name
NS_ip 8800 rm Folder_name
NS_ip 8800 write File_name
NS_ip 8800 ls . -r


## Architecture
![Architecture](./img/DS_P2.jpg)
## Contribution
Andrey | Magomed
------|------
Design | Naming server
Data Node | Client
Client | 